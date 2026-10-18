#!/usr/bin/env python3
"""librdkafka transaction driver (confluent-kafka-python), suites/transactions/SCENARIOS.md.

Runs one scenario against one broker inside a client container and prints one JSON report as
the last line of standard output: {"client": ..., "scenario": ..., "facts": {...}}. It reports
facts only; suites/transactions/verify judges them. Exit 1 means the scenario could not
complete.
"""
import argparse
import json
import sys
import time

import confluent_kafka
from confluent_kafka import (Consumer, ConsumerGroupTopicPartitions, IsolationLevel, KafkaError,
                             KafkaException, Producer, TopicPartition)
from confluent_kafka.admin import AdminClient, NewTopic, OffsetSpec

BOOTSTRAP = ""
TARGET = ""


def name(kind):
    return "kcert-txn-%s-%s-%d" % (kind, TARGET, time.time_ns())


def keys(prefix, start, n):
    return ["%s-%02d" % (prefix, i) for i in range(start, start + n)]


def value_for(key):
    return "v-" + key


def err_name(exc):
    """Kafka error name of a failure; CLIENT:<name> for librdkafka-local errors."""
    if exc is None:
        return "NONE"
    err = exc.args[0] if isinstance(exc, KafkaException) and exc.args else exc
    if isinstance(err, KafkaError):
        if err.code() >= 0:
            return err.name()
        return "CLIENT:" + err.name()
    return "CLIENT:" + type(exc).__name__


def outcome(fn):
    try:
        fn()
        return "NONE"
    except Exception as e:  # noqa: BLE001 - every failure is a fact
        return err_name(e)


def admin():
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def create_topic(adm, topic, partitions):
    fs = adm.create_topics([NewTopic(topic, partitions, 1)])
    fs[topic].result(60)
    deadline = time.time() + 60
    while time.time() < deadline:
        md = adm.list_topics(topic, timeout=10)
        t = md.topics.get(topic)
        if t is not None and t.error is None and len(t.partitions) == partitions and all(p.leader >= 0 for p in t.partitions.values()):
            return
        time.sleep(0.2)
    raise RuntimeError("topic %s never became ready" % topic)


def producer(tid=None, txn_timeout_ms=60000):
    conf = {"bootstrap.servers": BOOTSTRAP, "message.timeout.ms": 30000, "request.timeout.ms": 20000}
    if tid is not None:
        conf["transactional.id"] = tid
        conf["transaction.timeout.ms"] = txn_timeout_ms
        # librdkafka refuses a message timeout longer than the transaction timeout.
        conf["message.timeout.ms"] = min(30000, txn_timeout_ms)
    return Producer(conf)


def send_sync(p, topic, partition, ks):
    errs = []

    def cb(err, _msg):
        if err is not None:
            errs.append(err)

    for k in ks:
        p.produce(topic, key=k, value=value_for(k), partition=partition, on_delivery=cb)
    left = p.flush(30)
    if left:
        raise KafkaException(KafkaError(KafkaError._TIMED_OUT, "%d records not delivered" % left))
    if errs:
        raise KafkaException(errs[0])


def list_offsets(adm, topic):
    tp = TopicPartition(topic, 0)
    lso = adm.list_offsets({tp: OffsetSpec.latest()}, isolation_level=IsolationLevel.READ_COMMITTED)[tp].result(30).offset
    hwm = adm.list_offsets({tp: OffsetSpec.latest()}, isolation_level=IsolationLevel.READ_UNCOMMITTED)[tp].result(30).offset
    return lso, hwm


def read_all(topic, partitions, isolation):
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": name("reader"), "isolation.level": isolation,
                  "enable.auto.commit": False, "fetch.wait.max.ms": 500})
    c.assign([TopicPartition(topic, p, confluent_kafka.OFFSET_BEGINNING) for p in range(partitions)])
    out = []
    quiet_since = time.time()
    seen = False
    try:
        while time.time() - quiet_since < 4 or (not seen and time.time() - quiet_since < 10):
            m = c.poll(0.5)
            if m is None:
                continue
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(m.error())
            out.append({"partition": m.partition(), "offset": m.offset(),
                        "key": m.key().decode(), "value": m.value().decode()})
            quiet_since = time.time()
            seen = True
    finally:
        c.close()
    return out


# ---- scenarios ------------------------------------------------------------------------------

def isolation():
    adm = admin()
    topic = name("isolation")
    create_topic(adm, topic, 1)
    p = producer(name("isolation-tid"))
    p.init_transactions(60)
    f, outcomes = {}, {}
    p.begin_transaction()
    send_sync(p, topic, 0, keys("c", 0, 6))
    f["open_lso"], f["open_hwm"] = list_offsets(adm, topic)
    outcomes["commit1"] = outcome(lambda: p.commit_transaction(60))
    p.begin_transaction()
    send_sync(p, topic, 0, keys("a", 0, 4))
    outcomes["abort"] = outcome(lambda: p.abort_transaction(60))
    p.begin_transaction()
    send_sync(p, topic, 0, keys("c", 6, 3))
    outcomes["commit2"] = outcome(lambda: p.commit_transaction(60))
    time.sleep(1)
    f["final_lso"], f["final_hwm"] = list_offsets(adm, topic)
    f["outcomes"] = outcomes
    f["read_committed"] = read_all(topic, 1, "read_committed")
    f["read_uncommitted"] = read_all(topic, 1, "read_uncommitted")
    return f


def group_offset(adm, group, topic, require_stable):
    tp = TopicPartition(topic, 0)
    try:
        fut = adm.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group, [tp])],
                                              require_stable=require_stable, request_timeout=5)[group]
        res = fut.result(10)
        for x in res.topic_partitions:
            if x.error is not None:
                return err_name(KafkaException(x.error))
            return "none" if x.offset < 0 else str(x.offset)
        return "absent"
    except Exception as e:  # noqa: BLE001
        return err_name(e)


def settled_offset(adm, group, topic):
    deadline = time.time() + 15
    while True:
        v = group_offset(adm, group, topic, True)
        if v != "UNSTABLE_OFFSET_COMMIT" or time.time() > deadline:
            return v
        time.sleep(0.2)


def send_offsets():
    adm = admin()
    src, out, group = name("so-in"), name("so-out"), name("so-group")
    create_topic(adm, src, 1)
    create_topic(adm, out, 1)
    send_sync(producer(), src, 0, keys("i", 0, 10))
    # Group metadata of a group with no members: generation -1, no member id.
    meta_consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group})
    meta = meta_consumer.consumer_group_metadata()
    p = producer(name("so-tid"))
    p.init_transactions(60)
    f, outcomes = {}, {}

    def txn(label, ks, offset, commit, while_open=None):
        p.begin_transaction()
        send_sync(p, out, 0, ks)
        outcomes[label + "_send_offsets"] = outcome(
            lambda: p.send_offsets_to_transaction([TopicPartition(src, 0, offset)], meta, 30))
        if while_open:
            while_open()
        outcomes[label + "_end"] = outcome(lambda: p.commit_transaction(60) if commit else p.abort_transaction(60))

    txn("txn1", keys("o", 0, 5), 5, True)
    f["after_commit1"] = settled_offset(adm, group, src)
    txn("txn2", keys("o", 5, 5), 10, False,
        lambda: f.__setitem__("stable_fetch_while_open", group_offset(adm, group, src, True)))
    f["after_abort"] = settled_offset(adm, group, src)
    txn("txn3", keys("o", 5, 5), 10, True)
    f["after_commit2"] = settled_offset(adm, group, src)
    meta_consumer.close()
    f["outcomes"] = outcomes
    f["read_committed"] = read_all(out, 1, "read_committed")
    return f


def cpp():
    inputs, parts = 60, 3
    adm = admin()
    src, out, group = name("cpp-in"), name("cpp-out"), name("cpp-group")
    create_topic(adm, src, parts)
    create_topic(adm, out, parts)
    plain = producer()
    for n in range(inputs):
        k = "i-%02d" % n
        plain.produce(src, key=k, value=value_for(k), partition=n % parts)
    if plain.flush(30):
        raise RuntimeError("input not delivered")
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "isolation.level": "read_committed",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest", "fetch.wait.max.ms": 500,
                  # Allowed deviation (docs/methodology.md): the classic group protocol.
                  "group.protocol": "classic"})
    c.subscribe([src])
    p = producer(name("cpp-tid"))
    p.init_transactions(60)
    commits = aborts = txn = committed_out = 0
    first_error = "NONE"
    deadline = time.time() + 120
    while committed_out < inputs and time.time() < deadline:
        msgs = [m for m in c.consume(5, 0.5) if m.error() is None]
        if not msgs:
            continue
        try:
            p.begin_transaction()
            for m in msgs:
                k = "o-" + m.key().decode()[2:]
                p.produce(out, key=k, value=value_for(k), partition=m.partition())
            if p.flush(30):
                raise KafkaException(KafkaError(KafkaError._TIMED_OUT))
            p.send_offsets_to_transaction(c.position(c.assignment()), c.consumer_group_metadata(), 30)
        except Exception as e:  # noqa: BLE001
            first_error = "produce:" + err_name(e)
            break
        txn += 1
        try:
            if txn % 3 == 0:
                p.abort_transaction(60)
                aborts += 1
                # Resume from the group's committed offsets (or the start).
                committed = c.committed(c.assignment(), 30)
                for tp in committed:
                    c.seek(TopicPartition(tp.topic, tp.partition, tp.offset if tp.offset >= 0 else confluent_kafka.OFFSET_BEGINNING))
            else:
                p.commit_transaction(60)
                commits += 1
                committed_out += len(msgs)
        except Exception as e:  # noqa: BLE001
            first_error = "end:" + err_name(e)
            break
    c.close()
    offsets = {}
    try:
        res = adm.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[group].result(30)
        for x in res.topic_partitions:
            if x.topic == src:
                offsets[str(x.partition)] = x.offset
    except Exception as e:  # noqa: BLE001
        if first_error == "NONE":
            first_error = "offset fetch:" + err_name(e)
    return {"commits": commits, "aborts": aborts, "first_error": first_error, "group_offsets": offsets,
            "read_committed": read_all(out, parts, "read_committed"),
            "read_uncommitted": read_all(out, parts, "read_uncommitted")}


def fencing():
    adm = admin()
    topic, tid = name("fence"), name("fence-tid")
    create_topic(adm, topic, 1)
    outcomes = {}
    a = producer(tid)
    a.init_transactions(60)
    a.begin_transaction()
    send_sync(a, topic, 0, keys("z", 0, 3))
    b = producer(tid)
    outcomes["b_init"] = outcome(lambda: b.init_transactions(60))
    outcomes["a_send_after_fence"] = outcome(lambda: send_sync(a, topic, 0, keys("z", 3, 1)))
    outcomes["a_commit_after_fence"] = outcome(lambda: a.commit_transaction(30))
    if outcomes["b_init"] == "NONE":
        b.begin_transaction()
        send_sync(b, topic, 0, keys("b", 0, 2))
        outcomes["b_commit"] = outcome(lambda: b.commit_transaction(60))
    time.sleep(1)
    lso, hwm = list_offsets(adm, topic)
    return {"outcomes": outcomes, "final_lso": lso, "final_hwm": hwm,
            "read_committed": read_all(topic, 1, "read_committed"),
            "read_uncommitted": read_all(topic, 1, "read_uncommitted")}


def timeout_abort():
    adm = admin()
    topic, tid = name("timeout"), name("timeout-tid")
    create_topic(adm, topic, 1)
    outcomes = {}
    p = producer(tid, 3000)
    p.init_transactions(60)
    p.begin_transaction()
    send_sync(p, topic, 0, keys("t", 0, 3))
    aborted = False
    for _ in range(45):
        time.sleep(1)
        lso, hwm = list_offsets(adm, topic)
        if lso == hwm:
            aborted = True
            break
    outcomes["late_commit"] = outcome(lambda: p.commit_transaction(30))
    del p
    q = producer(tid)
    q.init_transactions(60)
    q.begin_transaction()
    send = outcome(lambda: send_sync(q, topic, 0, keys("u", 0, 1)))
    if send != "NONE":
        outcomes["new_producer_send"] = send
    outcomes["new_producer_commit"] = outcome(lambda: q.commit_transaction(60))
    time.sleep(1)
    lso, hwm = list_offsets(adm, topic)
    return {"aborted_by_broker": aborted, "outcomes": outcomes, "final_lso": lso, "final_hwm": hwm,
            "read_committed": read_all(topic, 1, "read_committed"),
            "read_uncommitted": read_all(topic, 1, "read_uncommitted")}


def timeout_limit():
    """init_transactions() is where librdkafka sends InitProducerId."""
    outcomes = {}
    for label, ms in (("at_max_900000", 900000), ("above_max_900001", 900001)):
        p = producer(name("limit-" + label), ms)
        outcomes[label] = outcome(lambda: p.init_transactions(60))
    return {"outcomes": outcomes}


SCENARIOS = {
    "isolation": isolation,
    "send-offsets": send_offsets,
    "consume-transform-produce": cpp,
    "fencing": fencing,
    "timeout-abort": timeout_abort,
    "timeout-limit": timeout_limit,
}


def main():
    global BOOTSTRAP, TARGET
    ap = argparse.ArgumentParser()
    ap.add_argument("-bootstrap", required=True)
    ap.add_argument("-scenario", required=True)
    ap.add_argument("-target", default="broker")
    ap.add_argument("-deadline", default="")
    a = ap.parse_args()
    BOOTSTRAP, TARGET = a.bootstrap, a.target
    fn = SCENARIOS.get(a.scenario)
    if fn is None:
        print("txn_driver: unknown scenario " + a.scenario, file=sys.stderr)
        sys.exit(1)
    try:
        facts = fn()
    except Exception as e:  # noqa: BLE001
        print("txn_driver: %r" % (e,), file=sys.stderr)
        sys.exit(1)
    ver = getattr(confluent_kafka, "__version__", None) or confluent_kafka.version()[0]
    client = "confluent-kafka %s (librdkafka %s)" % (ver, confluent_kafka.libversion()[0])
    print(json.dumps({"client": client, "scenario": a.scenario, "facts": facts}))
    sys.stdout.flush()
    sys.exit(0)


if __name__ == "__main__":
    main()
