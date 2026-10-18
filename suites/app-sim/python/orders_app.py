#!/usr/bin/env python3
"""orders_app.py — the Python half of the Kafka application simulator.

The same ordinary application as ../java/OrdersApp.java, written against confluent-kafka
(librdkafka) the way a Python team on Apache Kafka would: AdminClient for setup, an idempotent
producer, a consumer group of several members that rebalances, and a transactional
consume-transform-produce loop. It knows nothing about KubeMQ; repointing it is a change of
bootstrap address.

It does not judge its own results. Every observable fact goes to a JSON-lines ledger and
../verify.py decides — an application that graded itself would pass whenever it stopped looking.

Commands (one per process): version, admin, produce, group, eos. Every command first prints and
records the confluent-kafka and librdkafka versions actually loaded, so run.sh and verify.py can
refuse a result produced by a client other than the one the run asked for.
"""
import argparse
import json
import os
import sys
import threading
import time

import confluent_kafka
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

ORDERS = "orders"
ENRICHED = "orders-enriched"
PAYMENTS = "payments"

_lock = threading.Lock()
_ledger = None


def emit(**kv):
    """Append one event. A write failure is fatal: a ledger that drops events passes everything."""
    kv = {"t": int(time.time() * 1000), **kv}
    line = json.dumps(kv, separators=(",", ":")) + "\n"
    with _lock:
        _ledger.write(line)
        _ledger.flush()


def err_name(e):
    if isinstance(e, KafkaException) and e.args and isinstance(e.args[0], KafkaError):
        k = e.args[0]
        return f"{k.name()}: {k.str()}"
    if isinstance(e, KafkaError):
        return f"{e.name()}: {e.str()}"
    return f"{type(e).__name__}: {e}"


def loaded_versions():
    """(confluent-kafka, librdkafka) as the loaded modules report them. confluent_kafka.version()
    returned a (str, int) tuple up to the 2.6 line and returns a plain str in 2.15 — indexing the
    str gave "2", which the version gate caught — so both shapes are accepted."""
    def first(v):
        return v[0] if isinstance(v, tuple) else str(v)
    return first(confluent_kafka.version()), first(confluent_kafka.libversion())


def common(args):
    return {"bootstrap.servers": args.bootstrap, "client.id": "orders-app-python"}


# ------------------------------------------------------------------------------------------ admin

def admin(args):
    """A Python service owns its own topic: `payments`, 6 partitions, at the production
    replication factor. Every call is recorded with its outcome."""
    a = AdminClient(common(args))
    try:
        md = a.list_topics(timeout=30)
        emit(ev="admin", app="python", op="describe_cluster", ok=True, brokers=len(md.brokers),
             controller=str(md.controller_id))
    except Exception as e:  # noqa: BLE001 — recorded, not swallowed
        emit(ev="admin", app="python", op="describe_cluster", ok=False, error=err_name(e))

    # The requested partition count is recorded with the create so the verifier can hold the
    # describe below to it: a refused create followed by an auto-created 1-partition "phantom"
    # would otherwise read as a successful describe.
    want = 6
    fs = a.create_topics([NewTopic(PAYMENTS, num_partitions=want, replication_factor=args.rf,
                                   config={"retention.ms": "86400000"})], request_timeout=60)
    for topic, f in fs.items():
        try:
            f.result(60)
            emit(ev="admin", app="python", op="create_topic", target=topic, ok=True, rf=args.rf,
                 requested_partitions=want)
        except Exception as e:  # noqa: BLE001
            name = err_name(e)
            emit(ev="admin", app="python", op="create_topic", target=topic,
                 ok=name.startswith("TOPIC_ALREADY_EXISTS"), rf=args.rf, requested_partitions=want, error=name)

    # A just-created topic is not instantly describable on any broker (Apache Kafka included):
    # retry through that window as a real script would, and record the attempts.
    parts, replicas, last_err, attempt = 0, 0, None, 0
    for attempt in range(1, 31):
        try:
            md = a.list_topics(topic=PAYMENTS, timeout=30)
            t = md.topics.get(PAYMENTS)
            if t is not None and t.error is None and t.partitions:
                parts = len(t.partitions)
                replicas = len(next(iter(t.partitions.values())).replicas)
                break
            last_err = err_name(t.error) if t is not None and t.error is not None else "topic not in metadata"
        except Exception as e:  # noqa: BLE001
            last_err = err_name(e)
        time.sleep(0.5)
    emit(ev="admin", app="python", op="describe_topic", target=PAYMENTS, ok=parts > 0,
         partitions=parts, replicas=replicas, attempts=attempt, error=None if parts else last_err)

    try:
        res = a.describe_configs([ConfigResource(ConfigResource.Type.TOPIC, ORDERS)], request_timeout=30)
        cfg = list(res.values())[0].result(30)
        ret = cfg.get("retention.ms")
        emit(ev="admin", app="python", op="describe_config", target=ORDERS, ok=True,
             retention_ms=None if ret is None else ret.value)
    except Exception as e:  # noqa: BLE001
        emit(ev="admin", app="python", op="describe_config", target=ORDERS, ok=False, error=err_name(e))


# ------------------------------------------------------------------------------ idempotent producer

def produce(args):
    """Idempotent, acks=all, one pass per codec; keyed by customer so per-customer order holds."""
    codecs = args.codecs.split(",")
    per = args.count // len(codecs)
    pad = "x" * args.pad
    seq = 0
    for codec in codecs:
        acked = [0]
        p = Producer({**common(args), "enable.idempotence": True, "acks": "all",
                      "compression.type": codec, "linger.ms": 5, "delivery.timeout.ms": 120000})

        def on_delivery(err, msg, s, key, codec=codec):
            if err is None:
                acked[0] += 1
                emit(ev="produced", app=args.app, topic=ORDERS, p=msg.partition(), o=msg.offset(),
                     key=key, seq=s, codec=codec)
            else:
                emit(ev="produce_failed", app=args.app, key=key, seq=s, codec=codec, error=err_name(err))

        end = seq + per
        while seq < end:
            s = seq
            key = f"cust-{s % 200}"
            value = json.dumps({"app": args.app, "seq": s, "codec": codec, "pad": pad},
                               separators=(",", ":"))
            try:
                p.produce(ORDERS, key=key, value=value,
                          on_delivery=lambda err, msg, s=s, key=key: on_delivery(err, msg, s, key))
                seq += 1
            except BufferError:
                p.poll(0.1)
                continue
            p.poll(0)
        left = p.flush(150)
        emit(ev="produce_phase", app=args.app, codec=codec, acked=acked[0], sent=per, unflushed=left)


# ------------------------------------------------------------------------------------ consumer group

def finished(args, last_record, start):
    now = time.time()
    if now - start > args.timeout_sec:
        return True
    return os.path.exists(args.done_file) and now - last_record[0] > args.idle_exit_sec


def member(args, group, name, stop, last_record):
    c = Consumer({**common(args), "group.id": group, "auto.offset.reset": "earliest",
                  "enable.auto.commit": False, "isolation.level": "read_uncommitted"})

    def on_assign(consumer, parts):
        emit(ev="assigned", group=group, member=name, partitions=sorted(p.partition for p in parts))

    def on_revoke(consumer, parts):
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as e:
            if e.args[0].code() != KafkaError._NO_OFFSET:
                emit(ev="commit_failed", group=group, member=name, error=err_name(e))
        emit(ev="revoked", group=group, member=name, partitions=sorted(p.partition for p in parts))

    def on_lost(consumer, parts):
        emit(ev="lost", group=group, member=name, partitions=sorted(p.partition for p in parts))

    c.subscribe([ORDERS], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)
    emit(ev="member_started", group=group, member=name)
    try:
        while not stop.is_set():
            msgs = c.consume(num_messages=500, timeout=0.5)
            got = False
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        emit(ev="poll_error", group=group, member=name, error=err_name(m.error()))
                    continue
                got = True
                v = json.loads(m.value())
                emit(ev="consumed", group=group, member=name, topic=m.topic(), p=m.partition(),
                     o=m.offset(), key=m.key().decode() if m.key() else None, src_app=v.get("app"),
                     seq=v.get("seq"), codec=v.get("codec"))
                last_record[0] = time.time()
            if got:
                try:
                    parts = c.commit(asynchronous=False)
                    for tp in parts or []:
                        emit(ev="committed", group=group, member=name, p=tp.partition, o=tp.offset)
                except KafkaException as e:
                    emit(ev="commit_failed", group=group, member=name, error=err_name(e))
    finally:
        c.close()
        emit(ev="member_stopped", group=group, member=name)


def group(args):
    """Three members, the third leaves, a fourth joins later: two rebalances under traffic."""
    grp = args.group or "orders-python"
    start = time.time()
    last_record = [start]
    stops, threads = {}, []

    def launch(name):
        stops[name] = threading.Event()
        t = threading.Thread(target=member, args=(args, grp, name, stops[name], last_record), daemon=True)
        t.start()
        threads.append(t)

    for i in range(1, args.members + 1):
        launch(f"m{i}")
    left = joined = False
    while not finished(args, last_record, start):
        el = time.time() - start
        if not left and el > args.leave_after_sec:
            stops[f"m{args.members}"].set()
            emit(ev="member_leave_requested", group=grp, member=f"m{args.members}")
            left = True
        if not joined and el > args.join_after_sec:
            launch(f"m{args.members + 1}")
            emit(ev="member_join_requested", group=grp, member=f"m{args.members + 1}")
            joined = True
        time.sleep(0.2)
    for s in stops.values():
        s.set()
    for t in threads:
        t.join(60)


# ----------------------------------------------------------------------------- transactional pipeline

class ProducerFenced(Exception):
    """The transactional producer hit a fatal error and must be replaced."""


def txn_call(fn):
    """librdkafka's transactional error contract, applied to one call: a retriable error means
    call it again; anything else is for the caller to classify."""
    for _ in range(120):
        try:
            return fn()
        except KafkaException as e:
            if e.args[0].retriable():
                time.sleep(0.5)
                continue
            raise
    raise RuntimeError("transactional call still retriable after 60s")


def abort_until_done(p, args, txn):
    """An abort that times out is still in progress and must be called again until it completes —
    starting a new transaction first fails with _CONFLICT. A fatal error means the producer itself
    is unusable and has to be replaced."""
    for attempt in range(1, 21):
        try:
            p.abort_transaction(30)
            return
        except KafkaException as e:
            err = e.args[0]
            if err.fatal():
                raise ProducerFenced(err_name(e))
            emit(ev="abort_retry", app=args.app, txn=txn, attempt=attempt, error=err_name(e))
    raise ProducerFenced(f"abort of transaction {txn} did not complete after 20 attempts")


def eos(args):
    """Consume-transform-produce, exactly once. Every Nth transaction is aborted on purpose after
    its records were acknowledged, and its input rewound — so both halves can be checked.

    Errors follow librdkafka's documented transactional contract, because an application that
    does not is what a coordinator failover exposes first: retriable errors are retried in place,
    abortable ones abort and rewind, and a fatal one replaces the producer — its successor's
    init_transactions fences the old one — before rewinding."""
    grp = args.group or "enricher-python"
    start = time.time()
    last_record = [start]
    c = Consumer({**common(args), "group.id": grp, "auto.offset.reset": "earliest",
                  "enable.auto.commit": False, "isolation.level": "read_committed"})

    def make_producer():
        prod = Producer({**common(args), "transactional.id": f"enricher-{args.app}-{args.run}",
                         "enable.idempotence": True, "acks": "all", "compression.type": "snappy"})
        txn_call(lambda: prod.init_transactions(60))
        return prod

    p = make_producer()
    c.subscribe([ORDERS])
    txn = 0
    aborted_once = set()

    def rewind(firsts):
        for part, off in firsts.items():
            try:
                c.seek(TopicPartition(ORDERS, part, off))
            except KafkaException:
                pass  # no longer assigned after a rebalance; its committed offset is the rewind

    try:
        while not finished(args, last_record, start):
            msgs = [m for m in c.consume(num_messages=500, timeout=0.5) if not m.error()]
            if not msgs:
                continue
            last_record[0] = time.time()
            txn += 1
            firsts, srcs = {}, []
            sent_ok = [0]
            try:
                txn_call(p.begin_transaction)
                for m in msgs:
                    v = json.loads(m.value())
                    firsts.setdefault(m.partition(), m.offset())
                    srcs.append(f"{v.get('app')}:{v.get('seq')}")
                    out = json.dumps({"app": args.app, "src_app": v.get("app"), "src_seq": v.get("seq"),
                                      "txn": txn}, separators=(",", ":"))

                    def dr(err, msg):
                        if err is None:
                            sent_ok[0] += 1
                    p.produce(ENRICHED, key=m.key(), value=out, on_delivery=dr)
                    p.poll(0)
                txn_call(lambda: p.send_offsets_to_transaction(c.position(c.assignment()),
                                                               c.consumer_group_metadata(), 30))
                batch_id = tuple(sorted(firsts.items()))
                if args.abort_every > 0 and txn % args.abort_every == 0 and batch_id not in aborted_once:
                    aborted_once.add(batch_id)
                    p.flush(60)
                    abort_until_done(p, args, txn)
                    emit(ev="txn_aborted", app=args.app, txn=txn, records=len(srcs), sent_ok=sent_ok[0], srcs=srcs)
                    rewind(firsts)
                else:
                    txn_call(lambda: p.commit_transaction(60))
                    emit(ev="txn_committed", app=args.app, txn=txn, records=len(srcs), srcs=srcs)
            except (KafkaException, ProducerFenced) as e:
                emit(ev="txn_error", app=args.app, txn=txn, error=err_name(e) if isinstance(e, KafkaException) else str(e))
                fatal = isinstance(e, ProducerFenced) or (isinstance(e, KafkaException) and e.args[0].fatal())
                if not fatal:
                    try:
                        abort_until_done(p, args, txn)
                    except ProducerFenced:
                        fatal = True
                if fatal:
                    emit(ev="producer_recreated", app=args.app, txn=txn)
                    try:
                        p.flush(5)
                    except Exception:  # noqa: BLE001 — the old producer is being discarded
                        pass
                    p = make_producer()
                rewind(firsts)
            finally:
                # Time spent inside a transaction is not idle time: a coordinator failover can
                # hold a commit for tens of seconds (~43s measured on Apache Kafka).
                last_record[0] = time.time()
    finally:
        c.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cmd", required=True, choices=["version", "admin", "produce", "group", "eos"])
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--ledger", default="")
    ap.add_argument("--app", default="python")
    ap.add_argument("--run", default="r")
    ap.add_argument("--rf", type=int, default=3)
    ap.add_argument("--count", type=int, default=4000)
    ap.add_argument("--codecs", default="snappy,zstd")
    ap.add_argument("--pad", type=int, default=400)
    ap.add_argument("--group", default="")
    ap.add_argument("--members", type=int, default=3)
    ap.add_argument("--leave-after-sec", type=float, default=8)
    ap.add_argument("--join-after-sec", type=float, default=16)
    ap.add_argument("--abort-every", type=int, default=5)
    ap.add_argument("--done-file", default="/out/produce.done")
    ap.add_argument("--idle-exit-sec", type=float, default=15)
    ap.add_argument("--timeout-sec", type=float, default=300)
    args = ap.parse_args()

    global _ledger
    path = args.ledger or f"/out/{args.cmd}-python.jsonl"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _ledger = open(path, "a", encoding="utf-8")
    emit(ev="start", app="python", cmd=args.cmd, bootstrap=args.bootstrap)
    ck, rdk = loaded_versions()
    print(f"confluent-kafka version: {ck} (librdkafka {rdk})", flush=True)
    emit(ev="client_version", app="python", cmd=args.cmd, lib="confluent-kafka", version=ck, librdkafka=rdk)
    rc = 0
    try:
        {"version": lambda _args: None, "admin": admin, "produce": produce, "group": group,
         "eos": eos}[args.cmd](args)
    except Exception as e:  # noqa: BLE001 — recorded as fatal, then re-raised via rc
        emit(ev="fatal", app="python", cmd=args.cmd, error=err_name(e))
        import traceback
        traceback.print_exc()
        rc = 1
    finally:
        emit(ev="end", app="python", cmd=args.cmd, rc=rc)
        _ledger.close()
    sys.exit(rc)


if __name__ == "__main__":
    main()
