"""confluent-kafka-python driver of the clients track (suites/clients/SCENARIO.md).

The ledger goes to stdout, one JSON object per line; diagnostics go to stderr.
"""

import json
import os
import sys
import threading
import time

import confluent_kafka
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

CLIENT = "confluent-kafka-python"
_lock = threading.Lock()

BOOTSTRAP = os.environ.get("KCERT_BOOTSTRAP", "")
MODE = os.environ.get("KCERT_MODE", "")
TOPIC = os.environ.get("KCERT_TOPIC", "")
GROUP = os.environ.get("KCERT_GROUP", "")
PARTITIONS = int(os.environ.get("KCERT_PARTITIONS", "0"))
RECORDS = int(os.environ.get("KCERT_RECORDS", "0"))
RECORDS2 = int(os.environ.get("KCERT_RECORDS2", "0"))
KEYS = int(os.environ.get("KCERT_KEYS", "1"))
TIMEOUT = int(os.environ.get("KCERT_TIMEOUT_SEC", "60"))
ADMIN = os.environ.get("KCERT_ADMIN", "1") != "0"



def emit(ev, **fields):
    line = json.dumps(dict(ev=ev, **fields))
    with _lock:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()


def fail(step, code, msg):
    emit("error", step=step, error_code=code, error=str(msg))
    emit("done")
    sys.exit(1)


def code_of(err):
    """Kafka protocol error code of a KafkaError (librdkafka's local errors are negative: -1)."""
    if err is None:
        return 0
    if isinstance(err, KafkaException):
        err = err.args[0] if err.args else None
    if isinstance(err, KafkaError):
        c = err.code()
        return c if c >= 0 else -1
    return -1


def value(seq):
    return "value-%06d" % seq


def key(seq):
    return "key-%02d" % (seq % KEYS)


def seq_of(v):
    try:
        s = v.decode() if isinstance(v, bytes) else v
        return int(s[len("value-"):]) if s and s.startswith("value-") else -1
    except ValueError:
        return -1


def versions():
    v = confluent_kafka.version()
    lib = confluent_kafka.libversion()
    # confluent_kafka.version() returns a (string, int) tuple; older releases returned a string.
    v = v[0] if isinstance(v, tuple) else v
    lib = lib[0] if isinstance(lib, tuple) else lib
    return "%s (librdkafka %s)" % (v, lib)


def create(num_partitions, rf):
    adm = AdminClient({"bootstrap.servers": BOOTSTRAP})
    nt = NewTopic(TOPIC, num_partitions=num_partitions, replication_factor=rf)
    err = None
    try:
        adm.create_topics([nt])[TOPIC].result(timeout=TIMEOUT)
    except KafkaException as e:
        err = e
    emit("create", topic=TOPIC, partitions=num_partitions, error_code=code_of(err), error=str(err or ""))
    if err is not None:
        fail("create", code_of(err), err)
    # An application waits until the topic is usable before producing.
    deadline = time.time() + TIMEOUT
    while time.time() < deadline:
        md = adm.list_topics(TOPIC, timeout=5)
        t = md.topics.get(TOPIC)
        if t is not None and t.error is None and t.partitions and \
                (num_partitions < 0 or len(t.partitions) == num_partitions) and \
                all(p.leader >= 0 and p.error is None for p in t.partitions.values()):
            return
        time.sleep(0.2)
    fail("create", -1, "topic never became ready")


def produce(producer, phase, lo, hi, keyed):
    for seq in range(lo, hi):
        def cb(err, msg, s=seq):
            if err is not None:
                emit("ack", phase=phase, seq=s, partition=-1, offset=-1, error_code=code_of(err), error=str(err))
            else:
                emit("ack", phase=phase, seq=s, partition=msg.partition(), offset=msg.offset(), error_code=0)
        while True:
            try:
                if keyed:
                    producer.produce(TOPIC, key=key(seq), value=value(seq),
                                     headers=[("seq", str(seq).encode()), ("client", CLIENT.encode())], on_delivery=cb)
                else:
                    producer.produce(TOPIC, value=value(seq), on_delivery=cb)
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
    left = producer.flush(TIMEOUT)
    if left:
        fail("produce", -1, "%d records still unacknowledged after %ds" % (left, TIMEOUT))


def on_assign(phase, flag, held=None):
    def cb(consumer, parts):
        emit("assigned", phase=phase, partitions=sorted(p.partition for p in parts))
        if held is not None:
            held[:] = [p.partition for p in parts]
        if parts:
            flag.set()  # an empty assignment is not readiness
    return cb


def emit_msg(msg, phase, got):
    if msg is None:
        return
    if msg.error() is not None:
        if msg.error().code() == KafkaError._PARTITION_EOF:
            return
        fail("consume", code_of(msg.error()), msg.error())
    seq = seq_of(msg.value())
    got.add(seq)
    hs = [[k, (v or b"").decode()] for k, v in (msg.headers() or [])]
    k = msg.key()
    emit("recv", phase=phase, seq=seq, partition=msg.partition(), offset=msg.offset(),
         key=k.decode() if k is not None else None, value=msg.value().decode(), headers=hs)


def poll_until(consumer, phase, got, want):
    deadline = time.time() + TIMEOUT
    while len(got) < want:
        if time.time() > deadline:
            fail("consume", -1, "received %d of %d records before the deadline" % (len(got), want))
        emit_msg(consumer.poll(0.5), phase, got)


def consume(phase, want, drain):
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "group.protocol": "classic",  # the one allowed deviation
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    got = set()
    c.subscribe([TOPIC], on_assign=on_assign(phase, threading.Event()))
    poll_until(c, phase, got, want)
    if drain:
        end = time.time() + 2
        while time.time() < end:
            emit_msg(c.poll(0.2), phase, got)
    err = None
    try:
        res = c.commit(asynchronous=False)
        for tp in res or []:
            if tp.error is not None:
                err = tp.error
    except KafkaException as e:
        err = e
    emit("commit", phase=phase, error_code=code_of(err), error=str(err or ""))
    c.close()


def scenario():
    if ADMIN:
        create(PARTITIONS, 1)
    p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True})
    produce(p, 1, 0, RECORDS, True)
    consume(1, RECORDS, False)
    produce(p, 2, RECORDS, RECORDS + RECORDS2, True)
    consume(2, RECORDS2, True)


def stock():
    if ADMIN:
        create(-1, -1)
    got = set()
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP})
    assigned = threading.Event()
    held = []
    c.subscribe([TOPIC], on_assign=on_assign(1, assigned, held))
    deadline = time.time() + TIMEOUT
    while not assigned.is_set():
        if time.time() > deadline:
            fail("consume", -1, "consumer was never assigned partitions")
        emit_msg(c.poll(0.2), 1, got)
    # Readiness barrier. librdkafka's default auto.offset.reset is latest and position() reports
    # only the last consumed offset + 1, so the barrier is the cached high watermark: librdkafka
    # learns it from the first fetch (or offset lookup) of a partition, which needs the start
    # position resolved. On the fresh topic that watermark is where the consumer starts.
    pending = set(held)
    while pending:
        if time.time() > deadline:
            fail("consume", -1, "start position never resolved on partitions %s" % sorted(pending))
        emit_msg(c.poll(0.2), 1, got)
        for p in list(pending):
            wm = c.get_watermark_offsets(TopicPartition(TOPIC, p), cached=True)
            if wm is not None and wm[1] >= 0:
                emit("position", phase=1, partition=p, offset=wm[1])
                pending.discard(p)
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    produce(p, 1, 0, RECORDS, False)
    poll_until(c, 1, got, RECORDS)
    c.close()


def main():
    if not (BOOTSTRAP and TOPIC and GROUP and MODE in ("scenario", "stock-defaults")):
        fail("inputs", -1, "KCERT_BOOTSTRAP, KCERT_TOPIC, KCERT_GROUP and KCERT_MODE are required")
    # librdkafka's documented default for group.protocol is classic; the scenario sets it explicitly
    # (the allowed deviation), stock defaults leave it unset.
    emit("start", client=CLIENT, version=versions(), mode=MODE, group_protocol="classic")
    try:
        if MODE == "scenario":
            scenario()
        else:
            stock()
    except KafkaException as e:
        fail("driver", code_of(e), e)
    emit("done")


if __name__ == "__main__":
    main()
