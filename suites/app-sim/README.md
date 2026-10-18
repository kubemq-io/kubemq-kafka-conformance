# App-sim track

An ordinary Kafka application, in Java (`kafka-clients`) and in Python (`confluent-kafka`, librdkafka),
run first against the Kafka oracle and then against KubeMQ with only the bootstrap address changed. It
answers the question a team replacing Kafka has: will our existing application keep working?

Ported from kubemq-server `tests/kafka-app-sim` (commit fcd45d818). The applications and `verify.py`
are unchanged; `track.sh` replaces the podman-only `run.sh` with the engine abstraction and the kit's
single-node brokers (replication factor 1). The three-broker and fault-injection runs of the original
are not ported (the cluster track that covered them was removed).

| Step | Java | Python |
|---|---|---|
| Setup | AdminClient creates `orders` (12 partitions), `orders-enriched` (12), `orders-dlq` (3, compacted), changes and reads back `retention.ms` | creates `payments` (6 partitions), reads a topic config |
| Produce | idempotent, `acks=all`, gzip then lz4 | idempotent, `acks=all`, snappy then zstd |
| Consume | a group of 3 members; one leaves, one joins later, so the group rebalances twice under traffic | the same |
| Exactly once | consume-transform-produce with input offsets committed in the transaction; every 5th transaction aborted on purpose | the same |

The applications never judge themselves: every acknowledged send, delivered record, assignment,
commit and admin call goes into a JSON-lines ledger under `/out`, and `verify.py` decides PASS, FAIL
or VOID from the ledgers plus a fresh reader that is a party to nothing else. `verify.py --self-test`
proves it fails on 42 injected defects; it runs in `pr.yml`.

Verdict of the row `app-sim.orders-apps`: the oracle run must PASS (otherwise VOID); then the KubeMQ
run's verdict is the row's (FAIL for anything but PASS). Each run's ledgers, logs, `instrument.txt`
and `report.md` land in `<run-dir>/transcripts/app-sim/<oracle|kubemq>/`.

Run only this track: `runner/certify.sh --track app-sim`. Pins: `app-sim.env`.
