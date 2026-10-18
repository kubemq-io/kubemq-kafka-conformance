# Transaction scenarios

Every client driver (franz-go in `franz/`, and later Java kafka-clients and librdkafka) implements
the scenarios below exactly as written and reports **facts only**: what each reader received
(partition, offset, key, value), the offsets the broker listed, and the outcome of each
transactional call as a Kafka error name (`NONE` for success; a client-side failure that carries
no Kafka error is `CLIENT:<message>`). The driver prints one JSON line as the last line of its
standard output:

```json
{"client": "<library> <version actually loaded>", "scenario": "<name>", "facts": { ... }}
```

`verify/` judges the facts, once for every client, and turns them into the observation that is
compared between the Kafka oracle and KubeMQ. A driver never judges itself.

Conventions for every scenario:

- Each scenario creates its own fresh topics (one copy) and uses a fresh `transactional.id`, both
  unique per run and per broker.
- Keys are `<prefix>-<NN>` (two digits); the value of a record is `v-<key>`.
- Readers are fresh consumers with no group, assigned the partitions from the start, and read until
  no record arrived for 4 seconds.

## isolation

One transactional producer, one fresh 1-partition topic.

1. Begin; send `c-00`..`c-05` and wait for their acknowledgements. While the transaction is still
   open, list partition 0's last stable offset (`open_lso`, read_committed latest) and high
   watermark (`open_hwm`, read_uncommitted latest). Commit (`outcomes.commit1`).
2. Begin; send `a-00`..`a-03`, wait for acknowledgements; abort (`outcomes.abort`).
3. Begin; send `c-06`..`c-08`, wait; commit (`outcomes.commit2`).
4. Wait one second, list both offsets again (`final_lso`, `final_hwm`).
5. Read the partition with read_committed (`read_committed`) and with read_uncommitted
   (`read_uncommitted`).

On Apache Kafka 4.3.1: `open_lso` 0, `open_hwm` 6, both final offsets 16 (each commit or abort
marker takes one offset), read_committed delivers offsets 0-5 and 12-14, read_uncommitted 0-5,
7-10 and 12-14, every call `NONE`.

Facts: `open_lso`, `open_hwm`, `final_lso`, `final_hwm` (integers), `outcomes` (object of call
name to error name), `read_committed`, `read_uncommitted` (arrays of
`{"partition","offset","key","value"}` in delivery order).

## send-offsets

Input topic (1 partition) holds `i-00`..`i-09`, produced without a transaction. One
transactional producer, output topic (1 partition), group `G` that has no members. Offsets are
sent the way Java's `sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(G))` sends them:
AddOffsetsToTxn, then TxnOffsetCommit with generation -1 and no member id.

1. Begin; send `o-00`..`o-04`; send offset 5 for input partition 0 (`outcomes.txn1_send_offsets`);
   commit (`outcomes.txn1_end`). Fetch the group's committed offset (`after_commit1`).
2. Begin; send `o-05`..`o-09`; send offset 10 (`txn2_send_offsets`); while open, fetch the group's
   offset with requireStable (`stable_fetch_while_open`); abort (`txn2_end`). Fetch (`after_abort`).
3. Begin; send `o-05`..`o-09` again; send offset 10 (`txn3_send_offsets`); commit (`txn3_end`).
   Fetch (`after_commit2`).
4. Read the output with read_committed (`read_committed`).

`after_*` fetches require stable offsets and are retried while the broker answers
UNSTABLE_OFFSET_COMMIT, for up to 15 seconds: the broker applies a transaction's offsets when it
writes the commit marker, shortly after the commit call returns. `stable_fetch_while_open` is a
single fetch. A group offset fact is the offset as a decimal string, `none` when nothing is
committed, or the error name. Kafka 4.3.1: `5`, `UNSTABLE_OFFSET_COMMIT`, `5`, `10`; read_committed `o-00`..`o-09`
once.

## consume-transform-produce

Input topic (3 partitions) holds `i-00`..`i-59`, record n on partition n mod 3, produced without a
transaction. Output topic has 3 partitions. One transactional consumer-producer in a fresh group,
read_committed, requiring stable offsets, starting from the earliest offset:

- poll at most 5 records; begin; for each `i-NN` send `o-NN` to the same partition of the output;
  send the polled offsets to the transaction; end the transaction;
- every third transaction is aborted on purpose (after its records were acknowledged), after which
  the client resumes from the group's committed offsets;
- stop when 60 outputs are committed, or after 120 seconds.

Facts: `commits`, `aborts`, `first_error` (`<call>:<error name>` of the first failed call, `NONE`),
`group_offsets` (partition as a string to committed offset, read after the session closed),
`read_committed`, `read_uncommitted` of the output topic.

## fencing

One fresh 1-partition topic, one transactional id `T`.

1. Producer A (`T`): begin, send `z-00`..`z-02`, wait for acknowledgements.
2. Producer B (`T`): initialize (`outcomes.b_init`). This fences A and aborts its transaction.
3. A sends `z-03` (`a_send_after_fence`), then commits (`a_commit_after_fence`), each bounded by
   30 seconds.
4. B: begin, send `b-00`, `b-01`, commit (`b_commit`).
5. Wait one second; list `final_lso`, `final_hwm`; read with read_committed and read_uncommitted.

## timeout-abort

One fresh 1-partition topic, one transactional id `T`, `transaction.timeout.ms=3000`.

1. Begin, send `t-00`..`t-02`, wait for acknowledgements, then send nothing more.
2. Every second for up to 45 seconds list the last stable offset and high watermark;
   `aborted_by_broker` is true when they become equal.
3. Commit (`outcomes.late_commit`, bounded by 30 seconds); close the producer.
4. A new producer with `T` and a 60 s timeout: begin, send `u-00`
   (`new_producer_send` only if it fails), commit (`new_producer_commit`).
5. Wait one second; list `final_lso`, `final_hwm`; read with read_committed and read_uncommitted.

## timeout-limit

Raw InitProducerId with a fresh transactional id and transaction timeout 900000 ms
(`outcomes.at_max_900000`), and again with another fresh id and 900001 ms
(`above_max_900001`), each retried while the coordinator is loading. A client library that only
reports the refusal on its first transactional call reports that call's error instead.

## transaction-version

1. ApiVersions: `finalized` is the finalized `transaction.version` max level (-1 when absent),
   `supported_max` the supported max version (-1 when absent).
2. A transactional producer on a fresh 1-partition topic initializes and records its epoch, then
   twice: begin, send two records (`v-00`,`v-01`, then `v-02`,`v-03`), commit
   (`outcomes.commit1`, `commit2`), record the epoch. `epochs` holds the three epochs.
3. Read with read_committed.

## compacted-read

One transactional producer, one fresh 1-partition topic with `cleanup.policy=compact`,
`segment.ms=1000`, `min.cleanable.dirty.ratio=0.01`, `delete.retention.ms=5000`,
`min.compaction.lag.ms=0` (the features track's compaction settings). Values are
`<transaction>:<key>`, not `v-<key>`.

1. Commit `k-00`..`k-04` = `1:<key>`; abort `k-00`..`k-04` = `aborted:<key>`; commit
   `k-00`..`k-02` = `3:<key>`.
2. Every 4 seconds for up to 120 seconds: commit one record with key `roll` (so segments roll and
   become cleanable), then read the partition with read_committed and read_uncommitted (3 s
   quiet). Stop when every `k-*` key has exactly one read_committed record and no `aborted:` value
   is left under read_uncommitted (`settled`).
3. Facts: `settled`, and the last `read_committed` and `read_uncommitted` reads.

On Kafka 4.3.1 the cleaner keeps `k-00`..`k-02` = `3`, `k-03`, `k-04` = `1` and discards the aborted
data; read_committed never shows an aborted value. The `roll` key is ignored by the verifier.

## idempotent-sequences (franz-go only, raw wire)

Hand-built uncompressed v2 record batches for partition 0 of a fresh topic, acks=all, sent with
Produce v12 at most (v13+ names topics by id only), an
idempotent producer id from InitProducerId without a transactional id (`outcomes.01_init`), epoch
`e`. Outcome of a produce is `NONE@<base offset>` or the error name.

| Step | Batch | Outcome key |
|---|---|---|
| 02 | epoch e, sequence 0: `k-00`..`k-02` | `02_seq0` |
| 03 | the same batch again | `03_seq0_resent` |
| 04 | epoch e, sequence 5: `x-00` | `04_seq5_gap` |
| 05 | epoch e, sequence 3: `k-03`, `k-04` | `05_seq3` |
| 06 | epoch e+1, sequence 0: `k-05` | `06_next_epoch_seq0` |
| 07 | epoch e, sequence 5: `x-01` | `07_old_epoch_seq5` |
| 08 | epoch e+1, sequence 1: `k-06` | `08_next_epoch_seq1` |

Then the partition is read with read_uncommitted (`read`).

## epoch-errors (franz-go only, raw wire)

Fresh topic, transactional id `T`. InitProducerId(`T`) twice (`01_init`, `02_second_init`):
`same_producer_id` and `epoch_delta` compare the two answers. Then, with the first (stale) and
the second (current) epoch in turn: AddPartitionsToTxn for partition 0 (`03`, `04`), a
transactional produce (`05` with `x-00`, `06` with `e-00`, `e-01`), EndTxn commit (`07`, `08`), and
InitProducerId(`T`) presenting the producer id and the stale epoch (`09_init_with_old_epoch`).
Then read_committed.
## upstream-copier (Kafka's own tools)

The path of Apache Kafka's transactions system test (`tests/kafkatest/tests/core/transactions_test.py`)
with Kafka's own tools from the pinned image (`upstream/copier.sh`): input topic 2 partitions,
output topic 3 partitions; `kafka-verifiable-producer` seeds values `0`..`999` with acks=all; one
`TransactionalMessageCopier` per input partition (`--transaction-size 50 --enable-random-aborts
--use-group-metadata`, one consumer group); `kafka-console-consumer --isolation-level
read_committed --from-beginning --timeout-ms 20000` reads the output. Facts: `seeded`,
`produced_acked` (success events of the verifiable producer), `copier_exits`,
`read_committed_values`. The ducktape test also bounces brokers and kills copiers; those failures
are out of scope (the cluster track that covered them was removed).

## Client notes

- Java (`java/TxnDriver.java`, kafka-clients 4.3.1 from the Kafka image) implements isolation,
  send-offsets, consume-transform-produce (classic group protocol, the allowed deviation), fencing,
  timeout-abort and timeout-limit. Group offsets come from the admin client with requireStable,
  which itself retries while offsets are unstable (15 s for `after_*`, 5 s for the fetch while
  open). Error names are `Errors.forException` of the first Kafka exception in the cause chain.
- transaction-version, idempotent-sequences and epoch-errors need raw protocol access and run with
  franz-go only.
- librdkafka (`python/txn_driver.py`, confluent-kafka 2.15.1 with its bundled librdkafka 2.15.1,
  installed from hash-pinned wheels) implements the same six scenarios as Java. Group metadata
  for send-offsets comes from a consumer with the group id that never subscribes (generation -1,
  no member id). Error names are librdkafka's names for broker errors; a librdkafka-local error
  (negative code) is `CLIENT:<name>`.
