# Client scenario, version 1

Every client in `matrix/clients.yaml` runs the same two scenarios, written once here and
implemented by a small driver per client under `drivers/<name>/`. A driver is an ordinary
application using the client library as its upstream documentation shows. It does not judge
itself: it writes every observable fact as one JSON object per line (the **ledger**) on standard
output, and `cmd/clients-track` decides the verdict from the ledger plus its own **fresh reader**
of the log, which is a party to nothing else (no group, every partition from the start to the high
watermark, then an OffsetFetch of the driver's group).

Each row runs on the Kafka oracle first, then on KubeMQ with only the bootstrap address changed.
The driver runs in a container (`runner/client-run.sh`), so it reaches both brokers on the
host-gateway alias.

## Inputs (environment)

| Variable | Meaning |
|---|---|
| `KCERT_BOOTSTRAP` | bootstrap address, the only thing that differs between the two legs |
| `KCERT_MODE` | `scenario` or `stock-defaults` |
| `KCERT_TOPIC` / `KCERT_GROUP` | fresh names per leg |
| `KCERT_PARTITIONS` | partitions to create (scenario: 3; stock-defaults: the topic is created with the broker default) |
| `KCERT_RECORDS` / `KCERT_RECORDS2` | records in produce phase 1 and phase 2 (scenario: 300 and 100; stock-defaults: 10 and 0) |
| `KCERT_KEYS` | distinct keys (20) |
| `KCERT_TIMEOUT_SEC` | the driver's own bound on each wait |
| `KCERT_ADMIN` | `1` when the driver creates the topic itself; `0` when the client has no admin API and the track creates it before the driver starts |

## Record content

Record `seq` (0-based over both phases): key `key-NN` with `NN = seq mod KCERT_KEYS` (two digits),
value `value-NNNNNN` (six digits), headers `seq` = decimal `seq` and `client` = the driver name, in
that order. Stock-defaults records carry no key and no headers, as the upstream quickstart sends
them (value only).

## Mode `scenario`

Settings are identical on both brokers and are part of the scenario (a typical application's
configuration), not deviations: producer `acks=all` with idempotence where the client has it;
consumer `auto.offset.reset=earliest`, auto commit off, explicit synchronous commit; the only
allowed client-config deviation is `group.protocol=classic` (`docs/methodology.md`).

1. `create` the topic with `KCERT_PARTITIONS` partitions, replication factor 1 (when `KCERT_ADMIN=1`).
2. Produce phase 1: records `0 .. RECORDS-1`; one `ack` per record.
3. Consume phase 1: a group member subscribes, receives until it holds `RECORDS` records, commits
   synchronously, closes (leaves the group).
4. Produce phase 2: records `RECORDS .. RECORDS+RECORDS2-1`.
5. Consume phase 2: a new member of the same group subscribes, receives until it holds `RECORDS2`
   records, keeps polling 2 seconds more (so a member that resumed from the wrong offset is seen
   doing it), commits, closes.

## Mode `stock-defaults`

Zero overrides: only the bootstrap address, the group id and, where the client requires them,
serializers. Whatever the pinned client defaults to (including the consumer group protocol) is
what runs, and the driver records it.

1. `create` the topic with the broker's default partitions and replication (when `KCERT_ADMIN=1`).
2. A consumer subscribes and polls until it holds a non-empty assignment, then waits until its
   start position is resolved on every assigned partition. Its default `auto.offset.reset` is
   usually `latest`, so its position must be fixed before anything is sent, or the row would race
   the producer. The barrier is the client's own signal, never a fixed sleep: Java and Spring call
   `position()` (a blocking call, not a setting); librdkafka clients (Python, Go, .NET) wait until
   the cached high watermark is known, which librdkafka learns only from a fetch or offset lookup
   that needs the position; kcat waits for its "Reached end of topic" line; KafkaJS waits for its
   first fetch after joining; sarama's claims start only after their offsets are resolved;
   franz-go and kafka-go default to the earliest offset, so they need none. Where the client gives
   a number, the driver records it as a `position` event, and the verifier checks it is resolved
   and not after the first record sent.
3. A producer sends records `0 .. RECORDS-1`.
4. The consumer receives until it holds `RECORDS` records, then closes. Commits are whatever the
   client does by default.

## Mode `verifiable` (upstream provenance)

Kafka's own `kafka-verifiable-producer` and `kafka-verifiable-consumer` from the pinned oracle
image, the clients Apache Kafka's ducktape system tests drive, run as those tests run them: the
track creates the topic (3 partitions); the producer sends 300 records with `--acks -1` and
`--repeating-keys 20` (value = the record number, key = the number modulo 20); the consumer joins a
group with `--group-protocol classic --reset-policy earliest --verbose`, commits after each batch
and stops after 300 records. The track turns the tools' JSON events into the ledger below
(`verify/verifiable.go`), so the same checks judge it: every acknowledged record delivered once, in
partition order, every commit successful, committed offsets at the log end, keys on one partition
in send order.

## Ledger events

One JSON object per line on standard output. Anything else (logs) goes to standard error.

| `ev` | Fields | When |
|---|---|---|
| `start` | `client`, `version` (the version the driver actually loaded, read from the library), `mode`, `group_protocol` (the protocol the consumer runs with: read from the client's effective configuration where it exposes one, as Java, Spring, franz-go and kcat do; otherwise the client's documented default: librdkafka's language bindings expose no configuration getter, and sarama, kafka-go and KafkaJS implement only the classic protocol) | first line |
| `create` | `topic`, `partitions`, `error_code` | after the create call |
| `ack` | `phase`, `seq`, `partition`, `offset`, `error_code`, `error` | per produced record |
| `assigned` | `phase`, `partitions` (sorted list) | on each assignment |
| `recv` | `phase`, `seq` (from the `seq` header, else parsed from the value), `partition`, `offset`, `key`, `value`, `headers` (list of `[name, value]`) | per delivered record |
| `position` | `phase`, `partition`, `offset` | stock defaults: the consumer's resolved start position before anything is sent |
| `commit` | `phase`, `error_code`, `error` | after each explicit commit |
| `error` | `step`, `error_code`, `error` | a step failed; the driver stops |
| `done` | — | last line; its absence means the driver crashed or was killed |

`error_code` is the Kafka protocol error code (0 for success, -1 when the client reports an error
that carries no Kafka code).

## What the verifier checks (any failure is a named problem)

- The ledger starts with `start` and ends with `done`; the loaded client version equals the pinned one.
- Every create and every commit succeeded; no `error` event.
- Every record was acknowledged exactly once, without error, with an offset; no (partition, offset)
  was handed out twice; every key's records sit on one partition in send order.
- Fresh reader: the log holds exactly the acknowledged records, each once, at the acknowledged
  partition and offset, with the content above and the producer's create time (not log-append
  time) inside the leg's own wall-clock window; the partition count is the one asked for.
- Each consume phase delivered exactly its phase's records, each once, in offset order within a
  partition, at the acknowledged position, with the content above, only from assigned partitions;
  phase 2 delivered nothing from phase 1 (it resumed from the committed offsets).
- The group's committed offsets equal the high watermarks at the end (scenario and verifiable).
- Stock defaults: a reported start position is resolved and not after the first record sent.

### Client limits a matrix row may allow

Some clients cannot do a step the way the scenario says. The row lists the limit (`limits:` in
`matrix/clients.yaml`, published with the row); the driver declares it; the verifier adapts only
that check. A declaration the row does not allow is a problem.

| Limit | Meaning | What the verifier does instead |
|---|---|---|
| `assignment-hidden` | the consumer API does not expose its group assignment | skips only the assigned-partition check |
| `ack-offsets-unknown` | the producer reports no offsets (acks=0 default, or a tool that prints none) | takes each record's position from the fresh reader, then runs every position check against it; the per-partition record counts are compared with the oracle's |
| `headers-per-run` | a command-line tool sets headers per run | expects only the `client` header |
| `commit-on-close` | the consumer commits only on close, by the client's auto commit | needs no `commit` event; still checks the committed offsets |

The leg's **observation** (counts, partition count, records per partition for keyed rows, stored
timestamp type, group protocol, committed-at-end) is then compared field by field between the
oracle and KubeMQ, exactly as in the features track.

Verdicts: oracle leg with any problem → VOID; KubeMQ leg with any problem → FAIL; both clean and
observations identical → PASS; observations differ → GAP if registered and signed in
`docs/gaps.md` with exactly that difference (reported as the row's `deviation`), otherwise FAIL.
A driver that cannot be built ran on neither broker: NOT-RUN.
