# Platform tools track

The `tools` track runs Apache Kafka's own platform tools, unmodified, against the Kafka oracle
first and then against KubeMQ with only the bootstrap address changed. The optional
`schema-registry` track does the same for Confluent Schema Registry. Rows live in
`matrix/tools.yaml` and `matrix/schema-registry.yaml`; the code is in `suites/tools/`.

## What runs

Every program under test comes from the pinned Apache Kafka 4.3.1 image (`compose/versions.env`):

| Group | Program | Rows |
|---|---|---|
| Kafka CLI | `kafka-topics.sh`, console producer and consumer, `kafka-consumer-groups.sh`, `kafka-configs.sh`, `kafka-get-offsets.sh`, `kafka-delete-records.sh`, verifiable producer and consumer, perf tests, `kafka-broker-api-versions.sh` | `tools.cli.*` |
| Kafka CLI, cluster administration | `kafka-leader-election.sh`, `kafka-metadata-quorum.sh`, `kafka-transactions.sh`, `kafka-reassign-partitions.sh`, `kafka-log-dirs.sh`, `kafka-features.sh`, `kafka-delegation-tokens.sh`, `kafka-configs.sh` on the broker, `kafka-cluster.sh` | `tools.cli.*` |
| Kafka Connect | `connect-distributed.sh` with the image's file connectors, two workers | `tools.connect.*` |
| Kafka Streams | the image's `WordCountDemo`, `exactly_once_v2` | `tools.streams.*` |
| MirrorMaker 2 | `connect-mirror-maker.sh`, with a second real Kafka 4.3.1 broker, both directions | `tools.mm2.into.*`, `tools.mm2.from.*` |
| Internal topics | `__consumer_offsets` and `__transaction_state` read with Kafka's own formatters | `tools.ops.*` |
| Lag and monitoring | Burrow v1.9.4, kafka-lag-exporter 0.8.2, Kafbat UI v1.4.2 | `tools.lag.*` |
| Schema Registry (opt-in) | Confluent Schema Registry 8.3.2 | `schema-registry.*` |

Each row's observation is a small set of facts (exit codes, exception classes, counts, offsets,
booleans), never timings or identifiers. Every probe also declares what real Kafka must show
(`suites/tools/probe/expect.go`): when the oracle misses it, the row is VOID, so a broken
instrument can never produce a PASS. A container-engine failure or a deadline is infrastructure:
the row is NOT-RUN, never a verdict.

Long, stateful programs (Connect, Streams, MirrorMaker 2, the lag tools, Schema Registry) run once
per broker as a scenario; each row is one phase of it. Every container goes through
`suites/tools/ctr.sh`, which calls only the engine abstraction (`runner/lib/engine.sh`); every tool
call (command, exit code, output) is logged to `logs/tools-calls.log` in the run directory,
never into git.

## Running

```bash
KUBEMQ_LICENSE_KEY=<key> runner/certify.sh --track tools
```

About 50 minutes on an 8-vCPU amd64 machine with Docker. `TOOLS_ONLY=<row id>,<row id>` limits a
run to some rows (the rest are NOT-RUN; for development only).

### Third-party images

Burrow, kafka-lag-exporter and Kafbat UI are third-party open-source images, pinned by digest in
`compose/versions.env`, pulled at run time from their publishers, never redistributed. Burrow and
kafka-lag-exporter publish linux/amd64 only; on arm64 hosts they run emulated.
kafka-lag-exporter is archived upstream (last release 0.8.2).

### Schema Registry (opt-in)

The Schema Registry image (`confluentinc/cp-schema-registry`) is under the Confluent Community
License, which is not an open-source license. The kit never ships it; the operator pulls it from
Confluent at run time, and only after reading and accepting that license:

```bash
CERT_ACCEPT_CONFLUENT_COMMUNITY_LICENSE=yes KUBEMQ_LICENSE_KEY=<key> \
  runner/certify.sh --track schema-registry
```

Setting the variable records that you have read and accepted that license; the decision is
yours.

`runner/certify.sh --full --include-opt-in` also runs it when the variable is set. An opt-in
track that did not run is listed as such and never fails a report; one that ran is judged like
any other track. Without the variable the runner does not start the track, and the track itself refuses to pull the image and
records every row NOT-RUN with the reason. The track uses the tools track's driver and probes.
Schema Registry runs its own bundled kafka-clients; the row records that its log carries the
client banner, not which version it is.

## Instrument facts (found on real Apache Kafka 4.3.1; kept so nobody rediscovers them)

- FileStreamSource records its file position in characters and seeks in bytes on restart, so the
  Connect input is ASCII only; multi-byte input re-reads a tail of the file on real Kafka.
- The `apache/kafka` image has no libstdc++, so Streams' RocksDB cannot load; the WordCount app
  uses the in-memory store suppliers, still backed by and restored from the changelog.
- An unkeyed console producer sends every line to one partition, so Streams input is keyed.
- A Connect worker asking for an unmeetable replication factor retries for Connect's own 60 s,
  then its herder dies with a creation timeout; Kafka 4.3.1's worker then hangs in shutdown, so the
  row stops it itself.
- MirrorMaker 2 translates a committed offset exactly only at a sync point; the rows commit the
  source group at the end of every partition.
- Burrow v1.9.4 (the latest release) cannot decode Kafka 4.x offset-commit records in
  `__consumer_offsets`: on Kafka itself it lists groups but has no lag for them.
- `kafka-delegation-tokens.sh` needs `--command-config` even on a plaintext listener.
- `kafka-cluster.sh unregister` exits 0 while printing that the cluster does not support the API.
- `kafka-delete-records.sh` is refused (POLICY_VIOLATION) on a compacted topic; the row uses a
  delete-policy topic.
- The kit runs both brokers with `offsets.retention.minutes=1` (for the features track's retention
  row), so an idle group's committed offsets expire about a minute after its consumer leaves. The
  lag rows make a fresh group per tool once the tool answers and read it within that minute.
- Burrow's consumer module gives up for good if `__consumer_offsets` is missing or still electing
  leaders when it starts; the row makes a warm-up group first and waits for the topic's leaders.
- kafka-lag-exporter 0.8.2 (kafka-clients 3.2.3) runs its admin client with zero retries, so any
  request that needs one retry fails the whole poll.
