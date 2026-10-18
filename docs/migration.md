# Migrating from Apache Kafka to KubeMQ, and back

Every step that claims a result names the row that proves it, in `matrix/migration.yaml` or in
another track. The migration rows run on a single node, and every one of them behaves on KubeMQ
exactly as a migration between two Apache Kafka 4.3.1 clusters does.

## 1. Before you start: is the workload a fit?

Everything the suite covers behaves on KubeMQ as on Kafka, except the optional features listed
in `docs/gaps.md`. Check whether the workload uses one of them:

| Check | What to do | Evidence |
|---|---|---|
| Kafka Streams with `group.protocol=streams` (KIP-1071) | Run Streams with the classic rebalance protocol, the Kafka Streams default | `apikey.88`, `apikey.89` (OPTIONAL) |
| Kerberos (SASL/GSSAPI) clients | Move those principals to SCRAM or mTLS | `security.gssapi.configurable` (OPTIONAL) |
| Delegation tokens (Spark, Flink on secured Kafka) | Give workers their own SCRAM credentials or mTLS certificates | `security.delegation-tokens.create-and-login` (OPTIONAL) |
| OAUTHBEARER over SASL_PLAINTEXT | Connect to the TLS port, or set `CONNECTORS_KAFKA_OAUTH_BEARER_ALLOW_PLAINTEXT=true` | `security.oauthbearer.plain-port` (OPTIONAL) |
| Topics created with a fixed `compression.type` | Set the codec on the producer; KubeMQ stores batches in the producer's codec | `features.produce.broker-recompression` (OPTIONAL) |

Records larger than 1 MiB are refused by default on both brokers. Raise
`CONNECTORS_KAFKA_MAX_MESSAGE_BYTES` on KubeMQ (Kafka's `message.max.bytes`) and the client
limits together (`features.produce.max-message-bytes`).

**Size-bounded topics.** KubeMQ honours `retention.bytes`, also on `cleanup.policy=compact,delete`
topics (`features.log.retention-bytes`, `features.log.retention-bytes-compact-delete`), and
MirrorMaker 2 creates a size-bounded topic on KubeMQ with its bound, partition count and records,
exactly as on a Kafka target (`migration.mm2.config-refusal`).

## 2. Cutover with MirrorMaker 2 (Kafka to KubeMQ)

1. **Prepare KubeMQ.** Create topics up front with the source's partition counts and topic
   configs, or let MirrorMaker 2 create them.
2. **Mirror.** Run MirrorMaker 2 with Kafka as the source cluster and KubeMQ as the target, with
   `sync.group.offsets.enabled=true` and `emit.checkpoints.enabled=true`. MirrorMaker 2 creates
   its offset-syncs, checkpoints and heartbeats topics on KubeMQ as compacted topics.
3. **Verify the copy.** For every topic: partition count, per-partition end offset, and every
   record's offset, key, value, headers and timestamp equal on both sides. Proven for a mirror
   into KubeMQ by the tools track's `tools.mm2.into.*` rows (partition counts, end offsets, every
   record's key, value and headers) and here by `migration.cutover.resume.*` (`offsets_identical`).
4. **Stop producers on Kafka.** Wait until MirrorMaker 2 lag is 0 for every partition.
5. **Stop consumers on Kafka** after they commit. Wait one checkpoint interval.
6. **Translate committed offsets.** MirrorMaker 2 writes translated offsets for each group to
   KubeMQ. Verify each group's committed offset on KubeMQ is at or below the source offset for
   every partition, never above. It equals the source offset only where the group committed at a
   point MirrorMaker 2 recorded an offset sync; elsewhere it lands earlier (consumers then re-read
   a few records, never skip). Row: `migration.offsets.mid-log`.
7. **Start consumers on KubeMQ** with the same group ids, only the bootstrap address changed, and
   `auto.offset.reset=earliest`. Since Kafka 3.5 MirrorMaker 2 keeps sparse offset syncs, so a
   group committed far back in a partition may get no translated offset for that partition; with
   `earliest` the consumer re-reads that partition from the start instead of skipping to its end
   (the default `latest` would silently skip records).
   They must resume at the translated offset: no record skipped, duplicates only below the
   translation point. Rows: `migration.cutover.resume.franz-go`,
   `migration.cutover.resume.kafka-clients` (Kafka's own Java console consumer).
8. **Start producers on KubeMQ.**
9. **Keep Kafka read-only** until the rollback window closes.

Compacted topics: the mirrored copy keeps the latest value per key and the deletes
(`migration.mm2.compacted-fidelity`). Transactional topics: with default settings MirrorMaker 2
reads the source with `read_uncommitted` and writes without transactions, so **records of aborted
transactions arrive on the target as ordinary, visible records** — on KubeMQ exactly as on a Kafka
target (`migration.mm2.transactional-fidelity`: 40 committed and 20 aborted records on both).
Set `src.consumer.isolation.level=read_committed` in the MirrorMaker 2 configuration before
mirroring transactional topics.

## 3. Rollback (KubeMQ to Kafka)

Before any producer writes to KubeMQ, rollback is trivial: point clients back at Kafka, whose
data and offsets are untouched. After KubeMQ has taken writes, the same procedure runs in
reverse, with KubeMQ as the MirrorMaker 2 source and a Kafka cluster as the target (a fresh one,
or the old one with the topics deleted: mirroring onto an existing topic of the same name would
append duplicates). Row: `migration.rollback.end-to-end` — after KubeMQ took 60 more records and
the group committed mid-log there, the reverse mirror reproduced every record (same partition,
offset, key, value and headers) and the group resumed on Kafka without skipping.

## 4. Configuration mapping

See `docs/operations.md` section 3 for every `server.properties` and topic setting and its
KubeMQ equivalent.

## 5. ACLs

KubeMQ serves Kafka's ACL admin APIs: DescribeAcls lists the rules, and CreateAcls and DeleteAcls
change them at run time (`security.acl.admin-apis`). Recreate the source cluster's ACLs on KubeMQ
with `kafka-acls.sh` or the Admin API, or write them as KubeMQ authorization policy rules;
authentication and authorization must be enabled on KubeMQ for them to apply. The security
track's `security.acl.*` rows show allowed and denied produce, consume, topic creation,
idempotent and transactional producers, DENY rules and metadata visibility behaving as on Kafka.

## 6. Rows

| Row | What it proves | Machine |
|---|---|---|
| `migration.offsets.mid-log` | groups committed mid-partition while records keep arriving: no translated offset is ahead of the source commit | single node |
| `migration.cutover.resume.franz-go` | a franz-go consumer resumes at the translated offsets: nothing skipped, no duplicates, in order | single node |
| `migration.cutover.resume.kafka-clients` | the same with Kafka's own Java console consumer | single node |
| `migration.mm2.compacted-fidelity` | latest value per key and deletes preserved | single node |
| `migration.mm2.transactional-fidelity` | what arrives from committed and aborted transactions with default MirrorMaker 2 settings (aborted records arrive too, on Kafka and KubeMQ alike) | single node |
| `migration.mm2.config-refusal` | a 3-partition source topic with `retention.bytes` mirrors with its partition count, bound and records | single node |
| `migration.rollback.end-to-end` | reverse mirror after KubeMQ took writes; records identical, group resumes on Kafka | single node |
