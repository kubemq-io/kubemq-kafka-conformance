# Operating KubeMQ in place of Apache Kafka

Every mapping cites its row in `matrix/operations.yaml` (or another track), run on Kafka 4.3.1 and
KubeMQ with the same workload; verdicts are in `results/REPORT.md`. A mapping marked "not a row"
is not measured by this suite.

This page is for the team that runs Kafka today: what to scrape, what to alert on, and which
Kafka settings have a KubeMQ equivalent.

## 1. Where the metrics are

| | Apache Kafka | KubeMQ |
|---|---|---|
| Format | JMX MBeans, usually exported to Prometheus by a JMX exporter agent | Prometheus text format, built in |
| Endpoint | the exporter's port | `http://<node>:8080/metrics` (the API port) |
| Granularity | per broker, per topic, per partition, per request type | node totals, plus per-topic and per-partition series for records and bytes, log end offset and log size |
| Consumer lag | not a broker metric; computed by `kafka-consumer-groups.sh` or a lag exporter | exported by the broker: `kubemq_kafka_consumer_group_lag{group,topic,partition}` |

Per-topic throughput panels read `kubemq_kafka_topic_messages_total{op,topic,partition}` and
`kubemq_kafka_topic_message_bytes_total{op,topic,partition}`; lag panels can read the broker
directly instead of a lag exporter.

## 2. Metric mapping

Rows marked with a row id are verified by `matrix/operations.yaml`; "none" means KubeMQ exports
no equivalent series.

| Kafka metric (MBean) | KubeMQ series | Row | Notes |
|---|---|---|---|
| `BrokerTopicMetrics MessagesInPerSec` | `kubemq_kafka_messages_total{op="produced"}` | `operations.metrics.messages-in` | records, node total |
| `BrokerTopicMetrics MessagesInPerSec{topic}` | `kubemq_kafka_topic_messages_total{op="produced",topic,partition}` | `operations.metrics.per-topic-labels` | per topic and partition |
| `BrokerTopicMetrics BytesInPerSec` | `kubemq_kafka_message_bytes_total{op="produced"}` | `operations.metrics.bytes-in` | record-batch bytes on the wire |
| `BrokerTopicMetrics BytesOutPerSec` | `kubemq_kafka_message_bytes_total{op="fetched"}` | `operations.metrics.bytes-out` | non-empty fetches only |
| `TotalProduceRequestsPerSec`, `TotalFetchRequestsPerSec` | `kubemq_kafka_operations_total{operation="produce"\|"fetch"}` | `operations.metrics.produce-requests`, `fetch-requests` | KubeMQ counts per partition, not per request |
| `FailedProduceRequestsPerSec` | not equivalent | not a row | Kafka counts only unexpected failures and record-validation errors; KubeMQ's `kubemq_kafka_produce_rejects_total` counts every refusal |
| `RequestMetrics ErrorsPerSec{request,error}` | `kubemq_kafka_produce_rejects_total{error_code}` | `operations.metrics.errors-by-code` | numeric code, produce only |
| `RequestMetrics TotalTimeMs{request=Produce}` | `kubemq_kafka_produce_admit_to_ack_seconds` (histogram) | `operations.metrics.produce-latency` | server-side span; excludes queueing and response send |
| `RequestMetrics TotalTimeMs{request=FetchConsumer}` | `kubemq_kafka_fetch_handler_duration_seconds` (histogram) | `operations.metrics.fetch-latency` | |
| `KafkaController ActiveControllerCount` | `kubemq_cluster_leader` | `operations.metrics.active-controller` | "leads the control shard"; reads 1 on a single node |
| `ReplicaManager UnderReplicatedPartitions` | `kubemq_kafka_under_replicated_partitions` | `operations.metrics.under-replicated` | partitions this node leads |
| `ReplicaManager UnderMinIsrPartitionCount` | `kubemq_kafka_under_min_isr_partitions` | `operations.metrics.under-min-isr` | partitions this node leads |
| `KafkaController OfflinePartitionsCount` | `kubemq_kafka_offline_partitions` | `operations.metrics.offline-partitions` | reported by the control-shard leader, as Kafka's active controller does |
| `ReplicaManager LeaderCount` | `kubemq_shard_leader` (sum) | `operations.metrics.leadership` | Raft shards, not partitions |
| `Log LogEndOffset` | `kubemq_kafka_log_end_offset{topic,partition}` | `operations.metrics.log-end-offset` | |
| `Log Size` | `kubemq_kafka_log_size_bytes{topic,partition}` | `operations.metrics.log-size` | retained record-batch bytes |
| `socket-server-metrics connection-count` | `kubemq_kafka_connections` | `operations.metrics.connections` | node total, no listener label |
| JVM `OpenFileDescriptorCount` | `process_open_fds` | `operations.metrics.open-files` | |
| consumer lag (tool-computed) | `kubemq_kafka_consumer_group_lag{group,topic,partition}` | `operations.lag.group-lag`, `operations.lag.partition-0..2`, `operations.lag.caught-up`, `operations.lag.active-member` | refreshed every 5 s by the group coordinator; at most 1024 series, overflow in `__other__` |
| `ControllerStats LeaderElectionRateAndTimeMs` | `kubemq_next_raft_leader_changes_total` | not a row | rate only, no election time |
| `NetworkProcessorAvgIdlePercent`, `RequestHandlerAvgIdlePercent` | none (nearest: `kubemq_kafka_connection_inflight_requests`) | not a row | different threading model |
| group state, member count | none in Prometheus | not a row | visible in the KubeMQ dashboard and API, and through `kafka-consumer-groups.sh` (`tools.cli.groups-describe`) |
| transaction coordinator metrics | none | not a row | transactions are listed and described with `kafka-transactions.sh` (`tools.cli.transactions-tool`) |

KubeMQ series with no Kafka counterpart that an operator should alert on:

- `kubemq_kafka_acks0_forward_dropped_total{topic,reason}` — records sent with acks=0 that a
  cluster follower could not forward. The only silent-loss signal; alert on any increase.
- `kubemq_kafka_undecodable_records_total{path}` — stored records the broker cannot decode.
- `kubemq_storage_full{node}` — the node refuses writes.
- `kubemq_kafka_produce_throttle_seconds_total{cause}` — produce back-pressure.

## 3. Broker and topic settings

KubeMQ reads no `server.properties`. Broker-level settings are environment variables of the
KubeMQ server (or fields of the operator's `KubemqCluster` resource); topic-level settings go
through the normal Kafka admin API. `kafka-configs.sh` on the broker works as on Kafka:
describe, describe `--all`, the cluster default, broker loggers, and setting a dynamic config and
removing it (`tools.cli.configs-broker`).

A topic config key Kafka does not define is refused with INVALID_CONFIG (40), as on Kafka
(`operations.config.unknown-topic-key`); `min.insync.replicas=0` is refused with 40
(`operations.config.min-insync-zero`); and a fresh topic reports the same defaults as Kafka
(`operations.config.topic-defaults`), except `segment.bytes`, which KubeMQ reports as its real
engine segment size, 64 MiB (`operations.config.segment-bytes`, OPTIONAL in `docs/gaps.md`).

Topic configs are honoured as on Kafka (`retention.ms`, `retention.bytes`, `cleanup.policy`
including `compact,delete`, `max.message.bytes`, `message.timestamp.type` including
LogAppendTime, the timestamp bounds of KIP-937, `min.insync.replicas` on acks=all, and the
compaction settings), with two exceptions listed in `docs/gaps.md`: `compression.type` is stored
and described but batches keep the producer's codec, and `segment.bytes` is stored but the engine
segments by `STORE_NEXT_SEGMENT_SIZE`.

| Kafka setting | KubeMQ | Verified by |
|---|---|---|
| `num.partitions` | none; auto-created topics get 1 partition, as on Kafka | `operations.config.create-defaults`, `operations.config.auto-create`, `features.admin.partition-limit` |
| `default.replication.factor` | none; -1 means "every node"; above the node count error 38 | `operations.config.create-defaults` (one node) |
| `min.insync.replicas` | topic config; acks=all produces are refused with NOT_ENOUGH_REPLICAS when the in-sync set is smaller; the Raft write quorum always applies | `operations.config.min-insync-zero`, `operations.config.topic-defaults` |
| `auto.create.topics.enable` | always on; restrict through KubeMQ authorization | `operations.config.auto-create` (same topic, partitions and replicas as Kafka's default) |
| `log.retention.ms` / `retention.ms` | topic config; a topic without one gets the broker default, 7 days, as on Kafka | `features.log.retention-ms`, `features.log.retention-ms-zero` |
| `log.retention.bytes` / `retention.bytes` | topic config, enforced per partition | `features.log.retention-bytes`, `features.log.retention-bytes-compact-delete` |
| `message.max.bytes` | `CONNECTORS_KAFKA_MAX_MESSAGE_BYTES`; the topic default reports 1048588, as on Kafka | `operations.config.topic-defaults`, `features.produce.max-message-bytes` |
| `max.message.bytes` (topic) | enforced per record batch on produce | `features.produce.max-message-bytes` |
| `log.segment.bytes` / `segment.bytes` | KubeMQ reports its engine segment size (64 MiB, `STORE_NEXT_SEGMENT_SIZE`); setting it has no effect | `operations.config.segment-bytes` (OPTIONAL) |
| `compression.type` (topic) | stored and described; batches keep the producer's codec | `features.produce.broker-recompression` (OPTIONAL) |
| `message.timestamp.type` | CreateTime and LogAppendTime | `features.log.timestamp-createtime`, `features.log.timestamp-logappendtime` |
| `offsets.retention.minutes` | `CONNECTORS_KAFKA_OFFSETS_RETENTION_MINUTES` (default 10080) | `features.offsets.retention` |
| `transactional.id.expiration.ms` | `CONNECTORS_KAFKA_TRANSACTIONAL_ID_EXPIRATION_MS` | not a row |
| `transaction.max.timeout.ms` | `CONNECTORS_KAFKA_MAX_TRANSACTION_TIMEOUT_MS` (default 900000) | `operations.config.txn-timeout-too-big` (900001 ms refused with error 50) |
| `group.initial.rebalance.delay.ms` | no KubeMQ setting | not a row |
| `group.min.session.timeout.ms`, `group.max.session.timeout.ms` | fixed at 6000 and 300000 ms | `features.groups.session-timeout-bounds` |
| `listeners`, `advertised.listeners` | `CONNECTORS_KAFKA_PORT`, `CONNECTORS_KAFKA_TLS_PORT`, one advertised endpoint (`CONNECTORS_KAFKA_ADVERTISED_HOST`, `_ADVERTISED_PORT`) | runner compose files |
| SASL and TLS listener settings | `CONNECTORS_KAFKA_SASL_*`, global TLS certificate settings | `security.*` rows |
| `authorizer.class.name`, ACLs | KubeMQ authorization policy; the Kafka ACL admin APIs (DescribeAcls, CreateAcls, DeleteAcls) work as on Kafka | `security.acl.*` rows |
| client quotas | `CONNECTORS_KAFKA_PRODUCE_BYTE_RATE`, `_FETCH_BYTE_RATE`; described and altered through the Kafka API | `security.quota.*` rows |
| `offsets.topic.replication.factor`, `transaction.state.log.replication.factor`, `unclean.leader.election.enable`, thread pools | not applicable (internal Raft stores, no unclean election, different threading) | not a row |

## 4. Rolling restart and upgrade

Not measured by this suite.
