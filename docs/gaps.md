# Known differences from Apache Kafka

Every row of the suite runs on real Apache Kafka first and then on KubeMQ. Where KubeMQ behaves
differently on purpose, the difference is listed here **before** the run. The report shows such
a row as **OPTIONAL**: a Kafka feature KubeMQ does not offer, with who it affects and the
workaround. Any other difference from Kafka is a FAIL. A FAIL is never relabelled after the fact.

## How the runner reads this file

The runner reads the tables below: the first cell is the row id (as used in `matrix/`), the
second the exact difference the run must observe, the last the sign-off. The sign-off reads
`<who> <YYYY-MM-DD>` or `<who> <YYYY-MM-DDTHH:MMZ>` (UTC) and must be earlier than the run; a
date-only sign-off counts from the end of that day. A row with `-` as its sign-off is a draft
and is not registered. If either broker's behaviour changes, so that the observed difference no
longer equals the one listed, the row is FAIL until it is listed again.

## Optional features

Kafka features KubeMQ does not offer today. Each is shown as OPTIONAL in the report.

| Row | Deviation | Impact | Workaround | Roadmap | Signed off |
|---|---|---|---|---|---|
| `apikey.88` | ApiKey 88 StreamsGroupHeartbeat not advertised; Kafka advertises v0-v0 | Kafka Streams with group.protocol=streams (KIP-1071) cannot join a group. | Run Streams applications with the classic rebalance protocol, the Kafka Streams default. | No version committed. | KubeMQ 2026-10-06 |
| `apikey.89` | ApiKey 89 StreamsGroupDescribe not advertised; Kafka advertises v0-v0 | kafka-streams-groups.sh and Admin describe for KIP-1071 streams groups; such groups cannot exist on KubeMQ. | Describe classic Streams groups with kafka-consumer-groups.sh. | No version committed. | KubeMQ 2026-10-06 |
| `security.gssapi.configurable` | gssapi_mechanism: Kafka "configurable (documented; not run)", KubeMQ "refused by configuration validation" | Kerberos-authenticated fleets cannot connect; KubeMQ refuses GSSAPI in its mechanism list and does not start. | Migrate those principals to SCRAM or mTLS. | On demand. | KubeMQ 2026-10-07 |
| `security.delegation-tokens.create-and-login` | create: Kafka "NONE", KubeMQ "DELEGATION_TOKEN_AUTH_DISABLED"; token_login: Kafka "NONE", KubeMQ "not attempted" | kafka-delegation-tokens.sh and AdminClient createDelegationToken fail with DELEGATION_TOKEN_AUTH_DISABLED (61), the same answer a Kafka broker without a token secret gives; frameworks that hand delegation tokens to workers (Spark, Flink on secured Kafka) cannot use them. | Give workers their own SCRAM credentials or mTLS certificates. | No version committed. | KubeMQ 2026-10-07 |
| `security.oauthbearer.plain-port` | plain_port_login: Kafka "NONE", KubeMQ "UNSUPPORTED_SASL_MECHANISM" | Clients that use OAUTHBEARER over SASL_PLAINTEXT (allowed by Kafka) cannot log in to KubeMQ; KubeMQ offers OAUTHBEARER on its SASL_SSL port by default. | Connect OAUTHBEARER clients to the TLS port (SASL_SSL), or set `CONNECTORS_KAFKA_OAUTH_BEARER_ALLOW_PLAINTEXT=true` to offer it on the plaintext SASL port too. | No version committed. | KubeMQ 2026-10-07 |
| `features.produce.broker-recompression` | stored_codecs: Kafka ["gzip"], KubeMQ ["lz4"] | A topic created with compression.type=gzip (or any codec other than producer) keeps the batches in the producer's codec on KubeMQ; Kafka recompresses them. Consumers still decode every codec, so data is identical; only storage size and the codec consumers see differ. KubeMQ stores the per-topic compression.type and echoes it in DescribeConfigs but does not apply it. | Compress in the producer with the codec you want stored (compression.type on the producer). | On demand: Kafka's own default is compression.type=producer (no broker recompression); implement only if a customer requires a forced topic codec. | KubeMQ 2026-10-06 |
| `operations.config.segment-bytes` | segment.bytes: Kafka "1073741824", KubeMQ "67108864" | KubeMQ reports its real engine segment size (64 MiB); tools reading segment.bytes see that value; setting it has no effect (the engine segments by STORE_NEXT_SEGMENT_SIZE) | None needed for clients. | No version committed. | KubeMQ 2026-10-08T01:30Z |
| `feature.streams.version` | ApiVersions feature streams.version not advertised; Kafka supports 0-1, finalized 1 | kafka-features.sh describe and other tools that read this flag do not see it on KubeMQ. No client behaviour depends on it: the flags clients negotiate (transaction.version, group.version, metadata.version) are advertised as on Kafka. | None needed for clients. | No version committed. | KubeMQ 2026-10-06 |
| `feature.eligible.leader.replicas.version` | ApiVersions feature eligible.leader.replicas.version not advertised; Kafka supports 0-1, finalized 1 | kafka-features.sh describe and other tools that read this flag do not see it on KubeMQ. No client behaviour depends on it: the flags clients negotiate (transaction.version, group.version, metadata.version) are advertised as on Kafka. | None needed for clients. | No version committed. | KubeMQ 2026-10-06 |

## Broker-to-broker requests

Six requests are used only between Kafka brokers; no client or tool sends them. They are listed
and counted separately as NOT-APPLICABLE to application replacement: KubeMQ replaces Kafka for
applications and tools, not as a member of a Kafka broker cluster.

| Row | Deviation | Impact | Workaround | Roadmap | Signed off |
|---|---|---|---|---|---|
| `apikey.27` | ApiKey 27 WriteTxnMarkers not advertised; Kafka advertises v1-v2; broker-internal, not applicable to application replacement; Kafka broker interoperability remains unsupported | None for clients (broker-to-broker transaction markers). No client or tool sends this request. KubeMQ writes transaction markers internally. | None needed: broker-internal request. | No version committed. | KubeMQ 2026-10-13T07:38:09Z |
| `apikey.83` | ApiKey 83 InitializeShareGroupState not advertised; Kafka advertises v0-v0; broker-internal, not applicable to application replacement; Kafka broker interoperability remains unsupported | None for clients (share coordinator state persistence, broker-internal). No client or tool sends this request. KubeMQ persists share-group state internally. | None needed: broker-internal request. | No version committed. | KubeMQ 2026-10-13T07:38:09Z |
| `apikey.84` | ApiKey 84 ReadShareGroupState not advertised; Kafka advertises v0-v0; broker-internal, not applicable to application replacement; Kafka broker interoperability remains unsupported | None for clients (share coordinator state persistence, broker-internal). No client or tool sends this request. | None needed: broker-internal request. | No version committed. | KubeMQ 2026-10-13T07:38:09Z |
| `apikey.85` | ApiKey 85 WriteShareGroupState not advertised; Kafka advertises v0-v1; broker-internal, not applicable to application replacement; Kafka broker interoperability remains unsupported | None for clients (share coordinator state persistence, broker-internal). No client or tool sends this request. | None needed: broker-internal request. | No version committed. | KubeMQ 2026-10-13T07:38:09Z |
| `apikey.86` | ApiKey 86 DeleteShareGroupState not advertised; Kafka advertises v0-v0; broker-internal, not applicable to application replacement; Kafka broker interoperability remains unsupported | None for clients (share coordinator state persistence, broker-internal). No client or tool sends this request. | None needed: broker-internal request. | No version committed. | KubeMQ 2026-10-13T07:38:09Z |
| `apikey.87` | ApiKey 87 ReadShareGroupStateSummary not advertised; Kafka advertises v0-v1; broker-internal, not applicable to application replacement; Kafka broker interoperability remains unsupported | None for clients (share coordinator state persistence, broker-internal). No client or tool sends this request. | None needed: broker-internal request. | No version committed. | KubeMQ 2026-10-13T07:38:09Z |
