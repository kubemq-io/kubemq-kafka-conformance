# KubeMQ Kafka conformance suite

**This suite is written and published by KubeMQ.** Read it with that in mind, and rerun it yourself:
everything needed is here.

KubeMQ serves the Apache Kafka protocol. This suite checks how closely it behaves like Apache Kafka
for applications and Kafka's own tools. Every test runs on real Apache Kafka first (the reference),
then on KubeMQ with the same client code and only the bootstrap address changed. A test passes when
both brokers behave the same.

What is covered: the Kafka protocol (every ApiKey and feature flag), produce, fetch, consumer groups,
offsets, log retention and compaction, transactions and exactly-once, the main client libraries
(Java kafka-clients, Spring Kafka, franz-go, Sarama, segmentio kafka-go, confluent-kafka for Go,
Python and .NET, KafkaJS, kcat), Kafka's CLI tools, Kafka Connect, Kafka Streams,
MirrorMaker 2, lag tools, TLS, mTLS, SASL (PLAIN, SCRAM, OAUTHBEARER), ACLs and quotas, operations
(metrics, configs) and migration from Kafka to KubeMQ and back.

## Result


| Track | Rows | PASS | OPTIONAL | FAIL | VOID | NOT-RUN | FLAKY | Result |
|---|---|---|---|---|---|---|---|---|
| protocol | 82 | 78 | 4 | 0 | 0 | 0 | 0 | passed |
| features | 59 | 58 | 1 | 0 | 0 | 0 | 0 | passed |
| transactions | 23 | 23 | 0 | 0 | 0 | 0 | 0 | passed |
| clients | 25 | 25 | 0 | 0 | 0 | 0 | 0 | passed |
| app-sim | 1 | 1 | 0 | 0 | 0 | 0 | 0 | passed |
| tools | 44 | 44 | 0 | 0 | 0 | 0 | 0 | passed |
| security | 41 | 38 | 3 | 0 | 0 | 0 | 0 | passed |
| perf | 0 | 0 | 0 | 0 | 0 | 0 | 0 | passed |
| operations | 31 | 30 | 1 | 0 | 0 | 0 | 0 | passed |
| migration | 7 | 7 | 0 | 0 | 0 | 0 | 0 | passed |
| **all** | **313** | 304 | 9 | 0 | 0 | 0 | 0 | **passed** |

Measurements, outside the counts above (values in the detail): perf 9.


**OPTIONAL** means a Kafka feature KubeMQ does not offer today. Each one is listed before the run,
with who it affects and the workaround. Any other difference from Kafka would be a FAIL.

| Optional feature | Who is affected | Workaround |
|---|---|---|
| Kafka Streams new rebalance protocol (KIP-1071) | Streams applications that set `group.protocol=streams` | Use the classic rebalance protocol, the Kafka Streams default |
| Kerberos (SASL/GSSAPI) | Kerberos-authenticated clients | SCRAM or mTLS |
| Delegation tokens | Frameworks that hand tokens to workers (Spark, Flink on secured Kafka) | Give workers their own SCRAM credentials or mTLS certificates |
| OAUTHBEARER on the SASL_PLAINTEXT port | OAuth clients connecting without TLS | Use the TLS port, or set `CONNECTORS_KAFKA_OAUTH_BEARER_ALLOW_PLAINTEXT=true` |
| Broker-side topic compression | Topics created with a fixed `compression.type` | Set the codec on the producer |
| Fixed 64 MiB log segment size | Tools that read or set `segment.bytes` | None needed for clients |
| `streams.version` and `eligible.leader.replicas.version` feature flags | Tools that list feature flags | None needed for clients |

Six requests that Kafka brokers send only to each other are listed separately as not applicable:
KubeMQ replaces Kafka for applications and tools, not as a member of a Kafka broker cluster.

- Results page: <https://kubemq-io.github.io/kubemq-kafka-conformance/>
- One-page result: [`COMPATIBILITY.md`](COMPATIBILITY.md)
- Every test, with what was observed on each broker: [`results/REPORT.md`](results/REPORT.md)
- Machine-readable verdicts: [`results/summary.json`](results/summary.json)
- Each difference in full: [`docs/gaps.md`](docs/gaps.md)

## Run it yourself

You need Docker or podman, Go, `curl`, `jq`, `lsof`, `git` and a KubeMQ evaluation key
(request one from KubeMQ). arm64 and amd64 both work.

```bash
export KUBEMQ_LICENSE_KEY=<your evaluation key>
runner/certify.sh --preflight-only   # checks your machine, starts nothing
runner/certify.sh --quick            # protocol tests only, about 15 minutes
runner/certify.sh --full             # every test, 1 to 2 hours
```

The suite pulls the latest KubeMQ image and the pinned Apache Kafka 4.3.1 image, starts both on your
machine, runs every test against both, and writes the same report as above into `.work/run-<time>/report/`.
Details: [`docs/rerun.md`](docs/rerun.md).

## How verdicts are decided

[`docs/methodology.md`](docs/methodology.md): the reference model, the verdicts, and the rules the report
applies (a difference is OPTIONAL only if it was listed before the run; a test that did not run fails the
report).

Guides built on the same tests: [migrating from Kafka](docs/migration.md),
[operating KubeMQ in place of Kafka](docs/operations.md), [Kafka tools](docs/tools.md),
[performance baseline](docs/performance.md).

## Layout

| Path | What |
|---|---|
| `runner/` | `certify.sh` (starts the brokers, runs the tracks) and the report renderer |
| `suites/` | one folder per track: protocol, features, transactions, clients, app-sim, tools, security, perf, operations, migration, schema-registry (opt-in) |
| `matrix/` | every test row, with where it comes from (an upstream Apache Kafka test, or written by KubeMQ) |
| `compose/` | the broker definitions and `versions.env`, every image and client version |
| `docs/` | method, rerun guide, known differences and guides |
| `results/` | the published report and summary |

## License

Apache-2.0, see [`LICENSE`](LICENSE).
