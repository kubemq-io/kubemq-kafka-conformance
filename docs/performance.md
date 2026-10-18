# Performance baseline

**KubeMQ's published performance numbers are in the [KubeMQ vs Kafka benchmark](https://kubemq-io.github.io/kubemq-vs-kafka-benchmark/):**
two identical 3-broker clusters on Google Cloud, the OpenMessaging Benchmark client, and an
acknowledged-loss verifier beside the load.

This track is something else: a quick side-by-side run of Kafka's own load tools on one machine,
so an evaluator can size a trial on their own hardware. Its numbers depend entirely on that
machine (a laptop running both brokers and the load in containers is not a performance test), so
the published report does not show them, and they are outside the pass/fail verdicts: a row
passes when the measurement completed on both brokers.

## Method

- Apache Kafka's `kafka-producer-perf-test` and `kafka-consumer-perf-test` from the pinned Kafka
  image, in a client container on the same machine as the brokers.
- Kafka and KubeMQ measured one after the other; each broker runs alone (the other is paused).
- Single node, one topic of 6 partitions per message size, producer defaults except `acks`, no
  compression, unthrottled (`--throughput -1`); one warm-up, then the median of 3 repetitions.
  Consume rates are the consumer tool's fetch rates, which exclude the group join.
- Message sizes 100 bytes, 1 KiB and 10 KiB; produce with `acks=all` and with `acks=1`; consume.
- Both brokers keep their data on the same persistent disk at their shipped sync defaults: the
  track's own compose files bind-mount a directory of the run on that disk as Kafka's
  `log.dirs` (`/var/lib/kafka/data`, confirmed in the broker's startup log) and as KubeMQ's store
  (`/kubemq/store`). The kit's standard oracle (`compose/kafka-oracle.yaml`) is not used here. Kafka
  does not fsync each write (`log.flush.*` unset; the OS page cache flushes). KubeMQ runs its next
  storage engine's defaults. Neither is tuned.
- The 99th-percentile latency is the producer tool's, at full load: it includes time a record
  waits in the producer, so it describes behaviour under saturation, not idle latency.

Rerun: `runner/certify.sh --track perf` (about 6 minutes on an 8-vCPU machine). The track
boots its own pair with disk-backed data directories and removes them afterwards.

Exact commands, per message size `S` (100, 1024, 10240) and record count `N` (3,000,000,
300,000, 30,000), with `B` the bootstrap address of the broker being measured:

```bash
kafka-topics.sh --bootstrap-server B --create --topic T --partitions 6
kafka-producer-perf-test.sh --topic T --num-records 20000 --record-size S --throughput -1 --producer-props bootstrap.servers=B acks=all compression.type=none
kafka-producer-perf-test.sh --topic T --num-records N --record-size S --throughput -1 --producer-props bootstrap.servers=B acks=all compression.type=none
kafka-producer-perf-test.sh --topic T --num-records N --record-size S --throughput -1 --producer-props bootstrap.servers=B acks=1 compression.type=none
kafka-consumer-perf-test.sh --bootstrap-server B --topic T --messages N --timeout 60000
```

The first producer command is the warm-up. Each of the last three runs three times; the report
shows the median. The client container gets `KAFKA_HEAP_OPTS=-Xms512m -Xmx1g`.

## Results

Rerun the track and read the `perf.*` rows of your own `.work/run-<time>/report/REPORT.md`:
throughput for every workload on both brokers, and the producer tool's 99th-percentile latency
for the produce workloads. The published report lists these rows without values.

When reading your numbers:

- Consume rates exclude the consumer group join.
- The 99th-percentile latency is measured at full load and varies between runs, most of all for
  100-byte records; compare throughput first, and compare latency only between runs on the same
  machine.
- All rows run on a single node, with the brokers, the load and the container engine sharing one
  machine. For cluster numbers on identical hardware, see the
  [benchmark](https://kubemq-io.github.io/kubemq-vs-kafka-benchmark/).
