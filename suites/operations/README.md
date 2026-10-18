# Operations track (Track G)

Metric rows from `matrix/operations.yaml`: each maps an Apache Kafka operator metric (JMX MBean)
to the KubeMQ Prometheus series an operator would use instead. Both brokers are scraped before
and after the same scripted workload; package `metric` judges each leg (series present, counter
monotonic, change matches the workload) and compares the legs. The oracle leg must meet the
expectation or the row is VOID.

- `metric/scrape.go` — Prometheus text parser (KubeMQ `/metrics`) and `kafka-jmx.sh` CSV parser
  (Kafka), both normalised to one series form.
- `metric/check.go` — expectation kinds: exact counter growth, growth in a range, any growth,
  gauge equal to a value, gauge in a range.
- `metric/verdict.go` — oracle precondition (VOID), PASS, or the exact deviation text a gap
  registration must carry.
- `metric/matrix.go` — matrix loader and the workloads a row may name.
- `metric/mutation_test.go` — verifier mutation self-tests, run by `suites/verifier-selftest/run.sh`.

The track command runs the workloads and scrapes both brokers through the engine abstraction.
