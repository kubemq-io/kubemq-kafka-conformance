# Verifier self-tests

Every verifier in this repository is proven to turn red. Each mutation test feeds a verifier a
known-good input with exactly one injected defect and asserts a failure:

| Verifier | Defects injected | Test |
|---|---|---|
| Full-transcript diff (franz-go rows), on real Kafka 4.3.1 transcripts of three scenarios | lost record, duplicated record, reordered records, fetch offset off by one, produce ack offset off by one, every offset shifted, wrong produce error code, wrong fetch error code, wrong value | `suites/protocol/conformance/mutation_test.go` |
| Scenario invariants | fresh topic not starting at offset 0, records out of order within a partition, aborted record visible under read_committed, committed record lost | same file |
| Smoke round-trip (sarama, segmentio) | loss, duplicate, reorder, wrong offset, wrong header | same file |
| segmentio consumer-group resume | resume one too early, one too late, reorder, wrong offset | same file |
| Wrong error code on the wire | franz-go, sarama and segmentio against an in-memory broker that answers one Produce with `MESSAGE_TOO_LARGE` | same file |
| Row verdict mapping | broken KubeMQ leg is FAIL, broken oracle leg is VOID, clean pair is PASS | `suites/protocol/cmd/protocol-track/main_test.go` |
| ApiKey and feature verdicts | unregistered, unsigned, signed after the run, and changed-since-sign-off deviations are FAIL; exact signed deviation is GAP; stale or uncurated matrix fails | `suites/protocol/cmd/protocol-track/apikeys_test.go`, `suites/protocol/gaps/gaps_test.go` |
| Features track round trip (`AnalyzeRoundTrip`) | loss, duplicate, reorder, wrong offset, wrong value, null header turned empty | `suites/features/probe/mutation_test.go` |
| Features observation diff and verdict | wrong error code, missing field, registered exact deviation is GAP, larger or late-signed deviation is FAIL; Kafka's murmur2 test vectors | `suites/features/probe/mutation_test.go`, `suites/features/cmd/features-track/main_test.go` |
| Report renderer | missing row, unregistered or undated GAP, quarantine and expiry from published history, VOID budget over behaviour rows, VOID without root cause, partial published run, malformed results | `runner/report/build_test.go` |

Run them all:

```bash
suites/verifier-selftest/run.sh
```

They run on every pull request in `.github/workflows/pr.yml`, without a broker.

The mutation tests found one real weakness while being written: the sarama and segmentio
round-trip checks inherited from kubemq-server never compared offsets, so a broker answering
with wrong offsets would have passed a smoke row. Both now require the record at position i to
sit at offset i on the fresh topic.
