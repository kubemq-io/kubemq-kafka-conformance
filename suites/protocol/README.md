# Protocol track

Wire-protocol transcript parity. Each row of [`matrix/protocol.yaml`](../../matrix/protocol.yaml)
runs one scenario with one driver against the Kafka oracle, then against KubeMQ with only the
bootstrap address changed.

- **Full-transcript rows (franz-go):** every produce ack, fetch, ListOffsets, Metadata and
  transaction event is normalized (base-relative offsets, hashed values and headers) and diffed
  against the oracle. Scenario invariants are checked on raw events: a fresh topic's first
  offset is exactly 0, per-key order holds across partitions, read_committed hides aborted
  records, and ListOffsets latest under read_committed equals the last stable offset.
- **Rig self-check:** before KubeMQ runs, the oracle's transcript is compared with the same
  scenario on franz-go's in-memory `kfake` broker. If two known-good brokers do not read
  identical, the rig cannot be trusted and the row is VOID.
- **Smoke rows (sarama, segmentio/kafka-go):** produce and consume round-trip (and, for
  segmentio, consumer-group commit and resume) must succeed on both brokers.

Run it through the runner:

```bash
runner/certify.sh --track protocol
```

Or directly against brokers you started yourself:

```bash
cd suites/protocol
go run ./cmd/protocol-track -matrix ../../matrix/protocol.yaml \
  -oracle 127.0.0.1:28092 -kubemq 127.0.0.1:21092 -out /tmp/protocol.json
```

Unit tests need no broker: `go test ./...`.

## Provenance

Ported from `kubemq-server/tests/kafka-conformance` (host-mode legs only). Changes from the
source: the `conformance` build tag and every environment gate are gone (rows come from the
matrix, so nothing can be silently skipped); the in-process KubeMQ target is replaced by an
address target that dials the KubeMQ container; KubeMQ is diffed against real Kafka instead
of `kfake`, with `kfake` kept as the rig self-check; the in-process port pool is dropped.
