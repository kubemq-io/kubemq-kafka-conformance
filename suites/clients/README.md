# Clients track (Track C)

Does an ordinary application, written with each Kafka client library as its upstream documentation
shows, behave the same on KubeMQ as on Apache Kafka? Each client in `matrix/clients.yaml` runs the
scenario in [SCENARIO.md](SCENARIO.md) against the Kafka oracle, then against KubeMQ with only the
bootstrap address changed.

- `drivers/<name>/` — one small application per client. Each folder is self-contained: a Go
  `main.go` (franz-go and the other Go clients) or a `Containerfile` with its sources (Java and the
  other languages). A prospect can read and run it on its own against any Kafka-compatible
  bootstrap address by setting the `KCERT_*` variables of SCENARIO.md.
- `verifiable` — Kafka's own `kafka-verifiable-producer` and `kafka-verifiable-consumer` from the
  pinned oracle image, the clients Apache Kafka's ducktape system tests drive (upstream provenance).
- `verify/` — the verifier: ledger checks, fresh-reader checks, observation diff, and the mutation
  self-tests that prove it fails on loss, duplicates, reordering, wrong offsets and wrong error codes
  (`suites/verifier-selftest/run.sh`, run in `pr.yml`).
- `cmd/clients-track/` — the track runner `runner/certify.sh --track clients` calls.
- `clients.env` — pinned driver images and client versions, and the prebuilt multi-arch image of
  each driver by digest (`<NAME>_DRIVER_IMAGE`) with the source hash it was built from
  (`<NAME>_DRIVER_SOURCE`). `build-driver.sh` uses a prebuilt image only when that hash matches the
  driver's current sources, and builds locally otherwise (also when the private registry is not
  reachable). `publish-drivers.sh` (operator only) builds both architectures, pushes them and
  prints the pin lines.

Run only this track: `runner/certify.sh --track clients`. Run some rows:
`CLIENTS_ONLY=clients.kafka-clients.scenario runner/certify.sh --track clients` (the others are NOT-RUN).
Evidence per row and leg (ledgers, tool output, driver logs) lands in
`<run-dir>/transcripts/clients/<row>/`.
