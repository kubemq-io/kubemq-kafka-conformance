#!/usr/bin/env bash
# Run every verifier mutation self-test. Needs Go; needs no broker, no container engine and no
# license key, so it is safe on fork pull requests (.github/workflows/pr.yml).
#
# Each verifier's mutation tests live next to the verifier (they need its unexported code);
# this script is the single place that lists them. A verifier added without an entry here is a
# review finding.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

run() { echo "[selftest] $1"; (cd "${ROOT}/$2" && go test -count=1 -run "$3" ./... ); }

# Protocol track: transcript diff, scenario invariants, smoke round-trip and group-resume
# checks, wrong error code on the wire, and the row-level verdict mapping.
run "protocol verifiers" suites/protocol 'Mutation|TestRunRowVerdicts|TestClassify|TestRegistered'
# Features track: round-trip analysis (loss, duplicate, reorder, wrong offset, wrong content),
# observation diff (wrong error code), verdict mapping with gap binding, murmur2 vectors.
run "features verifiers" suites/features '.'
# Operations track (includes the rolling-upgrade continuity checker, kept although the cluster
# track that called it was removed):
# metric-row verifier (loss, duplicate, counter reset, stale or renamed
# series, wrong label, wrong error code, wrong lag), oracle-precondition VOID, matrix loader.
run "operations verifiers" suites/operations '.'
# Migration track: cutover resume (skip, duplicate, reorder, wrong start offset, translation ahead
# of the source), compacted latest values and tombstones, record identity, verdict mapping.
run "migration verifiers" suites/migration '.'
# Transactions track: isolation analysis (loss, duplicate, reorder, wrong offset, wrong content,
# aborted record visible, broker-side filtering, marker offsets, last stable offset, wrong error
# code), driver report parsing, verdict mapping with gap binding.
run "transactions verifiers" suites/transactions '.'
# Clients track: ledger and fresh-reader checks (loss, duplicate, reorder, wrong offset, wrong
# error code, wrong start offset, unassigned partition, crashed driver, wrong client version),
# observation diff and verdict mapping.
run "clients verifiers" suites/clients '.'
# Security track: plaintext-on-TLS classifier (Kafka answer, socket left open), handshake outcome
# (refused vs network failure), observation diff (old TLS version accepted), verdict mapping with
# gap binding, boot-failure and missing-posture accounting, certificate chain and names.
run "security verifiers" suites/security '.'
# Tools track: console round trip (loss, duplicate, reorder, wrong offset, wrong key or header,
# foreign record, split key), verifiable producer and consumer (loss, duplicate, wrong offset,
# failed or short commit), tool-output parsers, verdict mapping with gap binding.
run "tools verifiers" suites/tools '.'
# App-sim track: verify.py's own self-test (42 defects, from loss and duplicates to an aborted
# transaction made visible and a client other than the requested one, each judged by name).
echo "[selftest] app-sim verifier"; python3 "${ROOT}/suites/app-sim/verify.py" --self-test
# Report renderer: NOT-RUN for missing rows, unregistered GAP shown as FAIL, quarantine and
# expiry, VOID budget, malformed inputs.
run "report renderer rules" runner/report '.'
echo "[selftest] all verifier self-tests passed"
