#!/usr/bin/env bash
# Operations track entry point (Track G), called by runner/certify.sh.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
: "${ORACLE_BOOTSTRAP:?}" "${KUBEMQ_BOOTSTRAP:?}" "${TRACK_RESULTS:?}" "${CERT_RUN_DIR:?}" "${KUBEMQ_API_PORT:?}"
command -v go >/dev/null 2>&1 || { echo "[operations] ERROR: Go is required for the operations track (see suites/operations/go.mod)" >&2; exit 2; }
BIN="${CERT_RUN_DIR}/bin/operations-track"
mkdir -p "$(dirname "${BIN}")"
(cd "${HERE}" && go build -o "${BIN}" ./cmd/operations-track)
exec "${BIN}" -matrix "${ROOT}/matrix/operations.yaml" -oracle "${ORACLE_BOOTSTRAP}" -kubemq "${KUBEMQ_BOOTSTRAP}" \
  -oracle-scrape "${HERE}/oracle-jmx.sh" -kubemq-metrics "http://localhost:${KUBEMQ_API_PORT}/metrics" \
  -out "${TRACK_RESULTS}" -gaps "${ROOT}/docs/gaps.md" ${OPERATIONS_ONLY:+-only "${OPERATIONS_ONLY}"}
