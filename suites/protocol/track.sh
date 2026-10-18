#!/usr/bin/env bash
# Protocol track entry point, called by runner/certify.sh with ORACLE_BOOTSTRAP,
# KUBEMQ_BOOTSTRAP, TRACK_RESULTS and CERT_RUN_DIR set. Builds the Go suite and runs every row
# of matrix/protocol.yaml.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
: "${ORACLE_BOOTSTRAP:?}" "${KUBEMQ_BOOTSTRAP:?}" "${TRACK_RESULTS:?}" "${CERT_RUN_DIR:?}"

command -v go >/dev/null 2>&1 || { echo "[protocol] ERROR: Go is required for the protocol track (see suites/protocol/go.mod)" >&2; exit 2; }
BIN="${CERT_RUN_DIR}/bin/protocol-track"
mkdir -p "$(dirname "${BIN}")"
(cd "${HERE}" && go build -o "${BIN}" ./cmd/protocol-track)

exec "${BIN}" \
  -matrix "${ROOT}/matrix/protocol.yaml" \
  -oracle "${ORACLE_BOOTSTRAP}" \
  -kubemq "${KUBEMQ_BOOTSTRAP}" \
  -out "${TRACK_RESULTS}" \
  -dump-dir "${CERT_RUN_DIR}/transcripts/protocol" \
  -apikeys "${ROOT}/matrix/apikeys.yaml" \
  -apikey-impacts "${ROOT}/matrix/apikey-impacts.yaml" \
  -gaps "${ROOT}/docs/gaps.md" \
  ${PROTOCOL_ONLY:+-only "${PROTOCOL_ONLY}"}
