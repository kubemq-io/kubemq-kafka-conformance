#!/usr/bin/env bash
# Clients track entry point (Track C), called by runner/certify.sh. Drivers run in containers
# through runner/client-run.sh; the verifier and fresh reader run on the host.
# shellcheck source-path=SCRIPTDIR
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
: "${ORACLE_BOOTSTRAP:?}" "${KUBEMQ_BOOTSTRAP:?}" "${TRACK_RESULTS:?}" "${CERT_RUN_DIR:?}" "${CERT_ENGINE_ARCH:?}"
set -a
# shellcheck source=clients.env
. "${HERE}/clients.env"
set +a
BIN="${CERT_RUN_DIR}/bin/clients-track"
mkdir -p "$(dirname "${BIN}")"
(cd "${HERE}" && go build -o "${BIN}" ./cmd/clients-track)
exec "${BIN}" -matrix "${ROOT}/matrix/clients.yaml" -suite "${HERE}" -run-dir "${CERT_RUN_DIR}" \
  -oracle "${ORACLE_BOOTSTRAP}" -kubemq "${KUBEMQ_BOOTSTRAP}" -out "${TRACK_RESULTS}" \
  -gaps "${ROOT}/docs/gaps.md" ${CLIENTS_ONLY:+-only "${CLIENTS_ONLY}"}
