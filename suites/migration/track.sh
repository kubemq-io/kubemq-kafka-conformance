#!/usr/bin/env bash
# Migration track entry point (Track G, docs/migration.md), called by runner/certify.sh. Every
# client, broker and MirrorMaker 2 runs in a container (TRACK_CLIENTS=container); containers are
# started through the tools track's helper (suites/tools/ctr.sh), so only the engine abstraction
# is called.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
: "${ORACLE_BOOTSTRAP:?}" "${KUBEMQ_BOOTSTRAP:?}" "${TRACK_RESULTS:?}" "${CERT_RUN_DIR:?}" "${CERT_ENGINE_ARCH:?}"
: "${KAFKA_IMAGE_REPO:?}" "${KAFKA_IMAGE_DIGEST:?}"
command -v go >/dev/null 2>&1 || { echo "[migration] ERROR: Go is required for the migration track (see suites/migration/go.mod)" >&2; exit 2; }
CTR="${ROOT}/suites/tools/ctr.sh"
TOOLS_RUN_LABEL="mig-${CERT_RUN_ID:-$(basename "${CERT_RUN_DIR}" | tr -c 'a-zA-Z0-9\n' '-')$$}"
export TOOLS_RUN_LABEL
trap '"${CTR}" sweep "${TOOLS_RUN_LABEL}"' EXIT
BIN="${CERT_RUN_DIR}/bin/migration-track"
AGENT="${CERT_RUN_DIR}/bin/migration-agent-linux-${CERT_ENGINE_ARCH}"
mkdir -p "$(dirname "${BIN}")"
(cd "${HERE}" && go build -o "${BIN}" ./cmd/migration-track)
(cd "${HERE}" && CGO_ENABLED=0 GOOS=linux GOARCH="${CERT_ENGINE_ARCH}" go build -o "${AGENT}" ./cmd/migration-agent)
"${BIN}" -matrix "${ROOT}/matrix/migration.yaml" -oracle "${ORACLE_BOOTSTRAP}" -kubemq "${KUBEMQ_BOOTSTRAP}" \
  -agent "${AGENT}" -ctr "${CTR}" -out "${TRACK_RESULTS}" -gaps "${ROOT}/docs/gaps.md" ${MIGRATION_ONLY:+-only "${MIGRATION_ONLY}"}
