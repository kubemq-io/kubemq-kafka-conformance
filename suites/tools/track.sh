#!/usr/bin/env bash
# Tools track entry point (Track D), called by runner/certify.sh. Every program under test runs in
# a container of the pinned Apache Kafka image and reaches both brokers through the host-gateway
# alias (TRACK_CLIENTS=container in track.env).
# shellcheck source-path=SCRIPTDIR
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
: "${ORACLE_BOOTSTRAP:?}" "${KUBEMQ_BOOTSTRAP:?}" "${TRACK_RESULTS:?}" "${CERT_RUN_DIR:?}"
: "${KAFKA_IMAGE_REPO:?}" "${KAFKA_IMAGE_DIGEST:?}"
# Every container and network this run starts carries this label; the exit trap removes them.
TOOLS_RUN_LABEL="${CERT_RUN_ID:-$(basename "${CERT_RUN_DIR}" | tr -c 'a-zA-Z0-9\n' '-')$$}"
export TOOLS_RUN_LABEL
trap '"${HERE}/ctr.sh" sweep "${TOOLS_RUN_LABEL}"' EXIT
# Burrow's published image is amd64 only and crashes emulated; elsewhere build the pinned release
# for the engine's architecture once (the tag names the commit) and run that instead.
if [[ "${CERT_ENGINE_ARCH:-amd64}" != amd64 ]]; then
  # shellcheck source=../../runner/lib/engine.sh
  . "${ROOT}/runner/lib/engine.sh"
  : "${BURROW_SOURCE_COMMIT:?}" "${BURROW_SOURCE_SHA256:?}" "${BURROW_BUILD_IMAGE:?}" "${BURROW_RUNTIME_IMAGE:?}"
  BURROW_IMAGE="localhost/kcert-burrow:${BURROW_VERSION}-${BURROW_SOURCE_COMMIT:0:12}-${CERT_ENGINE_ARCH}"
  if ! engine_run image inspect "${BURROW_IMAGE}" >/dev/null 2>&1; then
    echo "[tools] building Burrow ${BURROW_VERSION} for ${CERT_ENGINE_ARCH} from source" >&2
    engine_run build -q -t "${BURROW_IMAGE}" -f "${HERE}/burrow/Containerfile" \
      --build-arg "BUILD_IMAGE=${BURROW_BUILD_IMAGE}" --build-arg "RUNTIME_IMAGE=${BURROW_RUNTIME_IMAGE}" \
      --build-arg "BURROW_SOURCE_COMMIT=${BURROW_SOURCE_COMMIT}" \
      --build-arg "BURROW_SOURCE_SHA256=${BURROW_SOURCE_SHA256}" "${HERE}/burrow" >&2
  fi
  export BURROW_IMAGE
fi
BIN="${CERT_RUN_DIR}/bin/tools-track"
mkdir -p "$(dirname "${BIN}")"
(cd "${HERE}" && go build -o "${BIN}" ./cmd/tools-track)
"${BIN}" -matrix "${ROOT}/matrix/tools.yaml" -oracle "${ORACLE_BOOTSTRAP}" -kubemq "${KUBEMQ_BOOTSTRAP}" \
  -out "${TRACK_RESULTS}" -gaps "${ROOT}/docs/gaps.md" -ctr "${HERE}/ctr.sh" ${TOOLS_ONLY:+-only "${TOOLS_ONLY}"}
