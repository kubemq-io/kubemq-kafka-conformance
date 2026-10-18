#!/usr/bin/env bash
# Performance baseline entry point (Track H), called by runner/certify.sh once per step:
#   track.sh prepare disk   data directories and KubeMQ's boot config into POSTURE_DIR
#   track.sh run disk       the measurements, against the pair the runner booted
#   track.sh finish         TRACK_RESULTS and the summary; the data directories are removed
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
: "${CERT_RUN_DIR:?}" "${TRACK_RESULTS:?}"
DIR="${CERT_RUN_DIR}/postures/perf/disk"
BIN="${CERT_RUN_DIR}/bin/perf-track"
if [[ ! -x "${BIN}" ]]; then
  mkdir -p "$(dirname "${BIN}")"
  (cd "${HERE}" && go build -o "${BIN}" ./cmd/perf-track)
fi
case "${1:?step}" in
  prepare)
    : "${POSTURE_DIR:?}" "${POSTURE_KUBEMQ_PORT:?}" "${POSTURE_KUBEMQ_API_PORT:?}" "${HOST_GATEWAY_ALIAS:?}"
    # The containers run as their images' own users; the directories must be writable by them.
    mkdir -p "${POSTURE_DIR}/kafka-data" "${POSTURE_DIR}/kubemq-data"
    chmod 0777 "${POSTURE_DIR}/kafka-data" "${POSTURE_DIR}/kubemq-data"
    cat >"${POSTURE_DIR}/kubemq.config" <<CFG
api:
  enable: true
  port: ${POSTURE_KUBEMQ_API_PORT}
store:
  engine: next
connectors:
  kafka:
    enable: true
    port: "${POSTURE_KUBEMQ_PORT}"
    advertisedhost: "${HOST_GATEWAY_ALIAS}"
CFG
    ;;
  run)
    : "${POSTURE_ORACLE_CONTAINER:?}" "${POSTURE_KUBEMQ_CONTAINER:?}" "${POSTURE_ORACLE_PORT:?}" "${POSTURE_KUBEMQ_PORT:?}"
    exec "${BIN}" run -matrix "${ROOT}/matrix/perf.yaml" -dir "${DIR}" -ctl "${HERE}/ctl.sh" \
      -oracle "${HOST_GATEWAY_ALIAS}:${POSTURE_ORACLE_PORT}" -kubemq "${HOST_GATEWAY_ALIAS}:${POSTURE_KUBEMQ_PORT}" \
      -oracle-container "${POSTURE_ORACLE_CONTAINER}" -kubemq-container "${POSTURE_KUBEMQ_CONTAINER}" ;;
  finish)
    "${BIN}" finish -matrix "${ROOT}/matrix/perf.yaml" -dir "${DIR}" -out "${TRACK_RESULTS}"
    # Gigabytes of broker data, written by the brokers' own users; never kept. The pair is already
    # down. Removed from a container running as root, so file ownership does not matter.
    "${HERE}/ctl.sh" client --user 0 --entrypoint sh -v "${DIR}:/d" -- "${KAFKA_IMAGE_REPO}@${KAFKA_IMAGE_DIGEST}" \
      -c 'rm -rf /d/kafka-data /d/kubemq-data' || echo "[perf] could not remove ${DIR}/*-data" >&2 ;;
  *) echo "[perf] unknown step $1" >&2; exit 2 ;;
esac
