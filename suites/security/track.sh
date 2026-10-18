#!/usr/bin/env bash
# Security track entry point (Track E), called by runner/certify.sh once per step:
#   track.sh prepare <posture>   certificates and both brokers' boot config into POSTURE_DIR
#   track.sh run <posture>       the posture's rows, against the pair the runner booted
#   track.sh finish              TRACK_RESULTS for the whole track
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
: "${CERT_RUN_DIR:?}" "${TRACK_RESULTS:?}"
BIN="${CERT_RUN_DIR}/bin/security-track"
if [[ ! -x "${BIN}" ]]; then
  mkdir -p "$(dirname "${BIN}")"
  (cd "${HERE}" && go build -o "${BIN}" ./cmd/security-track)
fi
step="${1:?step}"
case "${step}" in
  prepare|run)
    : "${POSTURE_DIR:?}"
    exec "${BIN}" "${step}" -matrix "${ROOT}/matrix/security.yaml" -gaps "${ROOT}/docs/gaps.md" \
      -posture "${2:?posture}" -dir "${POSTURE_DIR}" -hosts "localhost,127.0.0.1,::1,${HOST_GATEWAY_ALIAS:-localhost}" ;;
  finish)
    exec "${BIN}" finish -matrix "${ROOT}/matrix/security.yaml" -dir "${CERT_RUN_DIR}/postures/security" -out "${TRACK_RESULTS}" ;;
  *) echo "[security] unknown step ${step}" >&2; exit 2 ;;
esac
