#!/usr/bin/env bash
# Features track entry point (Track B), called by runner/certify.sh.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
: "${ORACLE_BOOTSTRAP:?}" "${KUBEMQ_BOOTSTRAP:?}" "${TRACK_RESULTS:?}" "${CERT_RUN_DIR:?}"
BIN="${CERT_RUN_DIR}/bin/features-track"
mkdir -p "$(dirname "${BIN}")"
(cd "${HERE}" && go build -o "${BIN}" ./cmd/features-track)
exec "${BIN}" -matrix "${ROOT}/matrix/features.yaml" -oracle "${ORACLE_BOOTSTRAP}" -kubemq "${KUBEMQ_BOOTSTRAP}" \
  -out "${TRACK_RESULTS}" -gaps "${ROOT}/docs/gaps.md" ${FEATURES_ONLY:+-only "${FEATURES_ONLY}"} \
  -client-run "${ROOT}/runner/client-run.sh" -java-image "${KAFKA_IMAGE_REPO:?}@${KAFKA_IMAGE_DIGEST:?}" \
  ${CERT_INCLUDE_OPT_IN:+-include-opt-in}
