#!/usr/bin/env bash
# Schema Registry track (opt-in, Track D), called by runner/certify.sh. Runs the tools track's
# driver with matrix/schema-registry.yaml.
#
# The image, confluentinc/cp-schema-registry, is under the Confluent Community License (not open
# source). It is pulled at run time from its publisher and never redistributed. The operator must
# read and accept that license explicitly (runner/certify.sh checks it too, from track.env):
#   CERT_ACCEPT_CONFLUENT_COMMUNITY_LICENSE=yes runner/certify.sh --track schema-registry
# Without it every row is NOT-RUN with that reason and nothing is pulled.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
TOOLS="${ROOT}/suites/tools"
: "${ORACLE_BOOTSTRAP:?}" "${KUBEMQ_BOOTSTRAP:?}" "${TRACK_RESULTS:?}" "${CERT_RUN_DIR:?}"
: "${KAFKA_IMAGE_REPO:?}" "${KAFKA_IMAGE_DIGEST:?}" "${SCHEMA_REGISTRY_IMAGE:?}"
TOOLS_RUN_LABEL="${CERT_RUN_ID:-$(basename "${CERT_RUN_DIR}" | tr -c 'a-zA-Z0-9\n' '-')$$}sr"
export TOOLS_RUN_LABEL
BIN="${CERT_RUN_DIR}/bin/tools-track"
mkdir -p "$(dirname "${BIN}")"
(cd "${TOOLS}" && go build -o "${BIN}" ./cmd/tools-track)
if [[ "${CERT_ACCEPT_CONFLUENT_COMMUNITY_LICENSE:-}" != "yes" ]]; then
  exec "${BIN}" -matrix "${ROOT}/matrix/schema-registry.yaml" -out "${TRACK_RESULTS}" -gaps "${ROOT}/docs/gaps.md" -ctr "${TOOLS}/ctr.sh" \
    -not-run "the Schema Registry image is under the Confluent Community License; read it and set CERT_ACCEPT_CONFLUENT_COMMUNITY_LICENSE=yes to accept it and run this track"
fi
trap '"${TOOLS}/ctr.sh" sweep "${TOOLS_RUN_LABEL}"' EXIT
"${BIN}" -matrix "${ROOT}/matrix/schema-registry.yaml" -oracle "${ORACLE_BOOTSTRAP}" -kubemq "${KUBEMQ_BOOTSTRAP}" \
  -out "${TRACK_RESULTS}" -gaps "${ROOT}/docs/gaps.md" -ctr "${TOOLS}/ctr.sh"
