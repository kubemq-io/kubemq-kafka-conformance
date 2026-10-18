#!/usr/bin/env bash
#
# runner/regen-apikeys.sh — regenerate matrix/apikeys.yaml against the pinned Kafka oracle and
# KubeMQ, so the committed matrix always describes the KubeMQ version compose/versions.env pins.
#
#   runner/regen-apikeys.sh [--versions-file <path>] [--out <path>]
#
# Boots the oracle and KubeMQ under their own run id and port block (the same mechanisms as
# runner/certify.sh, so it can run beside a kit run), asks both for ApiVersions with
# suites/protocol/cmd/apikeys-gen, writes the matrix, and removes both brokers. runner/bump-kubemq.sh
# calls it with the not-yet-committed versions file of a bump, so the pin and the matrix change in
# one pull request.
#
# apikeys-gen refuses when a key that differs from the oracle has no curated impact in
# matrix/apikey-impacts.yaml; add the impact and run this again.
#
# Needs KUBEMQ_LICENSE_KEY (never printed), a container engine and Go.
# Exit codes: 0 written; 2 anything else (nothing is written to --out on failure).
#
# Bash 3.2 compatible (macOS /bin/bash).
# shellcheck source-path=SCRIPTDIR
set -Eeuo pipefail
# Any unexpected failure is exit 2, as the header says (set -e alone would keep the command's code).
trap 'echo "[regen-apikeys] ERROR: command failed at line ${LINENO}: ${BASH_COMMAND}" >&2; exit 2' ERR

CERT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CERT_ROOT

log() { echo "[regen-apikeys] $*" >&2; }
die() { echo "[regen-apikeys] ERROR: $*" >&2; exit 2; }
usage() { sed -n '3,21p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

VERSIONS_FILE="${CERT_ROOT}/compose/versions.env"
OUT="${CERT_ROOT}/matrix/apikeys.yaml"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --versions-file) [[ $# -ge 2 ]] || die "--versions-file needs a path"; VERSIONS_FILE="$2"; shift ;;
    --out) [[ $# -ge 2 ]] || die "--out needs a path"; OUT="$2"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown argument '$1'" ;;
  esac
  shift
done
[[ -f "${VERSIONS_FILE}" ]] || die "no versions file at ${VERSIONS_FILE}"

# The given versions file wins over compose/versions.env: compose reads exported variables before
# its --env-file, and brokers.sh checks the running images against these digests.
set -a
# shellcheck source=../compose/versions.env
. "${VERSIONS_FILE}"
set +a
unset KAFKA_HOST_PORT KAFKA_GATEWAY_PORT KUBEMQ_KAFKA_PORT KUBEMQ_API_PORT

CERT_RUN_ID="${CERT_RUN_ID:-g$(date -u +%m%d%H%M%S)$$}"
[[ "${CERT_RUN_ID}" =~ ^[a-z0-9]+$ ]] || die "run id '${CERT_RUN_ID}' must be lowercase letters and digits"
export CERT_RUN_ID

# shellcheck source=lib/port-preflight.sh
. "${CERT_ROOT}/runner/lib/port-preflight.sh"
# shellcheck source=lib/engine.sh
. "${CERT_ROOT}/runner/lib/engine.sh"
# shellcheck source=lib/preflight.sh
. "${CERT_ROOT}/runner/lib/preflight.sh"
# shellcheck source=lib/brokers.sh
. "${CERT_ROOT}/runner/lib/brokers.sh"

preflight_license
preflight_go "$(sed -n 's/^go \([0-9.]*\).*/\1/p' "${CERT_ROOT}/suites/protocol/go.mod")"
engine_detect
preflight_arch
preflight_clock
pick_port_block "${CERT_RUN_ID}"
preflight_ports "${KAFKA_HOST_PORT}" "${KAFKA_GATEWAY_PORT}" "${KUBEMQ_KAFKA_PORT}" "${KUBEMQ_API_PORT}"

CERT_RUN_DIR="$(mktemp -d)"
mkdir -p "${CERT_RUN_DIR}/logs"
export CERT_RUN_DIR
cleanup() {
  brokers_down
  rm -rf "${CERT_RUN_DIR}"
}
trap cleanup EXIT

oracle_up
kubemq_up 127.0.0.1

log "asking Kafka ${KAFKA_VERSION} (127.0.0.1:${KAFKA_HOST_PORT}) and KubeMQ ${KUBEMQ_VERSION} (127.0.0.1:${KUBEMQ_KAFKA_PORT}) for ApiVersions"
(cd "${CERT_ROOT}/suites/protocol" && go run ./cmd/apikeys-gen \
  -oracle "127.0.0.1:${KAFKA_HOST_PORT}" -kubemq "127.0.0.1:${KUBEMQ_KAFKA_PORT}" \
  -impacts "${CERT_ROOT}/matrix/apikey-impacts.yaml" -out "${CERT_RUN_DIR}/apikeys.yaml" \
  -oracle-version "${KAFKA_VERSION}" -kubemq-version "${KUBEMQ_VERSION}") \
  || die "apikeys-gen failed (a newly differing key needs a curated impact in matrix/apikey-impacts.yaml); ${OUT} not changed"
cat "${CERT_RUN_DIR}/apikeys.yaml" >"${OUT}"
log "wrote ${OUT} for KubeMQ ${KUBEMQ_VERSION}"
