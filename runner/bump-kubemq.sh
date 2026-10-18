#!/usr/bin/env bash
#
# runner/bump-kubemq.sh — pin a KubeMQ image tag by digest in compose/versions.env.
#
#   runner/bump-kubemq.sh <tag>            resolve <tag> and rewrite the KubeMQ lines
#   runner/bump-kubemq.sh <tag> --dry-run  resolve and print, change nothing
#   runner/bump-kubemq.sh --verify         re-resolve the pinned KUBEMQ_VERSION and fail if the
#                                          registry no longer serves the pinned digests for it
#
# Options:
#   --versions-file <path>   file to read and rewrite (default compose/versions.env)
#   --apikeys-file <path>    ApiKey matrix to regenerate (default matrix/apikeys.yaml)
#   --no-apikeys             rewrite the pins only; regenerate the matrix later with
#                            runner/regen-apikeys.sh before committing
#
# <tag> is a release (v3.6.17) or an approved pre-release (v3.6.17-pre.1, decision D8). Any
# other tag (rc builds, dev builds, latest) is refused: certified evidence never comes from a
# dev-tag build.
#
# The registry is the one in KUBEMQ_IMAGE_REPO (europe-docker.pkg.dev/kubemq/images/kubemq-next),
# read anonymously over the Docker Registry HTTP API with curl and jq, so resolving needs no
# engine, login or extra tool (--dry-run, --verify and --no-apikeys need nothing more). The tag
# must resolve to a multi-architecture index with a linux/amd64 and a linux/arm64 image; the
# index digest is checked against the bytes served, and each architecture's manifest is fetched
# to prove it exists.
#
# Rewritten lines: KUBEMQ_VERSION, KUBEMQ_IMAGE_DIGEST, KUBEMQ_IMAGE_DIGEST_AMD64,
# KUBEMQ_IMAGE_DIGEST_ARM64, and the "resolved" comment above them. Nothing else changes.
#
# A bump also regenerates the ApiKey matrix (matrix/apikeys.yaml) against the new image, by
# running runner/regen-apikeys.sh on the rewritten pins before anything is written: that boots
# the oracle and the new KubeMQ, so it needs KUBEMQ_LICENSE_KEY and a container engine. Both files
# are written only when both steps succeed. Commit them together in their own pull request, then
# every lane rebases and reruns. (BUMP_APIKEYS_REGEN replaces the regeneration command; the
# self-test uses it.)
#
# Exit codes: 0 done (or verified); 1 --verify found a mismatch; 2 usage, network or registry error.
#
# Bash 3.2 compatible (macOS /bin/bash).
# shellcheck source-path=SCRIPTDIR
set -Eeuo pipefail

CERT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

log() { echo "[bump-kubemq] $*" >&2; }
die() { echo "[bump-kubemq] ERROR: $*" >&2; exit 2; }
usage() { sed -n '3,37p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

TAG=""
DRY_RUN=0
VERIFY=0
VERSIONS_FILE="${CERT_ROOT}/compose/versions.env"
APIKEYS_FILE="${CERT_ROOT}/matrix/apikeys.yaml"
APIKEYS=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --verify) VERIFY=1 ;;
    --versions-file) [[ $# -ge 2 ]] || die "--versions-file needs a path"; VERSIONS_FILE="$2"; shift ;;
    --apikeys-file) [[ $# -ge 2 ]] || die "--apikeys-file needs a path"; APIKEYS_FILE="$2"; shift ;;
    --no-apikeys) APIKEYS=0 ;;
    -h|--help) usage; exit 0 ;;
    -*) usage >&2; die "unknown option '$1'" ;;
    *) [[ -z "${TAG}" ]] || die "one tag only"; TAG="$1" ;;
  esac
  shift
done

for t in curl jq awk; do command -v "$t" >/dev/null 2>&1 || die "missing host tool: $t"; done
[[ -f "${VERSIONS_FILE}" ]] || die "no versions file at ${VERSIONS_FILE}"

# env_value <key>: the value of KEY=... in the versions file.
env_value() { sed -n "s/^$1=//p" "${VERSIONS_FILE}" | tail -1; }

REPO="$(env_value KUBEMQ_IMAGE_REPO)"
[[ -n "${REPO}" ]] || die "KUBEMQ_IMAGE_REPO is not set in ${VERSIONS_FILE}"

if [[ "${VERIFY}" == 1 ]]; then
  [[ -z "${TAG}" ]] || die "--verify takes no tag; it checks the pinned KUBEMQ_VERSION"
  TAG="$(env_value KUBEMQ_VERSION)"
  [[ -n "${TAG}" ]] || die "KUBEMQ_VERSION is not set in ${VERSIONS_FILE}"
fi
[[ -n "${TAG}" ]] || { usage >&2; die "give a tag, for example v3.6.17-pre.1"; }

# Releases and approved pre-releases only.
if ! [[ "${TAG}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-pre\.[0-9]+)?$ ]]; then
  die "tag '${TAG}' is neither a release (v3.6.17) nor an approved pre-release (v3.6.17-pre.1); certified evidence never comes from rc or dev builds"
fi

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

# shellcheck source=lib/registry.sh
. "${CERT_ROOT}/runner/lib/registry.sh"
export REGISTRY_LOG_PREFIX="[bump-kubemq]"
registry_resolve "${REPO}" "${TAG}"
INDEX_DIGEST="${REG_INDEX_DIGEST}"
AMD64_DIGEST="${REG_AMD64_DIGEST}"
ARM64_DIGEST="${REG_ARM64_DIGEST}"

# --- verify mode -----------------------------------------------------------------------------
if [[ "${VERIFY}" == 1 ]]; then
  bad=0
  for pair in "KUBEMQ_IMAGE_DIGEST ${INDEX_DIGEST}" "KUBEMQ_IMAGE_DIGEST_AMD64 ${AMD64_DIGEST}" "KUBEMQ_IMAGE_DIGEST_ARM64 ${ARM64_DIGEST}"; do
    key="${pair%% *}"; want="${pair#* }"; have="$(env_value "${key}")"
    if [[ "${have}" != "${want}" ]]; then
      log "MISMATCH ${key}: pinned ${have}, registry serves ${want} for ${TAG}"; bad=1
    fi
  done
  if [[ "${bad}" == 1 ]]; then
    log "the tag ${TAG} was moved after it was pinned; the pinned digests are what was certified"
    exit 1
  fi
  log "verified: ${VERSIONS_FILE} pins exactly what ${REPO}:${TAG} serves"
  exit 0
fi

# --- rewrite ---------------------------------------------------------------------------------
OLD_VERSION="$(env_value KUBEMQ_VERSION)"
for key in KUBEMQ_VERSION KUBEMQ_IMAGE_DIGEST KUBEMQ_IMAGE_DIGEST_AMD64 KUBEMQ_IMAGE_DIGEST_ARM64; do
  [[ "$(grep -c "^${key}=" "${VERSIONS_FILE}")" == 1 ]] || die "${VERSIONS_FILE} must contain exactly one ${key}= line"
done

RESOLVED="# KubeMQ resolved $(date -u +%Y-%m-%d) by runner/bump-kubemq.sh ${TAG} (anonymous registry read)."
awk -v ver="${TAG}" -v idx="${INDEX_DIGEST}" -v amd="${AMD64_DIGEST}" -v arm="${ARM64_DIGEST}" -v resolved="${RESOLVED}" '
  /^# KubeMQ resolved / { next }
  /^KUBEMQ_VERSION=/ { print resolved; print "KUBEMQ_VERSION=" ver; next }
  /^KUBEMQ_IMAGE_DIGEST=/ { print "KUBEMQ_IMAGE_DIGEST=" idx; next }
  /^KUBEMQ_IMAGE_DIGEST_AMD64=/ { print "KUBEMQ_IMAGE_DIGEST_AMD64=" amd; next }
  /^KUBEMQ_IMAGE_DIGEST_ARM64=/ { print "KUBEMQ_IMAGE_DIGEST_ARM64=" arm; next }
  { print }
' "${VERSIONS_FILE}" >"${WORK}/versions.env"

if [[ "${DRY_RUN}" == 1 ]]; then
  diff -u "${VERSIONS_FILE}" "${WORK}/versions.env" >&2 || true
  log "--dry-run: ${VERSIONS_FILE} not changed"
  exit 0
fi

# The matrix first, from the rewritten pins in the work copy; nothing is written unless it succeeds.
if [[ "${APIKEYS}" == 1 ]]; then
  log "regenerating the ApiKey matrix against KubeMQ ${TAG}"
  "${BUMP_APIKEYS_REGEN:-${CERT_ROOT}/runner/regen-apikeys.sh}" \
      --versions-file "${WORK}/versions.env" --out "${WORK}/apikeys.yaml" \
    || die "ApiKey matrix regeneration failed; ${VERSIONS_FILE} and ${APIKEYS_FILE} were not changed (fix the cause and run the bump again, or pass --no-apikeys and run runner/regen-apikeys.sh before committing)"
fi

cat "${WORK}/versions.env" >"${VERSIONS_FILE}"
if [[ "${OLD_VERSION}" == "${TAG}" ]]; then
  log "${VERSIONS_FILE}: ${TAG} re-pinned"
else
  log "${VERSIONS_FILE}: KubeMQ ${OLD_VERSION} -> ${TAG}"
fi
if [[ "${APIKEYS}" == 1 ]]; then
  cat "${WORK}/apikeys.yaml" >"${APIKEYS_FILE}"
  log "${APIKEYS_FILE}: regenerated for ${TAG}"
  log "next: commit compose/versions.env and matrix/apikeys.yaml together in their own pull request; every lane rebases and reruns"
else
  log "--no-apikeys: run runner/regen-apikeys.sh before committing; compose/versions.env and matrix/apikeys.yaml go in one pull request"
fi
