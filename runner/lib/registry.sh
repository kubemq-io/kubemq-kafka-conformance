# shellcheck shell=bash
#
# runner/lib/registry.sh — resolve an image tag to digests over the Docker Registry HTTP API,
# anonymously, with curl and jq only (no engine, no login).
#
#   registry_resolve <image repo> <tag>
#
# Sets REG_INDEX_DIGEST, REG_AMD64_DIGEST, REG_ARM64_DIGEST. The tag must be a multi-architecture
# index with exactly one linux/amd64 and one linux/arm64 image; the index digest is checked
# against the bytes served and each architecture's manifest is fetched to prove it exists.
# Dies (exit REGISTRY_DIE_CODE, default 2) with a message that names what was wrong.
# docker.io is read at registry-1.docker.io. Bash 3.2 compatible.

reg__die() { echo "${REGISTRY_LOG_PREFIX:-[registry]} ERROR: $*" >&2; exit "${REGISTRY_DIE_CODE:-2}"; }
reg__log() { echo "${REGISTRY_LOG_PREFIX:-[registry]} $*" >&2; }

reg__sha256() {
  if command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | awk '{print "sha256:" $1}'
  else sha256sum "$1" | awk '{print "sha256:" $1}'; fi
}

REG__INDEX_TYPES="application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json"
REG__IMAGE_TYPES="application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"

# reg__token: anonymous pull token from the realm the registry names in its 401 challenge.
reg__token() {
  local challenge realm service
  REG__TOKEN=""
  challenge="$(curl -sS -o /dev/null -D - "https://${REG__HOST}/v2/" | tr -d '\r' | sed -n 's/^[Ww][Ww][Ww]-[Aa]uthenticate: *//p')" \
    || reg__die "cannot reach https://${REG__HOST}/v2/"
  [[ -z "${challenge}" ]] && return 0   # open registry, no token
  realm="$(echo "${challenge}" | sed -n 's/.*realm="\([^"]*\)".*/\1/p')"
  service="$(echo "${challenge}" | sed -n 's/.*service="\([^"]*\)".*/\1/p')"
  [[ -n "${realm}" ]] || reg__die "registry ${REG__HOST} sent an authentication challenge without a realm: ${challenge}"
  REG__TOKEN="$(curl -sS -G "${realm}" --data-urlencode "scope=repository:${REG__PATH}:pull" \
                  ${service:+--data-urlencode "service=${service}"} | jq -r '.token // .access_token // empty')" \
    || reg__die "cannot get an anonymous pull token from ${realm}"
  [[ -n "${REG__TOKEN}" ]] || reg__die "registry ${REG__HOST} gave no anonymous pull token for ${REG__PATH}"
}

# reg__fetch <reference> <accept> <body-file> <headers-file>: GET a manifest; prints the HTTP status.
reg__fetch() {
  local auth=()
  [[ -n "${REG__TOKEN}" ]] && auth=(-H "Authorization: Bearer ${REG__TOKEN}")
  curl -sS ${auth[@]+"${auth[@]}"} -H "Accept: $2" -D "$4" -o "$3" -w '%{http_code}' \
    "https://${REG__HOST}/v2/${REG__PATH}/manifests/$1" || reg__die "cannot fetch ${REG__REPO}:$1"
}

# reg__arch <index-file> <arch>: the linux/<arch> digest in the index; exactly one must exist.
reg__arch() {
  local d n
  d="$(jq -r --arg a "$2" '[.manifests[] | select(.platform.os == "linux" and .platform.architecture == $a)] | map(.digest) | .[]' "$1")"
  n="$(printf '%s' "${d}" | grep -c . || true)"
  [[ "${n}" == 1 ]] || reg__die "${REG__REPO}:${REG__TAG} lists ${n} linux/$2 images; exactly one is required"
  echo "${d}"
}

registry_resolve() {
  REG__REPO="$1"; REG__TAG="$2"
  REG__HOST="${REG__REPO%%/*}"; REG__PATH="${REG__REPO#*/}"
  [[ "${REG__HOST}" == docker.io ]] && REG__HOST=registry-1.docker.io
  local work status media header d
  work="$(mktemp -d)"
  reg__token
  reg__log "resolving ${REG__REPO}:${REG__TAG}"
  status="$(reg__fetch "${REG__TAG}" "${REG__INDEX_TYPES}" "${work}/index.json" "${work}/index.headers")"
  case "${status}" in
    200) ;;
    404) rm -rf "${work}"; reg__die "tag ${REG__TAG} does not exist in ${REG__REPO}" ;;
    *) rm -rf "${work}"; reg__die "registry answered HTTP ${status} for ${REG__REPO}:${REG__TAG}" ;;
  esac
  media="$(jq -r '.mediaType // empty' "${work}/index.json")"
  case "${media}" in
    application/vnd.oci.image.index.v1+json|application/vnd.docker.distribution.manifest.list.v2+json) ;;
    *) rm -rf "${work}"; reg__die "${REG__REPO}:${REG__TAG} is not a multi-architecture index (media type '${media}'); the kit needs amd64 and arm64" ;;
  esac
  REG_INDEX_DIGEST="$(reg__sha256 "${work}/index.json")"
  header="$(tr -d '\r' <"${work}/index.headers" | sed -n 's/^[Dd]ocker-[Cc]ontent-[Dd]igest: *//p' | tail -1)"
  if [[ -n "${header}" && "${header}" != "${REG_INDEX_DIGEST}" ]]; then
    rm -rf "${work}"; reg__die "registry says the index digest is ${header} but the bytes served hash to ${REG_INDEX_DIGEST}"
  fi
  REG_AMD64_DIGEST="$(reg__arch "${work}/index.json" amd64)"
  REG_ARM64_DIGEST="$(reg__arch "${work}/index.json" arm64)"
  for d in "${REG_AMD64_DIGEST}" "${REG_ARM64_DIGEST}"; do
    status="$(reg__fetch "${d}" "${REG__IMAGE_TYPES}" "${work}/image.json" "${work}/image.headers")"
    [[ "${status}" == 200 ]] || { rm -rf "${work}"; reg__die "index lists ${d} but the registry answered HTTP ${status} for it"; }
    [[ "$(reg__sha256 "${work}/image.json")" == "${d}" ]] || { rm -rf "${work}"; reg__die "manifest ${d} served bytes with a different digest"; }
  done
  rm -rf "${work}"
  reg__log "index ${REG_INDEX_DIGEST}"
  reg__log "linux/amd64 ${REG_AMD64_DIGEST}"
  reg__log "linux/arm64 ${REG_ARM64_DIGEST}"
  export REG_INDEX_DIGEST REG_AMD64_DIGEST REG_ARM64_DIGEST
}
