#!/usr/bin/env bash
# The image a client driver runs in: suites/clients/build-driver.sh <driver>
#
# A driver with a prebuilt image pinned in clients.env (<NAME>_DRIVER_IMAGE, a multi-arch image by
# digest) and a matching source hash (<NAME>_DRIVER_SOURCE) runs that image: nothing is compiled.
# Otherwise the driver is built locally: an image from drivers/<driver>/Containerfile, tagged with
# its source hash so an unchanged driver is not rebuilt. A prebuilt image whose recorded source
# hash differs from the current sources is stale and is ignored (said on stderr). The registry is
# private: without access to it, the local build is used.
#
# Prints the image to run. For a Go driver (no Containerfile) without a usable prebuilt image it
# prints nothing: the track then cross-compiles the binary and mounts it into GO_DRIVER_IMAGE.
#
#   build-driver.sh --hash <driver>   print the driver's source hash and exit
#
# Needs the CERT_ENGINE* variables runner/certify.sh exports (not for --hash).
# shellcheck source-path=SCRIPTDIR
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
sum() { if command -v shasum >/dev/null 2>&1; then shasum -a 256; else sha256sum; fi; }

# source_hash <driver>: the driver's own files, plus the shared Go code for a Go driver, plus the
# pin values of clients.env (comments excluded, and without the prebuilt-image lines, which record
# this very hash, or the registry name, which does not change what is built).
source_hash() {
  local dir="${HERE}/drivers/$1"
  {
    (cd "${dir}" && find . -type f -not -path './target/*' -not -path './bin/*' -not -path './obj/*' | LC_ALL=C sort | xargs cat)
    if [[ ! -f "${dir}/Containerfile" ]]; then
      (cd "${HERE}" && find drivers/golib -type f | LC_ALL=C sort | xargs cat; cat go.mod go.sum)
    fi
    grep -E '^[A-Z][A-Z0-9_]*=' "${HERE}/clients.env" | grep -v -E '^([A-Z0-9_]+_DRIVER_(IMAGE|SOURCE)|DRIVER_REGISTRY)='
  } | sum | cut -c1-16
}

if [[ "${1:-}" == --hash ]]; then
  source_hash "${2:?driver name}"
  exit 0
fi

# shellcheck source=../../runner/lib/engine.sh
. "${ROOT}/runner/lib/engine.sh"
: "${CERT_ENGINE:?run from runner/certify.sh}" "${CERT_ENGINE_ARCH:?}"
set -a
# shellcheck source=clients.env
. "${HERE}/clients.env"
set +a
name="${1:?driver name}"
dir="${HERE}/drivers/${name}"
upper="$(echo "${name}" | tr '[:lower:]-' '[:upper:]_')"
hash="$(source_hash "${name}")"

pin="${upper}_DRIVER_IMAGE"; pin_src="${upper}_DRIVER_SOURCE"
if [[ -n "${!pin:-}" ]]; then
  if [[ "${!pin_src:-}" != "${hash}" ]]; then
    echo "[build-driver] ${name}: prebuilt image is for sources ${!pin_src:-unknown}, current sources are ${hash}: building locally" >&2
  elif engine_run image inspect "${!pin}" >/dev/null 2>&1 || engine_run pull -q "${!pin}" >/dev/null 2>&1; then
    echo "${!pin}"
    exit 0
  else
    echo "[build-driver] ${name}: prebuilt image ${!pin} is not reachable (the registry is private): building locally" >&2
  fi
fi

if [[ ! -f "${dir}/Containerfile" ]]; then
  exit 0 # a Go driver: the track builds and mounts the binary
fi
tag="localhost/kcert-driver-${name}:${hash}-${CERT_ENGINE_ARCH}"
if ! engine_run image inspect "${tag}" >/dev/null 2>&1; then
  args=()
  while IFS='=' read -r k _; do
    [[ "${k}" =~ ^[A-Z][A-Z0-9_]*$ ]] && args+=(--build-arg "${k}=${!k}")
  done <"${HERE}/clients.env"
  b="${upper}_BUILD_IMAGE"; r="${upper}_RUNTIME_IMAGE"
  args+=(--build-arg "BUILD_IMAGE=${!b:-}" --build-arg "RUNTIME_IMAGE=${!r:-}")
  engine_run build -q -t "${tag}" -f "${dir}/Containerfile" "${args[@]}" "${dir}" >&2
fi
echo "${tag}"
