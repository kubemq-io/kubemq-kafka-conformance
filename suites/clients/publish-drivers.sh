#!/usr/bin/env bash
# Operator only: publish prebuilt multi-architecture driver images, so a rerun compiles nothing.
#
#   suites/clients/publish-drivers.sh [driver...]      (default: every driver)
#
# Each driver is built for linux/amd64 and linux/arm64 from its current sources and pins, pushed
# to the private registry (DRIVER_REGISTRY in clients.env; PUBLISH_REGISTRY overrides it) as
# <registry>/<driver>:<source hash>-<arch>, and joined into one
# multi-architecture image <registry>/<driver>:<source hash>. The script prints the clients.env
# lines that pin it by digest (<NAME>_DRIVER_IMAGE) with its source hash (<NAME>_DRIVER_SOURCE);
# paste them into clients.env. Needs write access to the registry (gcloud credentials) and an
# engine that can build both architectures (emulation for the other one).
# shellcheck source-path=SCRIPTDIR
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
CERT_ROOT="${ROOT}"
export CERT_ROOT
# shellcheck source=../../runner/lib/engine.sh
. "${ROOT}/runner/lib/engine.sh"
engine_detect
set -a
# shellcheck source=clients.env
. "${HERE}/clients.env"
set +a
REGISTRY="${PUBLISH_REGISTRY:-${DRIVER_REGISTRY:?DRIVER_REGISTRY is pinned in clients.env}}"
log() { echo "[publish] $*" >&2; }

drivers=("$@")
if [[ ${#drivers[@]} -eq 0 ]]; then
  for d in "${HERE}"/drivers/*/; do
    d="$(basename "${d}")"
    [[ "${d}" == golib ]] || drivers+=("${d}")
  done
fi

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
for name in "${drivers[@]}"; do
  dir="${HERE}/drivers/${name}"
  upper="$(echo "${name}" | tr '[:lower:]-' '[:upper:]_')"
  hash="$("${HERE}/build-driver.sh" --hash "${name}")"
  ref="${REGISTRY}/${name}:${hash}"
  parts=()
  for arch in amd64 arm64; do
    log "${name}: building linux/${arch} (sources ${hash})"
    if [[ -f "${dir}/Containerfile" ]]; then
      args=()
      while IFS='=' read -r k _; do
        [[ "${k}" =~ ^[A-Z][A-Z0-9_]*$ ]] && args+=(--build-arg "${k}=${!k}")
      done <"${HERE}/clients.env"
      b="${upper}_BUILD_IMAGE"; r="${upper}_RUNTIME_IMAGE"
      args+=(--build-arg "BUILD_IMAGE=${!b:-}" --build-arg "RUNTIME_IMAGE=${!r:-}")
      engine_run build -q --platform "linux/${arch}" -t "${ref}-${arch}" -f "${dir}/Containerfile" "${args[@]}" "${dir}" >/dev/null
    else
      mkdir -p "${work}/${name}-${arch}"
      (cd "${HERE}" && CGO_ENABLED=0 GOOS=linux GOARCH="${arch}" go build -trimpath -o "${work}/${name}-${arch}/driver" "./drivers/${name}")
      engine_run build -q --platform "linux/${arch}" -t "${ref}-${arch}" -f "${HERE}/drivers/go.Containerfile" \
        --build-arg "RUNTIME_IMAGE=${GO_DRIVER_IMAGE}" "${work}/${name}-${arch}" >/dev/null
    fi
    engine_run push -q "${ref}-${arch}" >/dev/null
    parts+=("${ref}-${arch}")
  done
  digest="$(engine_manifest_push "${ref}" "${parts[@]}")"
  [[ "${digest}" == sha256:* ]] || { log "${name}: no digest for ${ref}"; exit 1; }
  log "${name}: ${ref} -> ${digest}"
  echo "${upper}_DRIVER_IMAGE=${REGISTRY}/${name}@${digest}"
  echo "${upper}_DRIVER_SOURCE=${hash}"
done
