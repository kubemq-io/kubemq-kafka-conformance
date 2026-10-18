# shellcheck shell=bash
#
# runner/lib/engine.sh — the container engine abstraction.
#
# This is the ONLY file in the repository allowed to name a specific engine binary or a
# specific host-gateway alias. Everything else calls the functions below. `pr.yml` enforces
# that with a grep.
#
# After engine_detect:
#   CERT_ENGINE          docker | podman — the binary the runner calls
#   CERT_ENGINE_RUNTIME  docker | podman — what actually runs the containers (the docker CLI
#                        can talk to a podman socket; the alias then follows the runtime)
#   CERT_ENGINE_ARCH     amd64 | arm64 — the architecture containers run as
#   CERT_COMPOSE         the compose command, as an array: CERT_COMPOSE=(docker compose)
#   HOST_GATEWAY_ALIAS   the hostname a container uses to reach the host
#   CERT_BIND_ADDR       host address published ports bind to
#
# Input:
#   CONTAINER_ENGINE     optional: docker | podman. Unset means auto-detect (docker first).
#
# Bash 3.2 compatible (macOS /bin/bash).

engine__die() { echo "[engine] ERROR: $*" >&2; exit "${ENGINE_DIE_CODE:-2}"; }
engine__log() { echo "[engine] $*" >&2; }

# engine__usable <bin>: the binary exists and its daemon / machine answers.
engine__usable() {
  command -v "$1" >/dev/null 2>&1 || return 1
  "$1" info >/dev/null 2>&1
}

engine_detect() {
  local want="${CONTAINER_ENGINE:-}"
  case "${want}" in
    "") if engine__usable docker; then CERT_ENGINE=docker
        elif engine__usable podman; then CERT_ENGINE=podman
        else engine__die "no usable container engine: neither 'docker info' nor 'podman info' answers. Start Docker Desktop, the Docker daemon, or 'podman machine start'."
        fi ;;
    docker|podman)
        engine__usable "${want}" || engine__die "CONTAINER_ENGINE=${want} but '${want} info' does not answer. Is the daemon or machine running?"
        CERT_ENGINE="${want}" ;;
    *)  engine__die "CONTAINER_ENGINE='${want}' is not supported (use docker or podman)" ;;
  esac

  # Runtime: the docker CLI pointed at a podman socket reports a "Podman Engine" component.
  CERT_ENGINE_RUNTIME="${CERT_ENGINE}"
  if [[ "${CERT_ENGINE}" == docker ]] \
     && docker version --format '{{range .Server.Components}}{{.Name}};{{end}}' 2>/dev/null | grep -qi podman; then
    CERT_ENGINE_RUNTIME=podman
  fi

  # Architecture containers run as (the engine's VM on macOS, the host on Linux).
  local arch
  if [[ "${CERT_ENGINE}" == podman ]]; then
    arch="$(podman info --format '{{.Host.Arch}}' 2>/dev/null)"
  else
    arch="$(docker version --format '{{.Server.Arch}}' 2>/dev/null)"
  fi
  case "${arch}" in
    amd64|x86_64) CERT_ENGINE_ARCH=amd64 ;;
    arm64|aarch64) CERT_ENGINE_ARCH=arm64 ;;
    *) engine__die "container engine reports architecture '${arch}'; only amd64 and arm64 are supported" ;;
  esac

  # Compose: the engine's own subcommand first, then the standalone binaries.
  if "${CERT_ENGINE}" compose version >/dev/null 2>&1; then
    CERT_COMPOSE=("${CERT_ENGINE}" compose)
  elif command -v docker-compose >/dev/null 2>&1; then
    CERT_COMPOSE=(docker-compose)
  elif [[ "${CERT_ENGINE}" == podman ]] && command -v podman-compose >/dev/null 2>&1; then
    CERT_COMPOSE=(podman-compose)
  else
    engine__die "no compose implementation found for ${CERT_ENGINE}: install the compose plugin or docker-compose"
  fi
  # podman-compose (also what "podman compose" runs when docker-compose is absent) puts every
  # service in a pod, and podman then refuses a client that joins a broker's network with
  # "--network container:<name>" ("container dependency is part of a pod"). Run it outside a pod.
  if [[ "${CERT_ENGINE}" == podman ]] && command -v podman-compose >/dev/null 2>&1 \
     && { [[ "${CERT_COMPOSE[0]}" == podman-compose ]] \
          || podman compose version 2>/dev/null | grep -qi 'podman-compose'; }; then
    CERT_COMPOSE=(podman-compose --in-pod=false)
  fi

  # Host-gateway alias follows the runtime. Brokers only advertise it; client containers
  # started for container-based tracks must map it to the host gateway themselves on Linux
  # engines that do not inject it.
  if [[ "${CERT_ENGINE_RUNTIME}" == podman ]]; then
    HOST_GATEWAY_ALIAS="host.containers.internal"
  else
    HOST_GATEWAY_ALIAS="host.docker.internal"
  fi

  # Bind address. On macOS (Docker Desktop, podman machine) the gateway reaches ports published
  # on the host loopback, so nothing is exposed to the network. On Linux the gateway is a bridge
  # address, which a loopback-only publish cannot serve.
  if [[ -z "${CERT_BIND_ADDR:-}" ]]; then
    if [[ "$(uname -s)" == Darwin ]]; then CERT_BIND_ADDR=127.0.0.1; else CERT_BIND_ADDR=0.0.0.0; fi
  fi

  export CERT_ENGINE CERT_ENGINE_RUNTIME CERT_ENGINE_ARCH HOST_GATEWAY_ALIAS CERT_BIND_ADDR
}

engine_describe() {
  echo "engine=${CERT_ENGINE} runtime=${CERT_ENGINE_RUNTIME} arch=${CERT_ENGINE_ARCH} compose='${CERT_COMPOSE[*]}' gateway=${HOST_GATEWAY_ALIAS} bind=${CERT_BIND_ADDR}"
}

# engine_run <args...>: run the engine binary.
engine_run() { "${CERT_ENGINE}" "$@"; }

# engine_compose <compose-file> <args...>: compose with the pinned versions.
engine_compose() {
  local file="$1"; shift
  "${CERT_COMPOSE[@]}" --env-file "${CERT_ROOT}/compose/versions.env" -f "${file}" "$@"
}

# engine_inspect <inspect args...>: "engine inspect" that tells a missing object from an engine
# that cannot be reached. Sets ENGINE_INSPECT_OUT and returns 0; returns 1 when the object does
# not exist. When the engine itself does not answer (on a laptop whose TCP ports are exhausted by
# other work the CLI cannot reach its socket), it retries with backoff for up to
# ENGINE_UNREACHABLE_TIMEOUT seconds (default 60), then dies with exit 2: an unreachable engine is
# an infrastructure failure, never evidence that a broker exited. It must run in the calling
# shell, not in $(...) or a pipe, so that exit stops the run; hence the variable, not stdout.
engine_inspect() {
  local err waited=0 delay=1 limit="${ENGINE_UNREACHABLE_TIMEOUT:-60}"
  ENGINE_INSPECT_OUT=""
  err="$(mktemp)"
  while :; do
    if ENGINE_INSPECT_OUT="$(engine_run "$@" 2>"${err}")"; then
      rm -f "${err}"; return 0
    fi
    if grep -qiE 'no such (container|object|image|volume)' "${err}"; then
      rm -f "${err}"; return 1
    fi
    if (( waited >= limit )); then
      engine__die "the container engine did not answer for ${waited}s ($(tr '\n' ' ' <"${err}" | cut -c1-200)); nothing was certified. Retry when the machine has free TCP ports and '${CERT_ENGINE} info' answers."
    fi
    (( waited == 0 )) && engine__log "container engine not answering; retrying for up to ${limit}s"
    sleep "${delay}"
    waited=$((waited + delay))
    (( delay < 8 )) && delay=$((delay * 2))
  done
}

# engine_image_digests <container>: every repo digest of the image a container runs, one per line,
# in ENGINE_IMAGE_DIGESTS. Returns 1 when the container does not exist.
engine_image_digests() {
  engine_inspect inspect --format '{{.Image}}' "$1" || return 1
  engine_inspect image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "${ENGINE_INSPECT_OUT}" || return 1
  # shellcheck disable=SC2034 # read by callers (runner/lib/brokers.sh)
  ENGINE_IMAGE_DIGESTS="$(printf '%s\n' "${ENGINE_INSPECT_OUT}" | sed -n 's/.*@\(sha256:[0-9a-f]*\).*/\1/p')"
}

# engine_container_exists <container>: exit 0 when the container exists (any state).
engine_container_exists() {
  engine_inspect inspect --format '{{.Id}}' "$1"
}

# engine_container_running <container>: exit 0 when the container exists and is running; 1 when
# it is stopped or absent. An engine that does not answer is retried, then exit 2 (engine_inspect).
engine_container_running() {
  engine_inspect inspect --format '{{.State.Running}}' "$1" || return 1
  [[ "${ENGINE_INSPECT_OUT}" == "true" ]]
}

# engine_logs <container> <file>: save a container's log to a file (never to git), with the
# license key redacted in case anything echoes it.
engine_logs() {
  engine_run logs "$1" 2>&1 | redact_secrets >"$2" || true
}

# redact_secrets: stdin to stdout with the value of KUBEMQ_LICENSE_KEY replaced.
redact_secrets() {
  if [[ -n "${KUBEMQ_LICENSE_KEY:-}" ]]; then
    REDACT_VALUE="${KUBEMQ_LICENSE_KEY}" awk '
      BEGIN { k = ENVIRON["REDACT_VALUE"]; n = length(k) }
      { line = $0; out = ""
        while ((i = index(line, k)) > 0) { out = out substr(line, 1, i - 1) "<redacted>"; line = substr(line, i + n) }
        print out line }'
  else
    cat
  fi
}

# engine_version: the engine server version, recorded in every report.
engine_version() {
  local v
  v="$(engine_run version --format '{{.Server.Version}}' 2>/dev/null)" || v=""
  if [[ -z "${v}" && "${CERT_ENGINE}" == podman ]]; then
    v="$(podman --version 2>/dev/null | awk '{print $NF}')"
  fi
  echo "${v:-unknown}"
}

# engine_client_run [engine run options...] -- <image> [command...]: run a short-lived client
# container that can reach both brokers through HOST_GATEWAY_ALIAS. Docker on Linux does not
# inject the alias, so it is mapped to the host gateway there; Docker Desktop and podman inject
# it themselves. The container is removed when it exits.
engine_client_run() {
  local opts=() o joined=0
  while [[ $# -gt 0 && "$1" != "--" ]]; do opts+=("$1"); shift; done
  [[ "${1:-}" == "--" ]] && shift
  # A client that joins another container's network namespace ("--network container:<name>")
  # uses that container's name resolution; Docker refuses a host mapping beside it.
  # Both spellings: "--network container:<name>" and "--network=container:<name>" (or --net).
  for o in ${opts[@]+"${opts[@]}"}; do
    case "${o}" in container:*|--network=container:*|--net=container:*) joined=1 ;; esac
  done
  if [[ "${CERT_ENGINE_RUNTIME}" == docker && "$(uname -s)" == Linux && "${joined}" == 0 ]]; then
    opts+=(--add-host "${HOST_GATEWAY_ALIAS}:host-gateway")
  fi
  engine_run run --rm ${opts[@]+"${opts[@]}"} "$@"
}

# engine_clock_skew: seconds between the container engine's clock and the host's (positive when
# the engine is behind). A podman or Docker VM drifts after the host sleeps; KubeMQ then refuses
# its license ("iat is in the future").
engine_clock_skew() {
  local image="$1" host engine
  host="$(date -u +%s)"
  engine="$(engine_run run --rm --entrypoint date "${image}" -u +%s 2>/dev/null)" || return 1
  echo $(( host - engine ))
}

# engine_clock_fix_hint: how to resync this engine's clock, for error messages.
engine_clock_fix_hint() {
  if [[ "${CERT_ENGINE_RUNTIME}" == podman ]]; then
    echo "podman machine ssh -- sudo chronyc makestep (or restart the podman machine)"
  else
    echo "restart Docker Desktop (or resync the Docker host's clock)"
  fi
}

# engine_manifest_push <list-ref> <image-ref>...: publish one multi-architecture image (an index)
# made of per-architecture images already pushed to the registry, and print its digest. Used only
# by operator publishing (suites/clients/publish-drivers.sh), never by a certification run.
engine_manifest_push() {
  local list="$1" digest
  shift
  if [[ "${CERT_ENGINE}" == podman ]]; then
    local f
    f="$(mktemp)"
    podman manifest rm "${list}" >/dev/null 2>&1 || true
    podman manifest create "${list}" "$@" >/dev/null || return 1
    podman manifest push --all --digestfile "${f}" "${list}" "docker://${list}" >/dev/null || return 1
    digest="$(cat "${f}")"
    rm -f "${f}"
  else
    docker manifest rm "${list}" >/dev/null 2>&1 || true
    docker manifest create "${list}" "$@" >/dev/null || return 1
    digest="$(docker manifest push "${list}" | grep -o 'sha256:[0-9a-f]*' | tail -1)" || return 1
  fi
  echo "${digest}"
}
