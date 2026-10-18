# shellcheck shell=bash
#
# runner/lib/preflight.sh — checks that run before anything is started. Each one dies with a
# message that says what to do. Requires engine.sh (engine_detect already called) and
# port-preflight.sh to be sourced.

pre__die() { echo "[preflight] ERROR: $*" >&2; exit "${PREFLIGHT_DIE_CODE:-2}"; }
pre__log() { echo "[preflight] $*" >&2; }

# preflight_license: KUBEMQ_LICENSE_KEY must be set. The value is never printed.
preflight_license() {
  local key="${KUBEMQ_LICENSE_KEY:-}"
  if [[ -z "${key//[[:space:]]/}" ]]; then
    pre__die "KUBEMQ_LICENSE_KEY is not set.
  The certification runs the released KubeMQ image, which needs a license key.
  Request a KubeMQ evaluation key, then:
    export KUBEMQ_LICENSE_KEY=<your key>
  and run this command again. The key is never printed or written to any report."
  fi
  if [[ "${key}" =~ [[:space:]] ]]; then
    pre__die "KUBEMQ_LICENSE_KEY contains whitespace; check that it was pasted as one line"
  fi
  pre__log "license key: present (${#key} characters, value not shown)"
}

# preflight_tools: host tools the runner itself needs.
preflight_tools() {
  local t missing=""
  for t in curl lsof awk sed jq tar git; do
    command -v "$t" >/dev/null 2>&1 || missing="${missing} $t"
  done
  [[ -z "${missing}" ]] || pre__die "missing host tools:${missing}"
}

# preflight_arch: the pinned images must carry a manifest for the engine's architecture, and
# a mismatch between host and engine architecture (emulation) is refused, because emulated
# brokers time out in ways the oracle model cannot tell apart from real failures.
preflight_arch() {
  local host_arch
  case "$(uname -m)" in
    x86_64|amd64) host_arch=amd64 ;;
    arm64|aarch64) host_arch=arm64 ;;
    *) pre__die "host architecture '$(uname -m)' is not supported (amd64 or arm64 only)" ;;
  esac
  if [[ "${host_arch}" != "${CERT_ENGINE_ARCH}" ]]; then
    pre__die "host is ${host_arch} but the container engine runs ${CERT_ENGINE_ARCH} (emulation). Use a native engine; on Apple silicon disable amd64 emulation for this run."
  fi
  local var
  for var in KAFKA_IMAGE_DIGEST KUBEMQ_IMAGE_DIGEST; do
    local per
    per="${var}_$(echo "${CERT_ENGINE_ARCH}" | tr '[:lower:]' '[:upper:]')"
    [[ -n "${!per:-}" ]] || pre__die "compose/versions.env has no ${per}: the pinned image does not list ${CERT_ENGINE_ARCH}"
  done
  pre__log "architecture: host ${host_arch}, engine ${CERT_ENGINE_ARCH}, pinned images carry ${CERT_ENGINE_ARCH}"
}

# preflight_ephemeral <port>...: refuse ports inside the operating system's range for outgoing
# connections. Any program's outgoing connection can hold such a port at any moment, and a
# publish then fails with "address already in use" although nothing listens there.
preflight_ephemeral() {
  local first last p
  if [[ "$(uname -s)" == Darwin ]]; then
    first="$(sysctl -n net.inet.ip.portrange.first 2>/dev/null)"
    last="$(sysctl -n net.inet.ip.portrange.last 2>/dev/null)"
  elif [[ -r /proc/sys/net/ipv4/ip_local_port_range ]]; then
    read -r first last </proc/sys/net/ipv4/ip_local_port_range
  fi
  [[ -n "${first:-}" && -n "${last:-}" ]] || return 0
  for p in "$@"; do
    if (( p >= first && p <= last )); then
      pre__die "port ${p} is inside this machine's range for outgoing connections (${first}-${last}); any program may hold it at any moment. Choose a port below ${first} (compose/versions.env or the *_PORT variables)."
    fi
  done
}

# preflight_ports <port>...: none reserved, none in the outgoing-connection range, all free
# (vendored port-preflight.sh).
preflight_ports() {
  preflight_ephemeral "$@"
  # A previous run's broker may still be releasing its sockets: wait up to PORT_FREE_WAIT seconds
  # (default 30) for every port to be free, then let preflight_free give the definitive answer.
  refuse_reserved_ports "$@"
  wait_ports_free "${PORT_FREE_WAIT:-30}" "$@" || true
  PF_LOG_PREFIX="[preflight]" preflight_free "$@"
  pre__log "ports free: $*"
}

# pick_port_block <run id>: when no port was given, the run's ports come from its run id, so two
# runs on one machine never ask for the same ports. Block i (0-99) starts at
# B = CERT_PORT_BLOCK_BASE + 20*i (compose/versions.env, 24000): Kafka host listener B, gateway
# B+5, KubeMQ Kafka B+10, KubeMQ API B+15. The gaps leave room for the posture pairs' fixed
# offsets (B+1..B+3, B+11..B+12, B+16). The run id picks the first block to try; a block with any
# of those ports in use is skipped. Exports KAFKA_HOST_PORT KAFKA_GATEWAY_PORT KUBEMQ_KAFKA_PORT
# KUBEMQ_API_PORT.
pick_port_block() {
  local base="${CERT_PORT_BLOCK_BASE:?compose/versions.env sets CERT_PORT_BLOCK_BASE}" start i slot b q busy
  start=$(( $(printf '%s' "$1" | cksum | awk '{print $1}') % 100 ))
  for ((i = 0; i < 100; i++)); do
    slot=$(( (start + i) % 100 ))
    b=$(( base + slot * 20 ))
    busy=""
    for q in $b $((b + 1)) $((b + 2)) $((b + 3)) $((b + 5)) $((b + 10)) $((b + 11)) $((b + 12)) $((b + 15)) $((b + 16)); do
      if [[ -n "$(pf__listen_pids "$q")" ]] || port_answers 127.0.0.1 "$q"; then busy=1; break; fi
    done
    if [[ -z "${busy}" ]]; then
      KAFKA_HOST_PORT=$b KAFKA_GATEWAY_PORT=$((b + 5)) KUBEMQ_KAFKA_PORT=$((b + 10)) KUBEMQ_API_PORT=$((b + 15))
      export KAFKA_HOST_PORT KAFKA_GATEWAY_PORT KUBEMQ_KAFKA_PORT KUBEMQ_API_PORT
      pre__log "ports: block ${slot} chosen from run id $1 (Kafka ${b}, gateway $((b + 5)), KubeMQ $((b + 10)), API $((b + 15)))"
      return 0
    fi
  done
  pre__die "no free port block between ${base} and $((base + 1999)); set KAFKA_HOST_PORT KAFKA_GATEWAY_PORT KUBEMQ_KAFKA_PORT KUBEMQ_API_PORT"
}

# wait_ports_free <seconds> <port>...: return 0 as soon as nothing listens on or answers at any
# port, 1 when the time runs out.
wait_ports_free() {
  local limit="$1" i p busy
  shift
  for ((i = 0; i <= limit; i++)); do
    busy=""
    for p in "$@"; do
      if [[ -n "$(pf__listen_pids "$p")" ]] || port_answers 127.0.0.1 "$p"; then busy="${busy} $p"; fi
    done
    [[ -z "${busy}" ]] && return 0
    [[ "$i" == 0 ]] && pre__log "waiting up to ${limit}s for ports to be released:${busy}"
    sleep 1
  done
  return 1
}

# preflight_go <minimum>: the protocol track and the report renderer are Go programs built at
# run time (prebuilt driver images are later work), so a missing or old Go fails here, before
# anything starts, instead of turning into NOT-RUN rows and a missing report.
preflight_go() {
  local want="$1" have
  command -v go >/dev/null 2>&1 || pre__die "Go ${want} or newer is required (the protocol track and the report renderer are built at run time). Install it from https://go.dev/dl/"
  have="$(go env GOVERSION 2>/dev/null | sed 's/^go//')"
  if ! printf '%s\n%s\n' "${want}" "${have}" | sort -t. -k1,1n -k2,2n -k3,3n -C; then
    pre__die "Go ${have} is too old; ${want} or newer is required (suites/protocol/go.mod)"
  fi
  pre__log "Go ${have} (minimum ${want})"
}

# preflight_clock: the engine's clock must be within 60 s of the host's. KubeMQ validates its
# license token's issue time against its own clock and refuses to start when the engine clock is
# behind.
preflight_clock() {
  local skew
  skew="$(engine_clock_skew "${KAFKA_IMAGE_REPO}@${KAFKA_IMAGE_DIGEST}")" || { pre__log "clock: could not read the engine clock; skipped"; return 0; }
  if (( skew > 60 || skew < -60 )); then
    pre__die "the container engine's clock is ${skew}s off the host's. KubeMQ will refuse its license.
  Resync the engine's clock: $(engine_clock_fix_hint)"
  fi
  pre__log "clock: engine within ${skew#-}s of the host"
}

# preflight_tcp_load: warn (never fail) when the machine already has more than 10,000 TCP
# connections. Another workload's bursts can then exhaust the local port range, and rows fail
# with "can't assign requested address" for reasons that have nothing to do with the brokers.
preflight_tcp_load() {
  local n=""
  if [[ "$(uname -s)" == Darwin ]]; then
    n="$(sysctl -n net.inet.tcp.pcbcount 2>/dev/null)"
  elif [[ -r /proc/net/tcp ]]; then
    n=$(( $(wc -l </proc/net/tcp) - 1 + $( [[ -r /proc/net/tcp6 ]] && echo $(( $(wc -l </proc/net/tcp6) - 1 )) || echo 0 ) ))
  fi
  [[ -n "${n}" ]] || return 0
  if (( n > 10000 )); then
    pre__log "WARNING: ${n} TCP connections are open on this machine. Another workload may exhaust the local port range during the run; rows can then fail with \"can't assign requested address\". Prefer a quiet machine."
  else
    pre__log "TCP connections on this machine: ${n}"
  fi
}
