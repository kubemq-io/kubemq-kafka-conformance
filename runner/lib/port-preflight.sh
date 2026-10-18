# Vendored from kubemq-server scripts/lib/port-preflight.sh at commit fcd45d818.
# Source of truth stays in kubemq-server; re-vendor on change, do not edit here.
# shellcheck shell=bash
#
# scripts/lib/port-preflight.sh — port-OWNERSHIP checks shared by every script that boots a
# broker on this host (tests/kafka-conformance/run.sh, scripts/cluster-start.sh,
# scripts/cluster-node-restart.sh, scripts/kafka-start.sh, scripts/kafkacert-run.sh,
# tests/kafka-conformance/kafka-share-control.sh, tests/kafka-conformance/kafka-sasl-start.sh).
#
# Why this exists. On macOS a wildcard bind (*:9092) SUCCEEDS silently next to another process's
# 127.0.0.1:9092 bind (SO_REUSEADDR), and a localhost client then reaches the MORE SPECIFIC bind —
# the other process. A readiness probe that only asks "does 127.0.0.1:<port> answer?" therefore
# passes against the wrong server, and a whole certification leg can run against a binary nobody
# meant to test. The two checks below close that trap from both ends:
#
#   preflight_free <port>...         BEFORE boot: die if <port> is RESERVED (below — checked first,
#                                    so a reserved port is never even dialled), if anything answers
#                                    on 127.0.0.1:<port> or [::1]:<port>, or if any process visible
#                                    to lsof LISTENs on <port> on any address.
#   assert_owned <pid> <port>...     AFTER boot: die unless <pid> itself LISTENs on every <port>
#                                    AND no other visible process LISTENs on that port.
#
# RESERVED PORTS. KubeMQ's own standard ports — gRPC 50000, REST 9090, API 8080, Kafka 9092,
# AMQP 5672 — plus 50001/50002 (an IDE-run second and third node) belong to the developer's OWN
# KubeMQ on a workstation (a container or an IDE run). A harness must never bind, dial or default
# to them: when that server is up, a wildcard bind beside it succeeds silently on macOS and every
# localhost client — including a "real Kafka" oracle leg — talks to the developer's server and
# can read "identical". When it happens to be down, a harness default that uses them is the same
# trap waiting for the next run. So they are refused UNCONDITIONALLY, whether or not anything
# listens there right now:
#   PF_RESERVED_PORTS        the deny-list (default "5672 8080 9090 9092 50000 50001 50002").
#   PF_ALLOW_RESERVED_PORTS  set to 1 to deliberately use a reserved port (a clean CI host, or a
#                            standard-port cluster on a machine with no developer KubeMQ). The
#                            ordinary free-port checks still apply.
#   refuse_reserved_ports <port>...  die if any <port> is reserved (preflight_free calls it).
#   port_of_addr <addr>              print the port of "host:port", "[v6]:port" or ":port"; die if
#                                    there is none.
#   refuse_reserved_addrs <addr>[,<addr>...]
#                                    die if any address in a comma-separated bootstrap list names a
#                                    reserved port — for the addresses a caller hands a harness to
#                                    DIAL (KAFKA_ADDR, KUBEMQ_ADDR), which no preflight covers.
#
# Plus these helpers built on the same primitives:
#
#   assert_sole_listener <port>...   AFTER a container publish (podman -p): die unless exactly one
#                                    visible process LISTENs on each <port> (the podman port
#                                    forwarder). There is no container pid to own the port.
#   assert_listeners_within <pid> <port>...
#                                    die if <pid> LISTENs on any TCP port NOT in the list — proves a
#                                    booted node opened nothing outside the port block it was given.
#   wait_owned_listener <pid> <port> <timeout-seconds>
#                                    poll until <pid> LISTENs on <port>; die if <pid> exits first or
#                                    the timeout passes.
#   port_answers <host> <port>       exit 0 if a TCP connect to <host>:<port> succeeds.
#
# Usage: source it, then call the functions.
#   source "<repo>/scripts/lib/port-preflight.sh"
#   preflight_free 31092 52000 10080
#   ... boot, PID=$! ...
#   assert_owned "$PID" 31092 52000 10080
#
# "die" means: print "[port-preflight] ERROR: ..." to stderr and `exit ${PF_DIE_CODE:-2}` — this
# file is sourced, so the exit ends the CALLING script (that is the point: nothing may continue on a
# port it does not own). A caller that maps infra failures to a different exit code sets PF_DIE_CODE
# before sourcing or before the call. PF_LOG_PREFIX (default "[port-preflight]") tags messages.
#
# Portability: bash 3.2 compatible (macOS /bin/bash) — no associative arrays, no mapfile. Needs
# lsof (macOS and most Linux distributions ship it); without lsof the ownership checks cannot be
# made and every function that needs it dies rather than passing blind.

# pf__log / pf__die: private helpers (double underscore = not part of the contract).
pf__log() { echo "${PF_LOG_PREFIX:-[port-preflight]} $*" >&2; }
pf__die() {
  echo "${PF_LOG_PREFIX:-[port-preflight]} ERROR: $*" >&2
  exit "${PF_DIE_CODE:-2}"
}

pf__require_lsof() {
  command -v lsof >/dev/null 2>&1 \
    || pf__die "lsof not found — port ownership cannot be proven, refusing to continue blind"
}

# pf__valid_port <port>: die unless <port> is an integer in 1..65535.
pf__valid_port() {
  case "$1" in
    ''|*[!0-9]*) pf__die "not a port number: '$1'" ;;
  esac
  if (( $1 < 1 || $1 > 65535 )); then
    pf__die "port out of range 1..65535: $1"
  fi
}

# port_answers <host> <port>: exit 0 when a TCP connection to <host>:<port> is accepted. Uses bash's
# /dev/tcp (no nc dependency). Loopback refusals return immediately, so no timeout is needed.
port_answers() {
  (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null
}

# pf__listen_pids <port>: print the pids (one per line, de-duplicated) that lsof shows in LISTEN on
# TCP <port>, on any address. Only processes visible to the current user are listed — the
# connect probe in preflight_free covers listeners owned by other users.
pf__listen_pids() {
  # lsof exits 1 when nothing matches; under a caller's `set -euo pipefail` that would abort the
  # caller from inside a $(...) — "no listener" is an answer here, not an error.
  { lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null || true; } | sort -u
}

# pf__describe_pid <pid>: "<pid> (<command>)" for error messages.
pf__describe_pid() {
  local cmd
  cmd="$( { ps -o comm= -p "$1" 2>/dev/null || true; } | head -n1)"
  echo "$1 (${cmd:-?})"
}

# The deny-list default (see file doc, RESERVED PORTS).
PF_RESERVED_PORTS_DEFAULT="5672 8080 9090 9092 50000 50001 50002"

# refuse_reserved_ports <port>...: die if any port is on the reserved list, unless
# PF_ALLOW_RESERVED_PORTS=1. Pure: never dials or inspects the port.
refuse_reserved_ports() {
  local port r reserved
  reserved="${PF_RESERVED_PORTS-$PF_RESERVED_PORTS_DEFAULT}"
  for port in "$@"; do
    pf__valid_port "$port"
    [[ "${PF_ALLOW_RESERVED_PORTS:-0}" == "1" ]] && continue
    for r in $reserved; do
      if [[ "$port" == "$r" ]]; then
        pf__die "port $port is RESERVED for the developer's own KubeMQ (reserved: ${reserved}). A harness never binds, dials or defaults to it — a localhost client could reach that server and pass. Move this port (CT_PORT_BASE=2000 for a cluster; the --*-port flags or KUBEMQ_*_PORT variables for one node), or set PF_ALLOW_RESERVED_PORTS=1 if you really mean it."
      fi
    done
  done
}

# port_of_addr <addr>: the port of "host:port", "[v6]:port" or ":port".
port_of_addr() {
  local a="$1" p
  p="${a##*:}"
  if [[ -z "$a" || "$p" == "$a" || -z "$p" ]]; then
    pf__die "not a host:port address: '$a'"
  fi
  pf__valid_port "$p"
  echo "$p"
}

# refuse_reserved_addrs <addr>[,<addr>...]: die if any address names a reserved port.
refuse_reserved_addrs() {
  local a p addrs=()
  # Split on commas with IFS scoped to the `read` alone (a function-wide IFS would leak — bash
  # scoping is dynamic — into refuse_reserved_ports, whose deny-list is space separated), and
  # without an unquoted expansion ("[::1]:9092" is also a glob pattern).
  IFS=',' read -r -a addrs <<<"$1"
  for a in ${addrs[@]+"${addrs[@]}"}; do
    a="${a// /}"
    [[ -n "$a" ]] || continue
    # port_of_addr dies inside the $(...) subshell; propagate that as this caller's exit.
    p="$(port_of_addr "$a")" || exit "${PF_DIE_CODE:-2}"
    refuse_reserved_ports "$p"
  done
}

# preflight_free <port>...: die if ANY of the ports is reserved or already in use (see file doc).
preflight_free() {
  pf__require_lsof
  local port pids p owners
  # Every port is checked against the deny-list BEFORE any of them is probed: a reserved port must
  # never be dialled, even to find out whether it is free.
  refuse_reserved_ports "$@"
  for port in "$@"; do
    pf__valid_port "$port"
    if port_answers 127.0.0.1 "$port"; then
      pids="$(pf__listen_pids "$port")"
      owners=""
      for p in $pids; do owners="${owners:+$owners, }$(pf__describe_pid "$p")"; done
      pf__die "port $port is already taken: something answers on 127.0.0.1:$port (listener: ${owners:-not visible to lsof}). Refusing to boot beside it — a localhost client would reach THAT process, not ours."
    fi
    if port_answers ::1 "$port"; then
      pf__die "port $port is already taken: something answers on [::1]:$port. Refusing to boot beside it."
    fi
    pids="$(pf__listen_pids "$port")"
    if [[ -n "$pids" ]]; then
      owners=""
      for p in $pids; do owners="${owners:+$owners, }$(pf__describe_pid "$p")"; done
      pf__die "port $port is already taken: LISTEN socket held by ${owners} (not on loopback, but a wildcard bind beside it would still be ambiguous). Refusing to boot."
    fi
  done
}

# assert_owned <pid> <port>...: die unless <pid> LISTENs on every <port> and is the ONLY visible
# process listening there.
assert_owned() {
  pf__require_lsof
  local pid="$1" port pids p others
  shift
  [[ -n "$pid" ]] || pf__die "assert_owned: empty pid"
  kill -0 "$pid" 2>/dev/null || pf__die "assert_owned: pid $pid is not running"
  for port in "$@"; do
    pf__valid_port "$port"
    if ! lsof -nP -a -p "$pid" -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      pids="$(pf__listen_pids "$port")"
      others=""
      for p in $pids; do others="${others:+$others, }$(pf__describe_pid "$p")"; done
      pf__die "port $port is NOT owned by pid $pid (listeners on $port: ${others:-none}). The process we booted does not hold the port our clients will dial."
    fi
    others=""
    for p in $(pf__listen_pids "$port"); do
      [[ "$p" == "$pid" ]] && continue
      others="${others:+$others, }$(pf__describe_pid "$p")"
    done
    if [[ -n "$others" ]]; then
      pf__die "port $port is held by pid $pid AND by ${others} — two listeners on one port; a localhost client may reach the other one. Refusing to continue."
    fi
  done
}

# assert_sole_listener <port>...: die unless exactly one visible process LISTENs on each <port>.
# Prints "<port> <pid> (<command>)" per port on stdout for the caller's log.
assert_sole_listener() {
  pf__require_lsof
  local port pids n
  for port in "$@"; do
    pf__valid_port "$port"
    pids="$(pf__listen_pids "$port")"
    n="$(printf '%s\n' "$pids" | grep -c '[0-9]' || true)"
    if [[ "$n" != "1" ]]; then
      pf__die "expected exactly one listener on port $port, found ${n}: ${pids:-none}"
    fi
    echo "$port $(pf__describe_pid "$pids")"
  done
}

# assert_listeners_within <pid> <port>...: die if <pid> LISTENs on a TCP port outside the list.
# Prints the pid's listening ports (sorted, space separated) on stdout for the caller's log.
assert_listeners_within() {
  pf__require_lsof
  local pid="$1" allowed port ok a stray="" seen=""
  shift
  [[ -n "$pid" ]] || pf__die "assert_listeners_within: empty pid"
  allowed=" $* "
  # lsof NAME column for a listener: "*:41000", "127.0.0.1:41800", "[::1]:41000".
  for port in $( { lsof -nP -a -p "$pid" -iTCP -sTCP:LISTEN -Fn 2>/dev/null || true; } \
                | sed -n 's/^n.*:\([0-9][0-9]*\)$/\1/p' | sort -un); do
    seen="${seen:+$seen }$port"
    ok=0
    for a in $allowed; do [[ "$a" == "$port" ]] && ok=1 && break; done
    [[ "$ok" == "1" ]] || stray="${stray:+$stray }$port"
  done
  if [[ -n "$stray" ]]; then
    pf__die "pid $pid listens on port(s) outside its allowed set [${*}]: ${stray} (all its listeners: ${seen})"
  fi
  echo "${seen}"
}

# pid_listens <pid> <port>: exit 0 when <pid> itself LISTENs on TCP <port> (no die — a predicate
# for callers that run their own wait loop, e.g. to print a log tail before failing).
pid_listens() {
  lsof -nP -a -p "$1" -iTCP:"$2" -sTCP:LISTEN >/dev/null 2>&1
}

# wait_owned_listener <pid> <port> <timeout-seconds>: poll (1s) until <pid> LISTENs on <port>.
wait_owned_listener() {
  pf__require_lsof
  local pid="$1" port="$2" timeout="${3:-60}" i
  pf__valid_port "$port"
  for ((i = 0; i < timeout; i++)); do
    kill -0 "$pid" 2>/dev/null || pf__die "pid $pid exited before it listened on port $port"
    if pid_listens "$pid" "$port"; then
      return 0
    fi
    sleep 1
  done
  pf__die "pid $pid did not listen on port $port within ${timeout}s"
}
