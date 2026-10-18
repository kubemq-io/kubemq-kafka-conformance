#!/usr/bin/env bash
#
# runner/certify.sh — one command to run the KubeMQ Kafka certification.
#
#   runner/certify.sh --quick              quick tier (about 15 minutes)
#   runner/certify.sh --full               every track (1 to 2 hours)
#   runner/certify.sh --track <name>       one track; repeatable
#   runner/certify.sh --list               list tracks and their tier
#   runner/certify.sh --preflight-only     run every check, start nothing
#
# Options:
#   --keep                leave the brokers running after the run (for debugging)
#   --include-opt-in      also run matrix rows marked opt_in (rows known to stop the broker
#                         under test; without it they are NOT-RUN with their reason), and with
#                         --full the opt-in tracks of runner/tracks.txt whose license is accepted
#   --publish             also write COMPATIBILITY.md and reports/<kubemq-version>/ in the repository
#                         (CI does this; a local run writes the report into its run directory only).
#                         Only with --full.
#   --run-dir <dir>       where logs and results go (default .work/run-<UTC timestamp>)
#   --kafka-version <rc>  preview against an Apache Kafka release candidate (for example
#                         4.4.0-rc4): the oracle and Kafka tools use that tag, resolved by digest
#                         at run time. A preview never certifies: with --publish it writes
#                         reports/preview/kafka-<rc>-kubemq-<version>/ and never COMPATIBILITY.md.
#   --down <run-id>       remove the brokers a --keep run left running, then exit
#
# Environment:
#   KUBEMQ_LICENSE_KEY    required. Never printed.
#   CONTAINER_ENGINE      docker | podman. Default: auto-detect.
#   CERT_BIND_ADDR        host address published ports bind to (default: loopback on macOS,
#                         all interfaces on Linux).
#   Ports: KAFKA_HOST_PORT, KAFKA_GATEWAY_PORT, KUBEMQ_KAFKA_PORT, KUBEMQ_API_PORT, all four or
#                         none. With none, a free block is chosen from the run id (CERT_RUN_ID).
#
# Every track is a directory suites/<name>/ with:
#   track.env   TRACK_TIER=quick|full, TRACK_CLIENTS=host|container, optional TRACK_POSTURES
#               (broker pairs rebooted per posture; see run_posture_track)
#   track.sh    runs the track; reads ORACLE_BOOTSTRAP, KUBEMQ_BOOTSTRAP, TRACK_RESULTS,
#               CERT_RUN_DIR; writes TRACK_RESULTS (results JSON). Its exit code is ignored: the
#               verdicts are in the JSON, and a missing JSON makes the track NOT-RUN.
# A track listed in runner/tracks.txt without a track.sh yet is recorded as NOT-RUN.
# A track with TRACK_CLIENTS=container gets bootstrap addresses on the host-gateway alias (KubeMQ
# is re-advertised there) and starts its client containers with runner/client-run.sh, never with
# an engine binary.
#
# Exit codes: 0 report passed; 1 report failed (any FAIL, NOT-RUN, or VOID over budget);
# 2 preflight or infrastructure failure (nothing was certified).
#
# Bash 3.2 compatible (macOS /bin/bash).
# shellcheck source-path=SCRIPTDIR
set -Eeuo pipefail
# Any unexpected failure is an infrastructure failure (exit 2), never a report verdict (0 or 1).
# Commands whose non-zero exit is expected run between allow_failure and expect_success.
# shellcheck disable=SC2317,SC2329 # invoked by the ERR trap
on_err() {
  local rc=$? line="$1"
  echo "[certify] ERROR: unexpected failure (exit ${rc}) at line ${line}: ${BASH_COMMAND}" >&2
  exit 2
}
trap 'on_err ${LINENO}' ERR
allow_failure() { set +e; trap - ERR; }
expect_success() { trap 'on_err ${LINENO}' ERR; set -e; }

CERT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CERT_ROOT

log() { echo "[certify] $*" >&2; }
die() { echo "[certify] ERROR: $*" >&2; exit 2; }

usage() { sed -n '3,45p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

# The exact command, recorded in every report.
CERT_COMMAND="runner/certify.sh"
for a in "$@"; do CERT_COMMAND="${CERT_COMMAND} $(printf '%q' "$a")"; done
MODE=""
KAFKA_PREVIEW=""
TRACKS=()
KEEP=0
PUBLISH=()
PREFLIGHT_ONLY=0
CERT_RUN_DIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --quick) MODE=quick ;;
    --full) MODE=full ;;
    --track) [[ $# -ge 2 ]] || die "--track needs a name"; MODE=track; TRACKS+=("$2"); shift ;;
    --list) MODE=list ;;
    --preflight-only) PREFLIGHT_ONLY=1 ;;
    --keep) KEEP=1 ;;
    --publish) PUBLISH=(--publish) ;;
    --include-opt-in) export CERT_INCLUDE_OPT_IN=1 ;;
    --kafka-version) [[ $# -ge 2 ]] || die "--kafka-version needs a release-candidate tag"; KAFKA_PREVIEW="$2"; shift ;;
    --run-dir) [[ $# -ge 2 ]] || die "--run-dir needs a path"; CERT_RUN_DIR="$2"; shift ;;
    --down) [[ $# -ge 2 ]] || die "--down needs a run id"; MODE=down; DOWN_ID="$2"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown argument '$1'" ;;
  esac
  shift
done
[[ -n "${MODE}" || "${PREFLIGHT_ONLY}" == 1 ]] || { usage >&2; die "choose --quick, --full, --track <name>, --list, --down <run-id> or --preflight-only"; }
if [[ ${#PUBLISH[@]} -gt 0 && "${MODE}" != full ]]; then
  die "--publish needs --full: a published report covers every track, and a partial run would leave tracks out"
fi

# --- track registry ------------------------------------------------------------------------
# runner/tracks.txt: one line per track, "<name> <tier>", in run order.
ALL_TRACKS=()
TRACK_TIERS=()
while read -r name tier _; do
  [[ -z "${name}" || "${name}" == \#* ]] && continue
  ALL_TRACKS+=("${name}"); TRACK_TIERS+=("${tier}")
done <"${CERT_ROOT}/runner/tracks.txt"

tier_of() {
  local i
  for i in "${!ALL_TRACKS[@]}"; do
    [[ "${ALL_TRACKS[$i]}" == "$1" ]] && { echo "${TRACK_TIERS[$i]}"; return 0; }
  done
  return 1
}

if [[ "${MODE}" == list ]]; then
  for i in "${!ALL_TRACKS[@]}"; do
    impl="not implemented"; [[ -x "${CERT_ROOT}/suites/${ALL_TRACKS[$i]}/track.sh" ]] && impl="implemented"
    printf '%-12s %-6s %s\n' "${ALL_TRACKS[$i]}" "${TRACK_TIERS[$i]}" "${impl}"
  done
  exit 0
fi

# opt_in_accepted <track>: exit 0 when the operator accepted the license the track's track.env
# names (TRACK_LICENSE_ACCEPT_VAR set to yes in the environment); else says why on stderr.
opt_in_accepted() {
  local var lic
  # shellcheck disable=SC1090,SC1091
  var="$(TRACK_LICENSE_ACCEPT_VAR=""; . "${CERT_ROOT}/suites/$1/track.env" 2>/dev/null; echo "${TRACK_LICENSE_ACCEPT_VAR}")"
  # shellcheck disable=SC1090,SC1091
  lic="$(TRACK_LICENSE=""; . "${CERT_ROOT}/suites/$1/track.env" 2>/dev/null; echo "${TRACK_LICENSE}")"
  if [[ -z "${var}" ]]; then
    log "opt-in track $1: its track.env names no TRACK_LICENSE_ACCEPT_VAR; not run"
    return 1
  fi
  if [[ "${!var:-}" != yes ]]; then
    log "opt-in track $1 uses software under the ${lic:-license named in its track.env}; to run it, read that license and export ${var}=yes"
    return 1
  fi
}

SELECTED=()
case "${MODE}" in
  quick) for i in "${!ALL_TRACKS[@]}"; do [[ "${TRACK_TIERS[$i]}" == quick ]] && SELECTED+=("${ALL_TRACKS[$i]}"); done ;;
  full)
    for i in "${!ALL_TRACKS[@]}"; do
      if [[ "${TRACK_TIERS[$i]}" == opt-in ]]; then
        [[ "${CERT_INCLUDE_OPT_IN:-}" == 1 ]] || { log "opt-in track ${ALL_TRACKS[$i]} not run (needs --include-opt-in)"; continue; }
        opt_in_accepted "${ALL_TRACKS[$i]}" || continue
      fi
      SELECTED+=("${ALL_TRACKS[$i]}")
    done ;;
  track)
    for t in "${TRACKS[@]}"; do
      tier_of "$t" >/dev/null || die "unknown track '$t' (see --list)"
      if [[ "$(tier_of "$t")" == opt-in ]]; then opt_in_accepted "$t" || die "opt-in track $t: license not accepted"; fi
      SELECTED+=("$t")
    done ;;
esac

[[ "${PREFLIGHT_ONLY}" == 1 || "${MODE}" == down || ${#SELECTED[@]} -gt 0 ]] || die "no tracks selected"

# --- versions and libraries ----------------------------------------------------------------
# Ports: give all four (KAFKA_HOST_PORT KAFKA_GATEWAY_PORT KUBEMQ_KAFKA_PORT KUBEMQ_API_PORT) or
# none; with none, the run takes a free block chosen from its run id (pick_port_block).
PORTS_GIVEN=0
for v in KAFKA_HOST_PORT KAFKA_GATEWAY_PORT KUBEMQ_KAFKA_PORT KUBEMQ_API_PORT; do
  [[ -n "${!v:-}" ]] && PORTS_GIVEN=$((PORTS_GIVEN + 1))
done
[[ "${PORTS_GIVEN}" == 0 || "${PORTS_GIVEN}" == 4 ]] \
  || die "set all four of KAFKA_HOST_PORT KAFKA_GATEWAY_PORT KUBEMQ_KAFKA_PORT KUBEMQ_API_PORT, or none (the run then picks a free block)"
set -a
# shellcheck source=../compose/versions.env
. "${CERT_ROOT}/compose/versions.env"
set +a

# Kafka release-candidate preview: the oracle image is the candidate, resolved by digest now.
if [[ -n "${KAFKA_PREVIEW}" ]]; then
  [[ "${KAFKA_PREVIEW}" =~ ^[0-9]+\.[0-9]+\.[0-9]+-rc[0-9]+$ ]] \
    || die "--kafka-version takes an Apache Kafka release candidate such as 4.4.0-rc4, not '${KAFKA_PREVIEW}'; released Kafka versions are pinned in compose/versions.env"
  # shellcheck source=lib/registry.sh
  . "${CERT_ROOT}/runner/lib/registry.sh"
  REGISTRY_LOG_PREFIX="[certify]" registry_resolve "${KAFKA_IMAGE_REPO}" "${KAFKA_PREVIEW}"
  KAFKA_IMAGE_DIGEST="${REG_INDEX_DIGEST}" KAFKA_IMAGE_DIGEST_AMD64="${REG_AMD64_DIGEST}" KAFKA_IMAGE_DIGEST_ARM64="${REG_ARM64_DIGEST}"
  KAFKA_VERSION="${KAFKA_PREVIEW%-rc*}"
  export KAFKA_IMAGE_DIGEST KAFKA_IMAGE_DIGEST_AMD64 KAFKA_IMAGE_DIGEST_ARM64 KAFKA_VERSION KAFKA_PREVIEW
  log "PREVIEW against Apache Kafka ${KAFKA_PREVIEW} (${KAFKA_IMAGE_DIGEST}); this run certifies nothing"
fi

# KUBEMQ_VERSION=latest (the public kit): run whatever the registry serves as latest, resolved
# to digests now, so run.json and the report record exactly which image ran.
if [[ "${KUBEMQ_VERSION}" == latest && "${MODE}" != down ]]; then
  # shellcheck source=lib/registry.sh
  . "${CERT_ROOT}/runner/lib/registry.sh"
  REGISTRY_LOG_PREFIX="[certify]" registry_resolve "${KUBEMQ_IMAGE_REPO}" latest
  KUBEMQ_IMAGE_DIGEST="${REG_INDEX_DIGEST}" KUBEMQ_IMAGE_DIGEST_AMD64="${REG_AMD64_DIGEST}" KUBEMQ_IMAGE_DIGEST_ARM64="${REG_ARM64_DIGEST}"
  export KUBEMQ_IMAGE_DIGEST KUBEMQ_IMAGE_DIGEST_AMD64 KUBEMQ_IMAGE_DIGEST_ARM64
  log "KubeMQ latest resolved to ${KUBEMQ_IMAGE_DIGEST}"
fi

# --- run id ----------------------------------------------------------------------------------
# Every container and compose project name carries the run id, so runs never collide. Set
# before the libraries: brokers.sh names the containers when it is sourced.
if [[ "${MODE}" == down ]]; then
  CERT_RUN_ID="${DOWN_ID}"
else
  CERT_RUN_ID="${CERT_RUN_ID:-r$(date -u +%m%d%H%M%S)$$}"
fi
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

if [[ "${MODE}" == down ]]; then
  engine_detect
  CERT_RUN_DIR="$(mktemp -d)"; mkdir -p "${CERT_RUN_DIR}/logs"; export CERT_RUN_DIR
  brokers_down
  rm -rf "${CERT_RUN_DIR}"
  log "removed the brokers of run ${CERT_RUN_ID}"
  exit 0
fi

# --- preflight -----------------------------------------------------------------------------
# The protocol track checks the committed ApiKey matrix against the live brokers; a matrix made for
# another KubeMQ version can only fail that row. Stop before anything starts instead.
for t in ${SELECTED[@]+"${SELECTED[@]}"}; do
  [[ "${t}" == protocol ]] || continue
  # With latest, the protocol track's matrix row compares the committed matrix to the live image.
  [[ "${KUBEMQ_VERSION}" != latest ]] || continue
  matrix_kubemq="$(sed -n 's/^kubemq: kubemq-next //p' "${CERT_ROOT}/matrix/apikeys.yaml")"
  [[ "${matrix_kubemq}" == "${KUBEMQ_VERSION}" ]] \
    || die "matrix/apikeys.yaml names KubeMQ ${matrix_kubemq:-none}, compose/versions.env pins ${KUBEMQ_VERSION}: regenerate it with runner/regen-apikeys.sh (runner/bump-kubemq.sh does this on a bump)"
done

preflight_license
preflight_tools
preflight_go "$(sed -n 's/^go \([0-9.]*\).*/\1/p' "${CERT_ROOT}/suites/protocol/go.mod")"
engine_detect
log "$(engine_describe)"
preflight_arch
preflight_clock
preflight_tcp_load
[[ "${PORTS_GIVEN}" == 4 ]] || pick_port_block "${CERT_RUN_ID}"
# A selected posture track's pairs use fixed offsets from the kit ports; check those up front too.
POSTURE_PORTS=()
for t in ${SELECTED[@]+"${SELECTED[@]}"}; do
  if grep -q '^TRACK_POSTURES=' "${CERT_ROOT}/suites/${t}/track.env" 2>/dev/null; then posture_env "${t}"; break; fi
done
preflight_ports "${KAFKA_HOST_PORT}" "${KAFKA_GATEWAY_PORT}" "${KUBEMQ_KAFKA_PORT}" "${KUBEMQ_API_PORT}" ${POSTURE_PORTS[@]+"${POSTURE_PORTS[@]}"}

if [[ "${PREFLIGHT_ONLY}" == 1 ]]; then
  log "preflight passed; --preflight-only, nothing started"
  exit 0
fi

# --- run directory -------------------------------------------------------------------------
if [[ -z "${CERT_RUN_DIR}" ]]; then
  CERT_RUN_DIR="${CERT_ROOT}/.work/run-$(date -u +%Y%m%dT%H%M%SZ)"
fi
mkdir -p "${CERT_RUN_DIR}/logs" "${CERT_RUN_DIR}/results"
CERT_RUN_DIR="$(cd "${CERT_RUN_DIR}" && pwd)"
export CERT_RUN_DIR
log "run directory: ${CERT_RUN_DIR} (not committed); run id ${CERT_RUN_ID}"

# shellcheck disable=SC2317,SC2329 # invoked by the EXIT trap (older shellcheck reports SC2317)
cleanup() {
  local rc=$?
  brokers_save_logs || true
  if [[ "${KEEP}" == 1 ]]; then
    log "--keep: brokers left running (oracle 127.0.0.1:${KAFKA_HOST_PORT}, KubeMQ 127.0.0.1:${KUBEMQ_KAFKA_PORT}); remove them with: runner/certify.sh --down ${CERT_RUN_ID}"
  else
    brokers_down
  fi
  exit "${rc}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# --- run metadata (goes into the report header) --------------------------------------------
# Operator, machine and run metadata for the report header. An operator-run report must be as
# auditable as a CI-run one: who ran it, where, with what, when, and the exact command.
operator_identity() {
  if [[ -n "${CERT_OPERATOR:-}" ]]; then echo "${CERT_OPERATOR}"; return; fi
  git -C "${CERT_ROOT}" config user.name 2>/dev/null || id -un
}

machine_description() {
  if [[ -n "${CERT_MACHINE:-}" ]]; then echo "${CERT_MACHINE}"; return; fi
  local os cpu mem
  case "$(uname -s)" in
    Darwin)
      os="macOS $(sw_vers -productVersion 2>/dev/null)"
      cpu="$(sysctl -n machdep.cpu.brand_string 2>/dev/null), $(sysctl -n hw.ncpu 2>/dev/null) cores"
      mem="$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 )) GiB"
      ;;
    *)
      # shellcheck disable=SC1091 # read at run time on Linux hosts
      os="$( (. /etc/os-release 2>/dev/null && echo "${PRETTY_NAME}") || uname -sr)"
      cpu="$(awk -F': ' '/model name/{print $2; exit}' /proc/cpuinfo 2>/dev/null), $(nproc 2>/dev/null) cores"
      mem="$(awk '/MemTotal/{printf "%d GiB", $2/1048576}' /proc/meminfo 2>/dev/null)"
      ;;
  esac
  echo "${os}, $(uname -m), ${cpu}, ${mem}"
}

write_run_json() {
  local tracks commit dirty=false
  tracks="$(printf '%s\n' "${SELECTED[@]}" | jq -R . | jq -s .)"
  commit="$(git -C "${CERT_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
  [[ -n "$(git -C "${CERT_ROOT}" status --porcelain 2>/dev/null)" ]] && dirty=true
  jq -n \
    --arg started "${RUN_STARTED}" --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg mode "${MODE}" --argjson tracks "${tracks}" \
    --arg command "${CERT_COMMAND}" --arg operator "$(operator_identity)" --arg machine "$(machine_description)" \
    --arg commit "${commit}" --argjson dirty "${dirty}" \
    --arg kv "${KAFKA_VERSION}" --arg ki "${KAFKA_IMAGE_REPO}" --arg kd "${KAFKA_IMAGE_DIGEST}" \
    --arg qv "${KUBEMQ_VERSION}" --arg qi "${KUBEMQ_IMAGE_REPO}" --arg qd "${KUBEMQ_IMAGE_DIGEST}" \
    --arg en "${CERT_ENGINE}" --arg er "${CERT_ENGINE_RUNTIME}" --arg ea "${CERT_ENGINE_ARCH}" \
    --arg ev "$(engine_version)" --arg hos "$(uname -s)" \
    --arg logs "${RAW_LOGS_FILE:-}" --arg logsd "${RAW_LOGS_DIGEST:-}" --arg kp "${KAFKA_PREVIEW}" \
    '{started_utc: $started, finished_utc: $finished, mode: $mode, tracks: $tracks,
      command: $command, operator: $operator, machine: $machine,
      repo: {commit: $commit, dirty: $dirty},
      kafka: {version: $kv, image: $ki, digest: $kd}, kafka_preview: $kp,
      kubemq: {version: $qv, image: $qi, digest: $qd},
      engine: {name: $en, runtime: $er, arch: $ea, version: $ev, host_os: $hos},
      raw_logs: {file: $logs, sha256: $logsd}}' >"${CERT_RUN_DIR}/run.json"
}

# package_raw_logs: broker logs, track logs, results and transcripts in one tarball inside the
# run directory (never in git), with its digest recorded in run.json and the report. The
# operator publishes it as a GitHub Release asset or in GCS (docs/release-cert.md).
package_raw_logs() {
  brokers_save_logs || true
  RAW_LOGS_FILE="raw-logs-${KUBEMQ_VERSION}${KAFKA_PREVIEW:+-kafka-${KAFKA_PREVIEW}}-$(date -u +%Y%m%dT%H%M%SZ).tar.gz"
  local parts=(logs results)
  [[ -d "${CERT_RUN_DIR}/transcripts" ]] && parts+=(transcripts)
  (cd "${CERT_RUN_DIR}" && tar -czf "${RAW_LOGS_FILE}" "${parts[@]}")
  if command -v shasum >/dev/null 2>&1; then
    RAW_LOGS_DIGEST="sha256:$(shasum -a 256 "${CERT_RUN_DIR}/${RAW_LOGS_FILE}" | awk '{print $1}')"
  else
    RAW_LOGS_DIGEST="sha256:$(sha256sum "${CERT_RUN_DIR}/${RAW_LOGS_FILE}" | awk '{print $1}')"
  fi
  log "raw logs: ${CERT_RUN_DIR}/${RAW_LOGS_FILE} (${RAW_LOGS_DIGEST})"
}
RUN_STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
export CERT_RUN_STARTED="${RUN_STARTED}"

# --- brokers -------------------------------------------------------------------------------
# The plain pair starts when the first ordinary track needs it, and again after a posture track.
ORACLE_RUNNING=0
KUBEMQ_ADVERTISED_NOW=""

ensure_kubemq_for() {
  local want=127.0.0.1
  [[ "${ORACLE_RUNNING}" == 1 ]] || { oracle_up; ORACLE_RUNNING=1; }
  [[ "$1" == container ]] && want="${HOST_GATEWAY_ALIAS}"
  if [[ "${KUBEMQ_ADVERTISED_NOW}" != "${want}" ]]; then
    kubemq_up "${want}"
    KUBEMQ_ADVERTISED_NOW="${want}"
  fi
}

not_run_json() {
  # not_run_json <track> <reason> <file>
  printf '{"track":"%s","not_run_reason":"%s","rows":[]}\n' "$1" "$2" >"$3"
}

# track_step <track> <dir> <step> [posture]: one step of a posture track, without the key.
track_step() {
  allow_failure
  TRACK_RESULTS="${CERT_RUN_DIR}/results/$1.json" KUBEMQ_LICENSE_KEY="" \
    "$2/track.sh" "$3" ${4:+"$4"} 2>&1 | redact_secrets >>"${CERT_RUN_DIR}/logs/track-$1.log"
  local rc=${PIPESTATUS[0]}
  expect_success
  return "${rc}"
}

# run_posture_track <track> <dir>: TRACK_POSTURES in track.env. Per posture: "track.sh prepare
# <posture>" writes POSTURE_DIR, the runner boots the track's own pair (key only into KubeMQ),
# "track.sh run <posture>" runs its rows, the pair is removed. Then "track.sh finish" writes the
# results. A posture whose prepare step fails or whose pair does not boot is recorded in
# POSTURE_DIR/prepare-failed or boot-failed for finish.
run_posture_track() {
  local p rc
  brokers_down; ORACLE_RUNNING=0; KUBEMQ_ADVERTISED_NOW=""
  posture_env "$1"
  for p in ${TRACK_POSTURES}; do
    export POSTURE="${p}" POSTURE_DIR="${CERT_RUN_DIR}/postures/$1/${p}"
    mkdir -p "${POSTURE_DIR}"
    if ! track_step "$1" "$2" prepare "${p}"; then
      log "track $1: prepare ${p} failed"
      echo "track.sh prepare ${p} failed; see logs/track-$1.log" >"${POSTURE_DIR}/prepare-failed"
      continue
    fi
    allow_failure
    (BROKERS_DIE_CODE=3 posture_up "$2" "$1-${p}") 2>&1 | redact_secrets | tee "${POSTURE_DIR}/boot.log" >&2
    rc=${PIPESTATUS[0]}
    expect_success
    # Exit 2 is the engine itself not answering (runner/lib/engine.sh): infrastructure, not a
    # broker that failed to boot, so the run stops instead of recording the posture's rows.
    [[ "${rc}" == 2 ]] && die "track $1 posture ${p}: the container engine did not answer; nothing was certified"
    if [[ "${rc}" == 0 ]]; then
      track_step "$1" "$2" run "${p}" || log "track $1: run ${p} exited non-zero"
    else
      cp "${POSTURE_DIR}/boot.log" "${POSTURE_DIR}/boot-failed"
    fi
    posture_down "$2" "$1-${p}"
  done
  track_step "$1" "$2" finish || log "track $1: finish exited non-zero"
}

# --- tracks --------------------------------------------------------------------------------
for t in "${SELECTED[@]}"; do
  dir="${CERT_ROOT}/suites/${t}"
  out="${CERT_RUN_DIR}/results/${t}.json"
  if [[ ! -x "${dir}/track.sh" ]]; then
    log "track ${t}: not implemented yet -> NOT-RUN"
    not_run_json "${t}" "track not implemented" "${out}"
    continue
  fi
  TRACK_CLIENTS=host
  TRACK_POSTURES=""
  # shellcheck disable=SC1090,SC1091
  [[ -f "${dir}/track.env" ]] && . "${dir}/track.env"
  if [[ -n "${TRACK_POSTURES}" ]]; then
    log "track ${t}: postures ${TRACK_POSTURES}"
    t0=$(date +%s)
    run_posture_track "${t}" "${dir}"
    log "track ${t}: finished in $(( $(date +%s) - t0 ))s; log ${CERT_RUN_DIR}/logs/track-${t}.log"
    [[ -s "${out}" ]] || { log "track ${t}: produced no results -> NOT-RUN"; not_run_json "${t}" "track produced no results" "${out}"; }
    continue
  fi
  ensure_kubemq_for "${TRACK_CLIENTS}"
  if [[ "${TRACK_CLIENTS}" == container ]]; then
    ORACLE_BOOTSTRAP="${HOST_GATEWAY_ALIAS}:${KAFKA_GATEWAY_PORT}"
    KUBEMQ_BOOTSTRAP="${HOST_GATEWAY_ALIAS}:${KUBEMQ_KAFKA_PORT}"
  else
    ORACLE_BOOTSTRAP="127.0.0.1:${KAFKA_HOST_PORT}"
    KUBEMQ_BOOTSTRAP="127.0.0.1:${KUBEMQ_KAFKA_PORT}"
  fi
  log "track ${t}: oracle ${ORACLE_BOOTSTRAP}, KubeMQ ${KUBEMQ_BOOTSTRAP}"
  t0=$(date +%s)
  allow_failure
  ORACLE_BOOTSTRAP="${ORACLE_BOOTSTRAP}" KUBEMQ_BOOTSTRAP="${KUBEMQ_BOOTSTRAP}" \
    TRACK_RESULTS="${out}" CERT_RUN_DIR="${CERT_RUN_DIR}" \
    KUBEMQ_LICENSE_KEY="" \
    "${dir}/track.sh" 2>&1 | redact_secrets >"${CERT_RUN_DIR}/logs/track-${t}.log"
  trc=${PIPESTATUS[0]}
  # A KubeMQ that stopped during this track is removed now, and the next track boots a fresh one.
  if kubemq_reap_if_stopped; then
    KUBEMQ_ADVERTISED_NOW=""
  fi
  expect_success
  log "track ${t}: finished in $(( $(date +%s) - t0 ))s (exit ${trc}); log ${CERT_RUN_DIR}/logs/track-${t}.log"
  if [[ ! -s "${out}" ]]; then
    log "track ${t}: produced no results -> NOT-RUN"
    not_run_json "${t}" "track produced no results (exit ${trc})" "${out}"
  fi
done

package_raw_logs
write_run_json

# --- report --------------------------------------------------------------------------------
RENDER="${CERT_ROOT}/runner/report/render.sh"
if [[ -x "${RENDER}" ]]; then
  allow_failure
  KUBEMQ_LICENSE_KEY="" "${RENDER}" --run-dir "${CERT_RUN_DIR}" ${PUBLISH[@]+"${PUBLISH[@]}"}
  rrc=$?
  expect_success
  exit "${rrc}"
fi
log "report renderer not present yet; results are in ${CERT_RUN_DIR}/results"
exit 1
