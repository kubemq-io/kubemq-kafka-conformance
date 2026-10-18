# shellcheck shell=bash
#
# runner/lib/brokers.sh — start, wait for, prove the identity of, and stop the two brokers.
# Requires engine.sh (engine_detect called) and compose/versions.env loaded into the
# environment. All logs go under CERT_RUN_DIR, never into git.

ORACLE_COMPOSE="${CERT_ROOT}/compose/kafka-oracle.yaml"
KUBEMQ_COMPOSE="${CERT_ROOT}/compose/kubemq-single.yaml"
# Names carry the run id, so two runs on one machine never touch each other's containers.
ORACLE_CONTAINER="kcert-kafka-oracle-${CERT_RUN_ID:?CERT_RUN_ID must be set}"
KUBEMQ_CONTAINER="kcert-kubemq-${CERT_RUN_ID}"
# Exported for tracks whose clients join a broker's network namespace (runner/client-run.sh).
export ORACLE_CONTAINER KUBEMQ_CONTAINER
# One license folder per machine, shared by every run (see compose/kubemq-single.yaml).
export KUBEMQ_LICENSE_VOLUME="${KUBEMQ_LICENSE_VOLUME:-kcert-kubemq-license}"
# KubeMQ's image runs as this user; the license folder must be writable by it.
KUBEMQ_UID=1001

br__die() { echo "[brokers] ERROR: $*" >&2; exit "${BROKERS_DIE_CODE:-2}"; }
br__log() { echo "[brokers] $*" >&2; }

# br__assert_digest <container> <index-digest> <arch-digest>: the running image is the pinned one.
br__assert_digest() {
  local c="$1" want_index="$2" want_arch="$3" got
  engine_image_digests "$c" || br__die "${c} disappeared before its image digest could be checked"
  got="$(echo "${ENGINE_IMAGE_DIGESTS}" | tr '\n' ' ')"
  case " ${got} " in
    *" ${want_index} "*|*" ${want_arch} "*) br__log "${c}: image digest verified (${want_index})" ;;
    *) br__die "${c} runs an image whose digests [${got}] match neither the pinned index ${want_index} nor ${CERT_ENGINE_ARCH} manifest ${want_arch}" ;;
  esac
}

# oracle_up: start the oracle. If it exits during startup because a listener port was still
# held (the previous run's sockets not yet released inside the engine's network layer), tear
# it down, wait, and start once more. This is a broker-boot retry, never a row retry.
oracle_up() {
  if oracle_boot; then return 0; fi
  if grep -q "Address in use" "${CERT_RUN_DIR}/logs/oracle.log" 2>/dev/null; then
    br__log "oracle listener port still held from a previous run; retrying the boot once in 10s"
    mv "${CERT_RUN_DIR}/logs/oracle.log" "${CERT_RUN_DIR}/logs/oracle-boot-1.log"
    engine_compose "${ORACLE_COMPOSE}" down -v --remove-orphans >/dev/null 2>&1 || true
    sleep 10
    oracle_boot && return 0
  fi
  br__die "Kafka oracle container exited during startup; see ${CERT_RUN_DIR}/logs/oracle.log"
}

oracle_boot() {
  br__log "starting Kafka oracle ${KAFKA_VERSION} (${KAFKA_IMAGE_DIGEST})"
  engine_compose "${ORACLE_COMPOSE}" up -d --quiet-pull >"${CERT_RUN_DIR}/logs/oracle-compose.log" 2>&1 \
    || { cat "${CERT_RUN_DIR}/logs/oracle-compose.log" >&2; br__die "Kafka oracle failed to start"; }
  br__wait_oracle "${ORACLE_CONTAINER}" "${KAFKA_HOST_PORT}" oracle || return 1
  br__log "Kafka oracle ready: host 127.0.0.1:${KAFKA_HOST_PORT}, containers ${HOST_GATEWAY_ALIAS}:${KAFKA_GATEWAY_PORT}"
}

# br__wait_oracle <container> <plaintext-port> <log-name>: wait until the oracle answers ApiVersions
# inside its container, then prove its version and image digest. Returns 1 if it exited.
br__wait_oracle() {
  local c="$1" port="$2" i ready=0 v
  for i in $(seq 1 "${ORACLE_READY_TIMEOUT:-90}"); do
    engine_container_running "${c}" || { engine_logs "${c}" "${CERT_RUN_DIR}/logs/$3.log"; return 1; }
    if engine_run exec "${c}" /opt/kafka/bin/kafka-broker-api-versions.sh \
         --bootstrap-server "localhost:${port}" >/dev/null 2>&1; then
      ready=1; break
    fi
    sleep 1
  done
  [[ "${ready}" == 1 ]] || br__die "Kafka oracle ${c} not ready after ${ORACLE_READY_TIMEOUT:-90}s"
  v="$(engine_run exec "${c}" /opt/kafka/bin/kafka-topics.sh --version 2>/dev/null | awk '{print $1}')"
  [[ "${v}" == "${KAFKA_VERSION}" ]] || br__die "Kafka oracle ${c} reports version '${v}', pinned ${KAFKA_VERSION}"
  br__assert_digest "${c}" "${KAFKA_IMAGE_DIGEST}" "$(br__arch_digest KAFKA_IMAGE_DIGEST)"
  br__log "${c} ready after ${i}s: Kafka ${v}"
}

# kubemq_license_volume: create the per-machine license volume once, owned by the KubeMQ user
# and private to it. Uses the pinned oracle image as the helper (KubeMQ's image has no shell).
kubemq_license_volume() {
  local owner
  # Two first-ever runs on a machine can both find no volume; the second create then fails
  # because the first just made it, so look again before giving up.
  engine_inspect volume inspect --format '{{.Name}}' "${KUBEMQ_LICENSE_VOLUME}" \
    || engine_run volume create "${KUBEMQ_LICENSE_VOLUME}" >/dev/null 2>&1 \
    || engine_inspect volume inspect --format '{{.Name}}' "${KUBEMQ_LICENSE_VOLUME}" \
    || br__die "cannot create the license volume ${KUBEMQ_LICENSE_VOLUME}"
  owner="$(engine_run run --rm --user 0 --entrypoint sh -v "${KUBEMQ_LICENSE_VOLUME}:/license" \
             "${KAFKA_IMAGE_REPO}@${KAFKA_IMAGE_DIGEST}" -c \
             "chown ${KUBEMQ_UID}:${KUBEMQ_UID} /license && chmod 700 /license && stat -c '%u %a' /license" 2>&1)" \
    || br__die "cannot prepare the license volume ${KUBEMQ_LICENSE_VOLUME}: ${owner}"
  [[ "${owner}" == "${KUBEMQ_UID} 700" ]] || br__die "license volume ${KUBEMQ_LICENSE_VOLUME} is '${owner}', want '${KUBEMQ_UID} 700'"
}

# kubemq_license_source <log>: how this boot got its license, for the run log. "activated" means
# it called the license server; "reused" means it started from the machine's cached lease.
kubemq_license_source() {
  if grep -q "first boot: activate\|\"via\": *\"activate\"\|via=activate" "$1" 2>/dev/null; then
    echo activated
  else
    echo reused
  fi
}

# kubemq_up [advertised-host]: default localhost (clients on the host). A track whose clients
# run in containers passes the gateway alias; the node is recreated when that changes.
kubemq_up() {
  export KUBEMQ_ADVERTISED_HOST="${1:-127.0.0.1}"
  br__log "starting KubeMQ ${KUBEMQ_VERSION} (${KUBEMQ_IMAGE_DIGEST}), Kafka advertised as ${KUBEMQ_ADVERTISED_HOST}:${KUBEMQ_KAFKA_PORT}"
  kubemq_license_volume
  engine_compose "${KUBEMQ_COMPOSE}" up -d --quiet-pull --force-recreate 2>&1 | redact_secrets >"${CERT_RUN_DIR}/logs/kubemq-compose.log"
  engine_container_running "${KUBEMQ_CONTAINER}" \
    || { cat "${CERT_RUN_DIR}/logs/kubemq-compose.log" >&2; br__die "KubeMQ failed to start"; }
  br__wait_kubemq "${KUBEMQ_CONTAINER}" "${KUBEMQ_API_PORT}" "${KUBEMQ_KAFKA_PORT}" kubemq
  br__log "KubeMQ Kafka listener advertised as ${KUBEMQ_ADVERTISED_HOST}:${KUBEMQ_KAFKA_PORT}"
}

# br__wait_kubemq <container> <api-port> <kafka-port> <log-name>: wait for /ready and the Kafka
# listener, then prove the image digest.
br__wait_kubemq() {
  local c="$1" log="${CERT_RUN_DIR}/logs/$4.log" i ready=0
  for i in $(seq 1 "${KUBEMQ_READY_TIMEOUT:-90}"); do
    engine_container_running "${c}" || { engine_logs "${c}" "${log}"; br__die "KubeMQ container ${c} exited during startup; see ${log}"; }
    if curl -sf "http://127.0.0.1:$2/ready" >/dev/null 2>&1 && port_answers 127.0.0.1 "$3"; then
      ready=1; break
    fi
    sleep 1
  done
  if [[ "${ready}" != 1 ]]; then
    engine_logs "${c}" "${log}"
    if grep -qi "license" "${log}" && ! grep -q "license verified" "${log}"; then
      br__die "KubeMQ did not open its Kafka listener and did not verify its license. Check KUBEMQ_LICENSE_KEY; see ${log}"
    fi
    br__die "KubeMQ ${c} not ready after ${KUBEMQ_READY_TIMEOUT:-90}s; see ${log}"
  fi
  br__assert_digest "${c}" "${KUBEMQ_IMAGE_DIGEST}" "$(br__arch_digest KUBEMQ_IMAGE_DIGEST)"
  engine_logs "${c}" "${CERT_RUN_DIR}/logs/$4-boot.log"
  br__log "${c} ready after ${i}s, license $(kubemq_license_source "${CERT_RUN_DIR}/logs/$4-boot.log") (folder ${KUBEMQ_LICENSE_VOLUME})"
}

br__arch_digest() {
  local per
  per="$1_$(echo "${CERT_ENGINE_ARCH}" | tr '[:lower:]' '[:upper:]')"
  echo "${!per}"
}

# kubemq_reap_if_stopped: if the KubeMQ container stopped (it exited during a track), save its
# log and remove it with its compose network at once, before anything else publishes a port.
# A stopped container left in place can leave the engine's port forwarding in a bad state.
# Returns 0 when it reaped, 1 when KubeMQ is still running or was never started.
kubemq_reap_if_stopped() {
  engine_container_exists "${KUBEMQ_CONTAINER}" || return 1
  engine_container_running "${KUBEMQ_CONTAINER}" && return 1
  engine_logs "${KUBEMQ_CONTAINER}" "${CERT_RUN_DIR}/logs/kubemq-stopped-$(date -u +%H%M%S).log"
  KUBEMQ_LICENSE_KEY="${KUBEMQ_LICENSE_KEY:-unused-for-teardown}" \
    engine_compose "${KUBEMQ_COMPOSE}" down -v --remove-orphans >/dev/null 2>&1 || true
  br__log "KubeMQ had stopped; its container and network were removed (log saved)"
  return 0
}

# brokers_save_logs: copy both broker logs into the run directory.
brokers_save_logs() {
  engine_logs "${ORACLE_CONTAINER}" "${CERT_RUN_DIR}/logs/oracle.log"
  engine_logs "${KUBEMQ_CONTAINER}" "${CERT_RUN_DIR}/logs/kubemq.log"
}

brokers_down() {
  # `down` interpolates the whole file, so the key guard and the ports need a value; nothing is
  # started. `certify.sh --down <run id>` does not know the kept run's ports, and needs none:
  # compose finds the containers by project name.
  local ports_known=1
  [[ -n "${KAFKA_HOST_PORT:-}" ]] || ports_known=0
  KUBEMQ_LICENSE_KEY="${KUBEMQ_LICENSE_KEY:-unused-for-teardown}" KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-1}" \
    KAFKA_GATEWAY_PORT="${KAFKA_GATEWAY_PORT:-2}" KUBEMQ_KAFKA_PORT="${KUBEMQ_KAFKA_PORT:-3}" KUBEMQ_API_PORT="${KUBEMQ_API_PORT:-4}" \
    engine_compose "${KUBEMQ_COMPOSE}" down -v --remove-orphans >/dev/null 2>&1 || true
  KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-1}" KAFKA_GATEWAY_PORT="${KAFKA_GATEWAY_PORT:-2}" \
    engine_compose "${ORACLE_COMPOSE}" down -v --remove-orphans >/dev/null 2>&1 || true
  # Leave the ports free for the next run before returning.
  [[ "${ports_known}" == 1 ]] || return 0
  wait_ports_free 30 "${KAFKA_HOST_PORT}" "${KAFKA_GATEWAY_PORT}" "${KUBEMQ_KAFKA_PORT}" "${KUBEMQ_API_PORT}" || true
}

# --- postures: a track's own broker pair, rebooted per posture (TRACK_POSTURES in track.env) ----
# For config that both brokers read only at boot (TLS, SASL users, authorization). The track's
# suites/<track>/compose-oracle.yaml and compose-kubemq.yaml use the POSTURE_* variables; the
# oracle must offer a plaintext listener on POSTURE_ORACLE_ADMIN_PORT, and KubeMQ reads its config
# from POSTURE_KUBEMQ_CONFIG, the base64 of POSTURE_DIR/kubemq.config ("track.sh prepare" writes it).
# Ports are fixed offsets from the kit's ports, so a lane's port block covers them.
posture_env() {
  export POSTURE_ORACLE_CONTAINER="kcert-$1-oracle-${CERT_RUN_ID}" POSTURE_KUBEMQ_CONTAINER="kcert-$1-kubemq-${CERT_RUN_ID}" \
    POSTURE_ORACLE_PORT=$((KAFKA_HOST_PORT + 1)) POSTURE_ORACLE_TLS_PORT=$((KAFKA_HOST_PORT + 2)) \
    POSTURE_ORACLE_ADMIN_PORT=$((KAFKA_HOST_PORT + 3)) POSTURE_KUBEMQ_PORT=$((KUBEMQ_KAFKA_PORT + 1)) \
    POSTURE_KUBEMQ_TLS_PORT=$((KUBEMQ_KAFKA_PORT + 2)) POSTURE_KUBEMQ_API_PORT=$((KUBEMQ_API_PORT + 1))
  POSTURE_PORTS=("${POSTURE_ORACLE_PORT}" "${POSTURE_ORACLE_TLS_PORT}" "${POSTURE_ORACLE_ADMIN_PORT}" \
    "${POSTURE_KUBEMQ_PORT}" "${POSTURE_KUBEMQ_TLS_PORT}" "${POSTURE_KUBEMQ_API_PORT}")
}

# posture_up <track-dir> <log-name>: boot the pair for POSTURE. Dies (BROKERS_DIE_CODE) on failure.
posture_up() {
  wait_ports_free 30 "${POSTURE_PORTS[@]}" || br__die "posture ports still held: ${POSTURE_PORTS[*]}"
  [[ -s "${POSTURE_DIR}/kubemq.config" ]] || br__die "track.sh prepare wrote no ${POSTURE_DIR}/kubemq.config"
  POSTURE_KUBEMQ_CONFIG="$(base64 <"${POSTURE_DIR}/kubemq.config" | tr -d '\n')"
  export POSTURE_KUBEMQ_CONFIG
  kubemq_license_volume
  br__log "posture ${POSTURE}: starting ${POSTURE_ORACLE_CONTAINER} and ${POSTURE_KUBEMQ_CONTAINER}"
  engine_compose "$1/compose-oracle.yaml" up -d --quiet-pull >>"${CERT_RUN_DIR}/logs/$2-compose.log" 2>&1 \
    || br__die "posture ${POSTURE}: oracle failed to start; see logs/$2-compose.log"
  engine_compose "$1/compose-kubemq.yaml" up -d --quiet-pull --force-recreate 2>&1 | redact_secrets >>"${CERT_RUN_DIR}/logs/$2-compose.log"
  br__wait_oracle "${POSTURE_ORACLE_CONTAINER}" "${POSTURE_ORACLE_ADMIN_PORT}" "$2-oracle" \
    || br__die "posture ${POSTURE}: oracle exited during startup; see logs/$2-oracle.log"
  br__wait_kubemq "${POSTURE_KUBEMQ_CONTAINER}" "${POSTURE_KUBEMQ_API_PORT}" "${POSTURE_KUBEMQ_PORT}" "$2-kubemq"
}

# posture_down <track-dir> <log-name>: save both logs, remove the pair (three attempts: a pair
# left running would hold the ports of the next posture), wait for its ports.
posture_down() {
  local i log="${CERT_RUN_DIR}/logs/$2-compose.log"
  engine_logs "${POSTURE_ORACLE_CONTAINER}" "${CERT_RUN_DIR}/logs/$2-oracle.log"
  engine_logs "${POSTURE_KUBEMQ_CONTAINER}" "${CERT_RUN_DIR}/logs/$2-kubemq.log"
  for i in 1 2 3; do
    KUBEMQ_LICENSE_KEY="${KUBEMQ_LICENSE_KEY:-unused-for-teardown}" POSTURE_KUBEMQ_CONFIG="${POSTURE_KUBEMQ_CONFIG:-unused}" \
      engine_compose "$1/compose-kubemq.yaml" down -v --remove-orphans >>"${log}" 2>&1 \
      && engine_compose "$1/compose-oracle.yaml" down -v --remove-orphans >>"${log}" 2>&1 && break
    br__log "posture ${POSTURE}: teardown attempt ${i} failed; see ${log}"
    sleep 5
  done
  wait_ports_free 30 "${POSTURE_PORTS[@]}" || true
}
