#!/usr/bin/env bash
# App-sim track entry point, called by runner/certify.sh. The two orders applications run in
# containers (runner/client-run.sh) against the Kafka oracle first, then against KubeMQ with only
# the bootstrap address changed; verify.py judges each run from the ledgers. Writes TRACK_RESULTS.
#
#   oracle run not PASS  -> VOID (the run proves nothing about KubeMQ)
#   KubeMQ run PASS      -> PASS
#   KubeMQ run FAIL/VOID -> FAIL (a check that could not run on KubeMQ ran on Kafka)
#
# Ported from kubemq-server tests/kafka-app-sim/run.sh: same steps, minus the three-broker and
# fault-injection options (not ported); podman-only calls replaced by the engine
# abstraction; replication factor 1 for the single-node brokers.
# shellcheck source-path=SCRIPTDIR
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
: "${ORACLE_BOOTSTRAP:?}" "${KUBEMQ_BOOTSTRAP:?}" "${TRACK_RESULTS:?}" "${CERT_RUN_DIR:?}" "${CERT_ENGINE_ARCH:?}"
# shellcheck source=../../runner/lib/engine.sh
. "${ROOT}/runner/lib/engine.sh"
set -a
# shellcheck source=app-sim.env
. "${HERE}/app-sim.env"
set +a
CLIENT_RUN="${ROOT}/runner/client-run.sh"
log() { echo "[app-sim] $*" >&2; }
sum() { if command -v shasum >/dev/null 2>&1; then shasum -a 256; else sha256sum; fi; }

# build <dir> <name>: build an application image once per source and pin digest; prints its tag.
build() {
  local dir="${HERE}/$1" digest tag
  digest="$( (cd "${dir}" && find . -type f | LC_ALL=C sort | xargs cat; cat "${HERE}/app-sim.env") | sum | cut -c1-16)"
  tag="localhost/kcert-appsim-$2:${digest}-${CERT_ENGINE_ARCH}"
  if ! engine_run image inspect "${tag}" >/dev/null 2>&1; then
    engine_run build -q -t "${tag}" -f "${dir}/Containerfile" \
      --build-arg "BUILD_IMAGE=${JAVA_BUILD_IMAGE}" \
      --build-arg "RUNTIME_IMAGE=$([[ "$2" == java ]] && echo "${JAVA_RUNTIME_IMAGE}" || echo "${PYTHON_RUNTIME_IMAGE}")" \
      --build-arg "KAFKA_CLIENTS_VERSION=${KAFKA_CLIENTS_VERSION}" "${dir}" >&2 || return 1
  fi
  echo "${tag}"
}

not_run() { # not_run <reason>
  jq -n --arg r "$1" '{schema: 1, track: "app-sim", not_run_reason: $r, rows: []}' >"${TRACK_RESULTS}"
  exit 0
}

JIMG="$(build java java)" || not_run "the Java application image could not be built"
PIMG="$(build python python)" || not_run "the Python application image could not be built"
log "images: ${JIMG} ${PIMG}"

# run_target <oracle|kubemq> <bootstrap>: the whole application run; sets RC and OUT.
run_target() {
  local target="$1" boot="$2" run
  OUT="${CERT_RUN_DIR}/transcripts/app-sim/${target}"
  rm -rf "${OUT}" && mkdir -p "${OUT}" && chmod 0777 "${OUT}"
  run="${target}-$(date -u +%Y%m%dT%H%M%SZ)"
  local common=(--bootstrap "${boot}" --run "${run}" --done-file /out/produce.done
    --idle-exit-sec "${APPSIM_IDLE_SEC}" --timeout-sec "${APPSIM_TIMEOUT_SEC}")
  java_app() { local n="$1"; shift; "${CLIENT_RUN}" -v "${OUT}:/out" -- "${JIMG}" "${common[@]}" "$@" >"${OUT}/${n}.log" 2>&1; }
  py_app() { local n="$1"; shift; "${CLIENT_RUN}" -v "${OUT}:/out" -- "${PIMG}" "${common[@]}" "$@" >"${OUT}/${n}.log" 2>&1; }
  {
    echo "requested_kafka_clients ${KAFKA_CLIENTS_VERSION}"
    echo "requested_confluent_kafka ${CONFLUENT_KAFKA_PYTHON_VERSION}"
    echo "requested_librdkafka ${CONFLUENT_KAFKA_PYTHON_VERSION}"
    echo "java_image ${JIMG}"
    echo "python_image ${PIMG}"
    echo "bootstrap ${boot}"
  } >"${OUT}/instrument.txt"
  log "${target}: [0/7] client versions"
  java_app version-java --cmd version
  py_app version-python --cmd version
  log "${target}: [1/7] admin (replication factor ${APPSIM_RF})"
  java_app admin-java --cmd admin --rf "${APPSIM_RF}"
  py_app admin-python --cmd admin --rf "${APPSIM_RF}"
  log "${target}: [2/7] consumer groups and exactly-once pipelines"
  java_app group-java --cmd group --group orders-java & local g1=$!
  py_app group-python --cmd group --group orders-python & local g2=$!
  java_app eos-java --cmd eos --app java --group enricher-java & local e1=$!
  py_app eos-python --cmd eos --app python --group enricher-python & local e2=$!
  sleep 8
  log "${target}: [3/7] produce (${APPSIM_COUNT} per application)"
  java_app produce-java --cmd produce --app java --count "${APPSIM_COUNT}" --codecs gzip,lz4 & local p1=$!
  py_app produce-python --cmd produce --app python --count "${APPSIM_COUNT}" --codecs snappy,zstd & local p2=$!
  wait "${p1}"; wait "${p2}"
  touch "${OUT}/produce.done"
  log "${target}: [4/7] drain"
  wait "${g1}"; wait "${g2}"; wait "${e1}"; wait "${e2}"
  log "${target}: [5/7] fresh-reader readback"
  java_app readback-java --cmd readback
  log "${target}: [6/7] committed offsets versus log end"
  java_app lag-java --cmd lag --groups orders-java,orders-python,enricher-java,enricher-python
  log "${target}: [7/7] verify"
  "${CLIENT_RUN}" -v "${OUT}:/out" -v "${HERE}/verify.py:/verify.py:ro" --entrypoint python3 -- "${PIMG}" \
    /verify.py /out --title "Kafka application simulator — ${target} (${boot})" >"${OUT}/verify.log" 2>&1
  RC=$?
  log "${target}: verify.py exit ${RC}"
}

# problems <report.md>: the FAIL and VOID lines of a verify.py report, as a JSON array.
problems() {
  grep -E '\| \*\*(FAIL|VOID)\*\* \|' "$1" 2>/dev/null | sed 's/^| //; s/ |$//' | jq -R . | jq -s .
}

leg_json() { # leg_json <rc> <out> <ms>
  local ok=false
  [[ "$1" == 0 ]] && ok=true
  jq -n --argjson ok "${ok}" --argjson rc "$1" --argjson ms "$3" --argjson p "$(problems "$2/report.md")" \
    --arg v "kafka-clients ${KAFKA_CLIENTS_VERSION}, confluent-kafka ${CONFLUENT_KAFKA_PYTHON_VERSION}" \
    '{ok: $ok, events: 0, verify_exit_code: $rc, problems: $p, client_version: $v, duration_ms: $ms}'
}

verdict_name() { case "$1" in 0) echo PASS ;; 1) echo FAIL ;; *) echo VOID ;; esac; }

t0=$(date +%s)
run_target oracle "${ORACLE_BOOTSTRAP}"; ORC=${RC}; OOUT="${OUT}"; t1=$(date +%s)
ORACLE_LEG="$(leg_json "${ORC}" "${OOUT}" $(( (t1 - t0) * 1000 )))"
KUBEMQ_LEG=null
if [[ "${ORC}" != 0 ]]; then
  VERDICT=VOID
  DETAIL="oracle run: verify.py $(verdict_name "${ORC}"); see transcripts/app-sim/oracle/report.md"
else
  run_target kubemq "${KUBEMQ_BOOTSTRAP}"; KRC=${RC}; KOUT="${OUT}"; t2=$(date +%s)
  KUBEMQ_LEG="$(leg_json "${KRC}" "${KOUT}" $(( (t2 - t1) * 1000 )))"
  if [[ "${KRC}" == 0 ]]; then
    VERDICT=PASS; DETAIL="verify.py PASS on both brokers"
  else
    VERDICT=FAIL; DETAIL="KubeMQ run: verify.py $(verdict_name "${KRC}") (PASS on Kafka); see transcripts/app-sim/kubemq/report.md"
  fi
fi
jq -n --arg v "${VERDICT}" --arg d "${DETAIL}" --argjson o "${ORACLE_LEG}" --argjson k "${KUBEMQ_LEG}" \
  --argjson ms $(( ($(date +%s) - t0) * 1000 )) \
  --arg title "$(sed -n 's/^    title: "\(.*\)"$/\1/p' "${ROOT}/matrix/app-sim.yaml" | head -1)" \
  --arg src "$(sed -n 's/^    source: //p' "${ROOT}/matrix/app-sim.yaml" | head -1)" \
  --arg jv "${KAFKA_CLIENTS_VERSION}" --arg pv "${CONFLUENT_KAFKA_PYTHON_VERSION}" \
  '{schema: 1, track: "app-sim",
    clients: {"kafka-clients (app-sim)": $jv, "confluent-kafka-python (app-sim)": $pv},
    rows: [{id: "app-sim.orders-apps", kind: "behaviour", title: $title, verdict: $v, provenance: "kubemq-authored",
            source: $src, detail: $d, oracle: $o, kubemq: $k, duration_ms: $ms}]}' >"${TRACK_RESULTS}"
log "app-sim.orders-apps ${VERDICT}: ${DETAIL}"
