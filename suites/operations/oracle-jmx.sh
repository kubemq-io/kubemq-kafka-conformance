#!/usr/bin/env bash
# Print one scrape of the Kafka oracle's JMX metrics as kafka-jmx.sh CSV, for the ObjectName
# patterns given as arguments. Runs Kafka's own kafka-jmx.sh inside the oracle container against
# the JMX port compose/kafka-oracle.yaml enables (container-local, never published). Called by
# operations-track; needs CERT_ROOT and the engine variables exported by runner/certify.sh.
set -euo pipefail
: "${CERT_ROOT:?}" "${CERT_ENGINE:?}"
# shellcheck source=runner/lib/engine.sh
. "${CERT_ROOT}/runner/lib/engine.sh"
# shellcheck source=runner/lib/brokers.sh
. "${CERT_ROOT}/runner/lib/brokers.sh"
[[ $# -gt 0 ]] || { echo "usage: $0 <ObjectName pattern>..." >&2; exit 2; }
args=()
for p in "$@"; do args+=(--object-name "${p}"); done
engine_run exec "${ORACLE_CONTAINER}" /opt/kafka/bin/kafka-jmx.sh \
  --jmx-url "service:jmx:rmi:///jndi/rmi://localhost:${ORACLE_JMX_PORT:-9999}/jmxrmi" \
  --one-time true --report-format csv "${args[@]}"
