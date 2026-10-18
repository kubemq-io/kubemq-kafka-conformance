#!/usr/bin/env bash
# Transactions track entry point (Track B transactions, idempotent producer, exactly-once),
# called by runner/certify.sh. Builds the host runner and the linux franz-go driver, then runs
# every row of matrix/transactions.yaml. Drivers run in client containers on the Kafka image
# pinned in compose/versions.env (exported by certify.sh).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
: "${ORACLE_BOOTSTRAP:?}" "${KUBEMQ_BOOTSTRAP:?}" "${TRACK_RESULTS:?}" "${CERT_RUN_DIR:?}"
: "${KAFKA_IMAGE_REPO:?}" "${KAFKA_IMAGE_DIGEST:?}" "${CERT_ENGINE_ARCH:?}"

command -v go >/dev/null 2>&1 || { echo "[transactions] ERROR: Go is required (see suites/transactions/go.mod)" >&2; exit 2; }
BIN="${CERT_RUN_DIR}/bin/transactions-track"
DRV="${CERT_RUN_DIR}/bin/transactions-drivers"
mkdir -p "${DRV}"
(cd "${HERE}" && go build -o "${BIN}" ./cmd/transactions-track)
(cd "${HERE}" && CGO_ENABLED=0 GOOS=linux GOARCH="${CERT_ENGINE_ARCH}" go build -o "${DRV}/txn-driver" ./cmd/txn-driver)
# shellcheck source=suites/transactions/images.env
. "${HERE}/images.env"
IMAGE="${KAFKA_IMAGE_REPO}@${KAFKA_IMAGE_DIGEST}"
# Java driver: kafka-clients and its runtime dependencies exactly as the pinned Kafka image ships
# them, streamed out through standard output so no container writes into a host directory.
mkdir -p "${DRV}/lib" "${DRV}/java" "${DRV}/upstream"
"${ROOT}/runner/client-run.sh" --entrypoint sh -- "${IMAGE}" -c \
  'cd /opt/kafka/libs && tar cf - kafka-clients-*.jar zstd-jni-*.jar lz4-java-*.jar snappy-java-*.jar slf4j-api-*.jar' \
  | tar xf - -C "${DRV}/lib"
ls "${DRV}"/lib/kafka-clients-*.jar >/dev/null 2>&1 || { echo "[transactions] ERROR: could not copy kafka-clients from ${IMAGE}" >&2; exit 2; }
cp "${HERE}/java/TxnDriver.java" "${DRV}/java/"
# librdkafka driver: confluent-kafka wheels downloaded by pip inside the Python image, checked
# against the hashes in python/requirements.txt, and streamed out the same way.
mkdir -p "${DRV}/wheels" "${DRV}/python"
"${ROOT}/runner/client-run.sh" --entrypoint sh -v "${HERE}/python:/req:ro" -- "${TXN_PYTHON_IMAGE}" -c \
  'pip download -q --disable-pip-version-check --no-deps --only-binary=:all: --require-hashes -r /req/requirements.txt -d /tmp/w >&2 && tar cf - -C /tmp/w .' \
  | tar xf - -C "${DRV}/wheels"
ls "${DRV}"/wheels/confluent_kafka-*.whl >/dev/null 2>&1 || { echo "[transactions] ERROR: could not download the confluent-kafka wheel" >&2; exit 2; }
cp "${HERE}/python/txn_driver.py" "${DRV}/python/"
cp "${HERE}"/upstream/*.sh "${DRV}/upstream/"
chmod -R a+rX "${DRV}"

exec "${BIN}" \
  -matrix "${ROOT}/matrix/transactions.yaml" \
  -oracle "${ORACLE_BOOTSTRAP}" \
  -kubemq "${KUBEMQ_BOOTSTRAP}" \
  -out "${TRACK_RESULTS}" \
  -gaps "${ROOT}/docs/gaps.md" \
  -client-run "${ROOT}/runner/client-run.sh" \
  -image "${IMAGE}" \
  -jdk-image "${TXN_JDK_IMAGE}" \
  -python-image "${TXN_PYTHON_IMAGE}" \
  -bin "${DRV}" \
  ${TRANSACTIONS_ONLY:+-only "${TRANSACTIONS_ONLY}"}
