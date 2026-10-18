#!/bin/sh
# Upstream row: the path of Apache Kafka's transactions system test
# (tests/kafkatest/tests/core/transactions_test.py, "TransactionsTest") with Kafka's own tools,
# run inside the pinned Kafka image as a client container:
#   1. kafka-verifiable-producer seeds the input topic (values 0..N-1, acks=all);
#   2. one TransactionalMessageCopier per input partition copies it to the output topic in
#      transactions of 50 with random aborts, committing input offsets with group metadata;
#   3. kafka-console-consumer reads the output with read_committed.
# The ducktape test's failure injection (broker bounces, copier kills) is out of scope here.
# Prints one JSON report line (SCENARIOS.md,
# "upstream-copier"); verify judges it.
# Usage: copier.sh <bootstrap> <target>
set -eu
B="$1"; T="$2"
N=1000
BIN=/opt/kafka/bin
ID="kcert-txn-copier-${T}-$(date +%s)-$$"
IN="${ID}-in"; OUT="${ID}-out"; G="${ID}-group"
W="$(mktemp -d)"
log() { echo "[copier] $*" >&2; }

"${BIN}/kafka-topics.sh" --bootstrap-server "$B" --create --topic "$IN" --partitions 2 --replication-factor 1 >&2
"${BIN}/kafka-topics.sh" --bootstrap-server "$B" --create --topic "$OUT" --partitions 3 --replication-factor 1 >&2

log "seeding ${N} records"
"${BIN}/kafka-verifiable-producer.sh" --bootstrap-server "$B" --topic "$IN" --max-messages "$N" --acks -1 >"$W/producer.jsonl" 2>"$W/producer.err" || true
ACKED="$(grep -c '"name":"producer_send_success"' "$W/producer.jsonl" || true)"

EXITS=""
for P in 0 1; do
  log "copying partition ${P}"
  set +e
  "${BIN}/kafka-run-class.sh" org.apache.kafka.tools.TransactionalMessageCopier --broker-list "$B" \
    --input-topic "$IN" --input-partition "$P" --output-topic "$OUT" --transactional-id "${ID}-tid-${P}" \
    --consumer-group "$G" --transaction-size 50 --transaction-timeout 60000 --enable-random-aborts \
    --use-group-metadata >"$W/copier-${P}.jsonl" 2>"$W/copier-${P}.err"
  rc=$?
  set -e
  EXITS="${EXITS}${EXITS:+,}${rc}"
  tail -2 "$W/copier-${P}.jsonl" >&2 || true
done

log "reading the output with read_committed"
"${BIN}/kafka-console-consumer.sh" --bootstrap-server "$B" --topic "$OUT" --from-beginning \
  --isolation-level read_committed --timeout-ms 20000 >"$W/output.txt" 2>"$W/consumer.err" || true

VER=unknown
for f in /opt/kafka/libs/kafka-tools-[0-9]*.jar; do VER="${f##*/kafka-tools-}"; VER="${VER%.jar}"; done
VALUES="$(awk 'BEGIN{ORS=""} NF{ if (n++) printf ","; printf "\"%s\"", $0 }' "$W/output.txt")"
printf '{"client":"kafka tools %s","scenario":"upstream-copier","facts":{"seeded":%d,"produced_acked":%d,"copier_exits":[%s],"read_committed_values":[%s]}}\n' \
  "$VER" "$N" "$ACKED" "$EXITS" "$VALUES"
