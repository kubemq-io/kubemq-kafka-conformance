#!/bin/sh
# kcat driver of the clients track (suites/clients/SCENARIO.md). The ledger goes to stdout.
#
# kcat is a command-line tool, so a few scenario steps take its own form, all declared in the
# matrix row: it has no admin API (the track creates the topic, KCERT_ADMIN=0); it prints no
# offsets for what it produces (limit ack-offsets-unknown) and sets headers per run, not per
# record (limit headers-per-run: each phase is one kcat run, as the tool is normally used, and its
# records carry only the client header; the run's exit code is every record's delivery result);
# and its group consumer commits only on close, by librdkafka's auto commit (limit
# commit-on-close: no commit event is reported; the track's fresh reader checks the committed
# offsets). Each consume phase reads to the end of every assigned partition (-e), so a member that
# resumed at the wrong offset, or got records twice, is seen doing it.
set -u
B="${KCERT_BOOTSTRAP:-}" T="${KCERT_TOPIC:-}" G="${KCERT_GROUP:-}" MODE="${KCERT_MODE:-}"
R="${KCERT_RECORDS:-0}" R2="${KCERT_RECORDS2:-0}" K="${KCERT_KEYS:-1}" W="${KCERT_TIMEOUT_SEC:-60}"
CLIENT=kcat
WORK="$(mktemp -d)"

fail() {
  jq -nc --arg s "$1" --arg e "$2" '{ev:"error",step:$s,error_code:-1,error:$e}'
  echo '{"ev":"done"}'
  exit 1
}

if [ -z "$B" ] || [ -z "$T" ] || [ -z "$G" ]; then
  fail inputs "KCERT_BOOTSTRAP, KCERT_TOPIC and KCERT_GROUP are required"
fi
VERSION="$(kcat -V 2>&1 | sed -n 's/^Version \([^ ]*\) (.*librdkafka \([^ ]*\) .*/\1 (librdkafka \2)/p')"
# The group protocol librdkafka actually defaults to, from its own configuration dump.
GP="$(kcat -X dump 2>/dev/null | sed -n 's/^group.protocol = //p')"
jq -nc --arg c "$CLIENT" --arg v "$VERSION" --arg m "$MODE" --arg gp "$GP" \
  '{ev:"start",client:$c,version:$v,mode:$m,group_protocol:$gp,ack_offsets_unknown:true}'

ack() { # ack <phase> <seq> <rc> <output>
  if [ "$3" -eq 0 ]; then
    jq -nc --argjson ph "$1" --argjson s "$2" '{ev:"ack",phase:$ph,seq:$s,partition:-1,offset:-1,error_code:0}'
  else
    jq -nc --argjson ph "$1" --argjson s "$2" --arg e "$4" '{ev:"ack",phase:$ph,seq:$s,partition:-1,offset:-1,error_code:-1,error:$e}'
  fi
}

# produce <phase> <lo> <hi>: one kcat run for the phase's keyed records.
produce() {
  seq=$2
  : >"$WORK/p$1.in"
  while [ "$seq" -lt "$3" ]; do
    printf 'key-%02d:value-%06d\n' $((seq % K)) "$seq" >>"$WORK/p$1.in"
    seq=$((seq + 1))
  done
  out="$(kcat -P -b "$B" -t "$T" -K: -H "client=$CLIENT" -X acks=all -X enable.idempotence=true \
    <"$WORK/p$1.in" 2>&1)"
  rc=$?
  seq=$2
  while [ "$seq" -lt "$3" ]; do ack "$1" "$seq" "$rc" "$out"; seq=$((seq + 1)); done
}

# assigned <phase> <stderr file>: one assigned event per rebalance kcat reported.
assigned() {
  grep 'rebalanced.*: assigned:' "$2" | sed 's/.*: assigned: //' | while read -r line; do
    echo "$line" | grep -o '\[[0-9]*\]' | tr -d '[]' | jq -sc --argjson ph "$1" '{ev:"assigned",phase:$ph,partitions:sort}'
  done
}

# recv <phase> <JSON output file>: kcat -J records to recv events.
recv() {
  jq -c --argjson ph "$1" '{ev:"recv",phase:$ph,
    seq: ((.payload // "") | ltrimstr("value-") | tonumber? // -1),
    partition, offset, key, value: .payload,
    headers: ((.headers // []) as $h | [range(0; $h | length; 2) as $i | [$h[$i], $h[$i + 1]]])}' "$2"
}

# consume <phase> [kcat options]: a group member reads to the end of every assigned partition,
# exits, and commits on close.
consume() {
  ph=$1
  shift
  timeout "$W" kcat -C -b "$B" -G "$G" -J -e "$@" "$T" >"$WORK/c$ph.out" 2>"$WORK/c$ph.err"
  rc=$?
  assigned "$ph" "$WORK/c$ph.err"
  recv "$ph" "$WORK/c$ph.out"
  [ "$rc" -eq 0 ] || fail consume "kcat consumer exited $rc: $(tail -c 400 "$WORK/c$ph.err")"
}

if [ "$MODE" = scenario ]; then
  produce 1 0 "$R"
  consume 1 -X auto.offset.reset=earliest
  produce 2 "$R" $((R + R2))
  consume 2 -X auto.offset.reset=earliest
else
  # Stock defaults: consumer first (default reset latest), then the producer, as the quickstart.
  timeout "$W" kcat -C -b "$B" -G "$G" -J -c "$R" "$T" >"$WORK/c1.out" 2>"$WORK/c1.err" &
  pid=$!
  waited=0
  until grep -q 'rebalanced.*: assigned: .*\[' "$WORK/c1.err" 2>/dev/null; do
    [ "$waited" -ge "$W" ] && fail consume "consumer was never assigned partitions"
    sleep 1
    waited=$((waited + 1))
  done
  # Readiness barrier: kcat reports "Reached end of topic <t> [<p>] at offset <n>" once it has
  # fetched to the end of a partition, which needs its start position (default latest) resolved.
  parts="$(grep 'rebalanced.*: assigned:' "$WORK/c1.err" | tail -1 | sed 's/.*: assigned: //' | grep -o '\[[0-9]*\]' | tr -d '[]')"
  for p in $parts; do
    until grep -q "Reached end of topic $T \[$p\]" "$WORK/c1.err" 2>/dev/null; do
      [ "$waited" -ge "$W" ] && fail consume "start position never resolved on partition $p"
      sleep 1
      waited=$((waited + 1))
    done
    at="$(grep "Reached end of topic $T \[$p\]" "$WORK/c1.err" | head -1 | sed 's/.* at offset \([0-9]*\).*/\1/')"
    jq -nc --argjson p "$p" --argjson o "$at" '{ev:"position",phase:1,partition:$p,offset:$o}'
  done
  seq=0
  lines=""
  while [ "$seq" -lt "$R" ]; do lines="${lines}$(printf 'value-%06d' "$seq")
"; seq=$((seq + 1)); done
  out="$(printf '%s' "$lines" | kcat -P -b "$B" -t "$T" 2>&1)"
  rc=$?
  seq=0
  while [ "$seq" -lt "$R" ]; do ack 1 "$seq" "$rc" "$out"; seq=$((seq + 1)); done
  wait "$pid"
  crc=$?
  assigned 1 "$WORK/c1.err"
  recv 1 "$WORK/c1.out"
  [ "$crc" -eq 0 ] || fail consume "kcat consumer exited $crc: $(tail -c 400 "$WORK/c1.err")"
fi
echo '{"ev":"done"}'
