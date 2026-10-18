#!/usr/bin/env python3
"""verify.py — decides whether a Kafka application survived being repointed.

Reads every *.jsonl ledger in a run directory (written by the Java and Python apps) and checks the
properties an existing Kafka application depends on. It never trusts an application's own view of
its gaps: "what is in the log" comes only from the fresh readback reader, which is a party to
nothing else in the run.

Three outcomes, never two:
  PASS (exit 0)  every check ran and held.
  FAIL (exit 1)  a property the application relies on was violated; the report names the items.
  VOID (exit 2)  a check could not run or the event it needs never happened (no rebalance, no
                 abort, an incomplete readback). VOID exists so "we learned nothing" can never be
                 read as "it works".

It also refuses to attribute a result to the wrong client: every process records the client library
it actually loaded, and that must equal the version the run requested (from --expect-java-client /
--expect-python-client, else the requested_* lines run.sh writes into instrument.txt). For Java that
means the version, the jar it came from (kafka-clients-<requested>.jar) and, where the release is
pinned below or by --expect-java-commit, the git commit the jar was built from — a release
candidate's jar reports only the release it becomes, so the commit is what tells rc1 from any other
4.4.0 build. For Python it means confluent-kafka AND the librdkafka it bundles (the same version
unless --expect-librdkafka says otherwise). An app that ran processes but whose requested version
was never recorded makes the run VOID. And it holds
every topic to the partition count its create asked for — as described by the admin client and as
seen by the fresh reader's own Metadata — so a phantom auto-created 1-partition topic cannot pass.

`verify.py --self-test` builds a clean synthetic run, proves it PASSes, then breaks it one way at a
time and proves each break is caught BY NAME. Run it before believing any green.
"""
import argparse
import collections
import glob
import json
import os
import sys
import tempfile

PASS, FAIL, VOID, INFO = "PASS", "FAIL", "VOID", "INFO"
SAMPLE = 8  # how many offending items to name per finding


def load(run_dir):
    events = []
    for path in sorted(glob.glob(os.path.join(run_dir, "*.jsonl"))):
        with open(path, encoding="utf-8") as f:
            for n, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError as ex:
                    raise SystemExit(f"VOID: unreadable ledger line {path}:{n}: {ex}")
                e["_file"] = os.path.basename(path)
                e["_line"] = n
                events.append(e)
    return events


class Report:
    def __init__(self):
        self.rows = []

    def add(self, section, check, status, detail, items=None):
        self.rows.append((section, check, status, detail, list(items or [])[:SAMPLE],
                          len(items) if items is not None else 0))

    def verdict(self):
        s = [r[2] for r in self.rows]
        return FAIL if FAIL in s else VOID if VOID in s else PASS

    def has(self, check, status):
        return any(r[1] == check and r[2] == status for r in self.rows)

    def render(self, title):
        out = [f"# {title}", "", f"**Verdict: {self.verdict()}**", ""]
        section = None
        for sec, check, status, detail, items, total in self.rows:
            if sec != section:
                out += ["", f"## {sec}", "", "| check | result | detail |", "|---|---|---|"]
                section = sec
            named = ""
            if items:
                more = f" (+{total - len(items)} more)" if total > len(items) else ""
                named = " — " + ", ".join(str(i) for i in items) + more
            out.append(f"| {check} | **{status}** | {detail}{named} |")
        return "\n".join(out) + "\n"


# Ledger events that are the applications moving data: a disruption must land before the last of
# them, or it hit an idle cluster ("disruption landed under traffic").
TRAFFIC_EVENTS = ("produced", "produce_failed", "consumed", "txn_committed", "txn_aborted", "txn_error")


def ident(e):
    return (e.get("app") if e.get("ev") in ("produced", "produce_failed") else e.get("src_app"), e.get("seq"))


# Which library each app reports, and how a requested version reads once loaded. A release
# candidate's jar reports the release it will become (kafka-clients 4.4.0-rc1 says "4.4.0"), so the
# "-rcN" suffix is dropped before comparing; nothing else is normalised.
CLIENT_LIB = {"java": "kafka-clients", "python": "confluent-kafka"}
APPS = ("java", "python")

# The git commit each kafka-clients release was built from (first 16 hex digits, what AppInfoParser
# reports), from `git ls-remote https://github.com/apache/kafka refs/tags/<tag>^{}`. A release
# candidate is installed locally from its image, so nothing but this pin tells its jar from another
# build of the same release: an RC with no pin here (or --expect-java-commit) cannot pass.
PINNED_COMMITS = {
    "4.3.1": "26b251a451ce941d",
    "4.4.0-rc1": "812f9108eb7e2af6",
}


def is_rc(requested):
    base, sep, rc = (requested or "").partition("-rc")
    return bool(sep) and rc.isdigit()


def loaded_form(app, requested):
    if requested is None:
        return None
    if app == "java":
        base, sep, rc = requested.partition("-rc")
        if sep and rc.isdigit():
            return base
    return requested


def read_expectations(run_dir):
    """The client versions a run asked for, as run.sh recorded them in instrument.txt."""
    want = {}
    keys = {"requested_kafka_clients": "java", "requested_confluent_kafka": "python",
            "requested_kafka_clients_commit": "java_commit", "requested_librdkafka": "librdkafka"}
    path = os.path.join(run_dir, "instrument.txt")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and parts[0] in keys and parts[1] not in ("none", "-"):
                    want[keys[parts[0]]] = parts[1]
    return want


def check_clients(r, by, expect):
    """Every process must say which client library it loaded, and it must be the one requested.

    A result from a client other than the one the run asked for certifies nothing about the
    requested one — and a process that never said which client it ran cannot be attributed."""
    sec = "Client versions"
    expect = expect or {}
    if not any(expect.get(a) for a in APPS):
        r.add(sec, "requested versions recorded", VOID,
              "the run does not record which client versions it asked for (no --expect-* flag and no "
              "requested_* line in instrument.txt), so no result below can be attributed to a client")
        return
    reported = {}
    for e in by["client_version"]:
        reported.setdefault((e.get("app"), e.get("cmd")), []).append(e)
    started = sorted({(e.get("app"), e.get("cmd")) for e in by["start"]}, key=str)
    for app in APPS:
        lib = CLIENT_LIB[app]
        requested = expect.get(app)
        procs = [k for k in started if k[0] == app]
        if not requested:
            if procs:
                r.add(sec, f"{app} {lib}", VOID,
                      f"requested version not recorded for {app}: {len(procs)} {app} processes ran, but nothing "
                      f"says which {lib} the run asked for, so their results cannot be attributed",
                      [f"{a} {c}" for a, c in procs])
            continue
        want = loaded_form(app, requested)
        if not procs:
            r.add(sec, f"{app} {lib}", VOID, f"{requested} was requested but no {app} process ran")
            continue
        if app == "java":
            want_jar = f"kafka-clients-{requested}.jar"
            want_commit = (expect.get("java_commit") or PINNED_COMMITS.get(requested) or "")[:16]
            if is_rc(requested) and not want_commit:
                r.add(sec, f"{app} {lib}", VOID,
                      f"{requested} is a release candidate with no pinned commit (PINNED_COMMITS or "
                      "--expect-java-commit): its jar reports only the release it becomes, so nothing "
                      "tells this candidate from another build of that release")
                continue
        else:
            want_rdk = expect.get("librdkafka") or requested
        silent, wrong, unproven = [], [], []
        for a, c in procs:
            evs = reported.get((a, c), [])
            if not evs:
                silent.append(f"{a} {c}")
            for ev in evs:
                if ev.get("version") != want:
                    wrong.append(f"{a} {c}: loaded {ev.get('version')}")
                if app == "java":
                    if not ev.get("jar") or (want_commit and not ev.get("commit")):
                        unproven.append(f"{a} {c}: no jar or commit recorded")
                    if ev.get("jar") and ev["jar"] != want_jar:
                        wrong.append(f"{a} {c}: loaded from {ev['jar']}, not {want_jar}")
                    if want_commit and ev.get("commit") and ev["commit"][:16] != want_commit:
                        wrong.append(f"{a} {c}: commit {ev['commit']}, not {want_commit} ({requested})")
                else:
                    if not ev.get("librdkafka"):
                        unproven.append(f"{a} {c}: no librdkafka version recorded")
                    elif ev["librdkafka"] != want_rdk:
                        wrong.append(f"{a} {c}: librdkafka {ev['librdkafka']}, not {want_rdk}")
        if wrong:
            r.add(sec, f"{app} {lib}", FAIL,
                  f"requested {requested} (which loads as {want}) but a process ran a different client — "
                  "its results say nothing about the requested one", wrong)
        elif silent or unproven:
            r.add(sec, f"{app} {lib}", VOID,
                  "a process never proved which client it loaded, so its results cannot be attributed",
                  silent + unproven)
        else:
            if app == "java":
                extra = f"from {want_jar}" + (f", commit {want_commit}" if want_commit else ", commit not pinned")
            else:
                extra = f"librdkafka {want_rdk}"
            r.add(sec, f"{app} {lib}", PASS,
                  f"{want} loaded in all {len(procs)} {app} processes (requested {requested}); {extra}")


def verify(events, lag_required=True, expect=None):
    r = Report()
    by = collections.defaultdict(list)
    for e in events:
        by[e["ev"]].append(e)

    # ------------------------------------------------------------------ processes
    for e in by["fatal"]:
        r.add("Processes", f"{e.get('app')} {e.get('cmd')}", FAIL, f"crashed: {e.get('error')}")
    started = {(e.get("app"), e.get("cmd")) for e in by["start"]}
    ended = {(e.get("app"), e.get("cmd")) for e in by["end"]}
    for k in sorted(started - ended, key=str):
        r.add("Processes", f"{k[0]} {k[1]}", VOID, "started but never wrote its end marker (killed or hung)")

    # ------------------------------------------------------------------ client versions
    check_clients(r, by, expect)

    # ------------------------------------------------------------------ injected failure
    # A run that asked for a disruption must prove it happened (a clean pass over an
    # event that never ran is the classic instrument lie) and is then judged on the
    # properties that must survive it, not on the retriable errors it provokes.
    chaos = by["chaos"]
    for e in chaos:
        if e.get("rc") != 0:
            r.add("Injected failure", "disruption happened", VOID,
                  f"`{e.get('cmd', '')[:80]}` exited {e.get('rc')}: {e.get('output', '').strip()[:160]} — nothing below "
                  "tells you how the system behaves under a failure that did not occur")
        else:
            r.add("Injected failure", "disruption happened", PASS, e.get("output", "").strip().replace("\n", "; ")[:200])
    # ...and that it landed while the applications were still moving data. A node killed after the
    # last send, delivery and transaction tested an idle cluster: the run then says nothing about
    # failover under traffic, however green everything below it is. (On a fast target the whole
    # produce phase can finish in a few seconds, before a fixed --chaos-after-sec fires.)
    traffic = [x for k in TRAFFIC_EVENTS for x in by[k]]
    for e in chaos:
        if e.get("rc") != 0:
            continue
        stamped = [x["t"] for x in traffic if isinstance(x.get("t"), (int, float))]
        if not isinstance(e.get("t"), (int, float)) or not stamped:
            r.add("Injected failure", "disruption landed under traffic", VOID,
                  "the ledgers carry no timestamps, so nothing shows the disruption happened while data was moving")
            continue
        after = collections.Counter(x["ev"] for x in traffic
                                    if isinstance(x.get("t"), (int, float)) and x["t"] >= e["t"])
        acks_after = after.get("produced", 0)
        if not after:
            r.add("Injected failure", "disruption landed under traffic", VOID,
                  f"it ran {(e['t'] - max(stamped)) / 1000:.1f}s after the last send, delivery or transaction — it hit an "
                  "idle cluster, so nothing below tests failover under traffic; fire it earlier (--chaos-after-sec) "
                  "or send more (--count)")
        else:
            r.add("Injected failure", "disruption landed under traffic", PASS,
                  f"after it: {acks_after} acknowledged sends, " +
                  ", ".join(f"{n} {k}" for k, n in sorted(after.items()) if k != "produced") +
                  ("" if acks_after else " (the producers had already finished; the consumers and pipelines had not)"))

    # ------------------------------------------------------------------ admin
    # What each create asked for, so a describe can be held to it. A broker that refuses a create
    # (or ignores its partition count) and then answers Metadata with an auto-created 1-partition
    # "phantom" makes the describe SUCCEED — only the comparison with the request exposes it.
    requested_parts = {}
    for e in by["admin"]:
        if e["op"] == "create_topic" and e.get("requested_partitions") is not None:
            requested_parts[e["target"]] = e["requested_partitions"]
    for e in by["admin"]:
        name = f"{e['app']} {e['op']}" + (f" {e['target']}" if e.get("target") else "")
        err = str(e.get("error", ""))
        if e["op"] == "create_topic" and ("TopicExists" in err or "TOPIC_ALREADY_EXISTS" in err):
            r.add("Admin calls", name, VOID,
                  "the topic already existed, so this target still holds an earlier run's data and every "
                  "loss/duplicate count below would mix two runs — start from an empty cluster")
            continue
        if e.get("ok") and e.get("expect_retention_ms") and e.get("retention_ms") != e["expect_retention_ms"]:
            r.add("Admin calls", name, FAIL,
                  f"the config change did not take effect: read back retention.ms={e.get('retention_ms')}, "
                  f"expected {e['expect_retention_ms']} after {e.get('attempts')} reads")
            continue
        if e["op"] == "describe_topic" and e.get("ok"):
            want = requested_parts.get(e.get("target"))
            if want is None:
                r.add("Admin calls", name, VOID,
                      "no create recorded how many partitions were asked for, so a phantom auto-created "
                      "topic cannot be told from the real one")
                continue
            if e.get("partitions") != want:
                r.add("Admin calls", name, FAIL,
                      f"described with {e.get('partitions')} partitions but the create asked for {want} — the "
                      "application is running on a topic it did not ask for (a refused create followed by an "
                      "auto-created phantom looks exactly like this)", [f"{e.get('target')}: {e.get('partitions')} != {want}"])
                continue
        if e.get("ok"):
            extra = {k: e[k] for k in ("brokers", "partitions", "replicas", "rf", "retention_ms", "count", "attempts") if k in e}
            note = e.get("error", "")
            r.add("Admin calls", name, PASS, ", ".join(f"{k}={v}" for k, v in extra.items()) + (f" ({note})" if note else ""))
        else:
            r.add("Admin calls", name, FAIL, f"refused: {e.get('error')}")

    # ------------------------------------------------------------------ producers
    acked = {}
    dup_ack = []
    for e in by["produced"]:
        k = (e["app"], e["seq"])
        if k in acked:
            dup_ack.append(k)
        acked[k] = e
    failed = {(e["app"], e["seq"]): e for e in by["produce_failed"]}
    phases = collections.defaultdict(int)
    for e in by["produced"]:
        phases[(e["app"], e["codec"])] += 1
    if not acked:
        r.add("Producers", "anything acknowledged", VOID, "no send was acknowledged — every other check is meaningless")
        return r
    r.add("Producers", "acknowledged sends", PASS if not failed else FAIL,
          f"{len(acked)} acknowledged, {len(failed)} failed; by codec: "
          + ", ".join(f"{a}/{c}={n}" for (a, c), n in sorted(phases.items())),
          [f"{a}:{s} {failed[(a, s)]['error']}" for (a, s) in sorted(failed)])
    r.add("Producers", "one acknowledgement per send", FAIL if dup_ack else PASS,
          "the same send acknowledged twice" if dup_ack else "every send acknowledged once", dup_ack)
    # An acknowledgement with no offset (the client reports -1) is its own finding. Apache Kafka
    # answers a retried idempotent batch that already landed with the offset it landed at; a
    # broker that answers DUPLICATE_SEQUENCE_NUMBER instead makes the client complete the send with
    # offset -1. Such sends are named here once, and kept out of the offset comparisons below,
    # where -1 would read as "two sends shared an offset" and "stored out of order" — both false.
    # Their presence and order are proven from the log itself by the fresh reader.
    no_offset = sorted((e["app"], e["seq"]) for e in by["produced"] if not isinstance(e.get("o"), int) or e["o"] < 0)
    r.add("Producers", "acknowledgement carries its offset", FAIL if no_offset else PASS,
          f"{len(no_offset)} sends were acknowledged with no offset (-1); Apache Kafka returns the offset a "
          "retried idempotent batch was first written at" if no_offset else "every acknowledgement carried an offset",
          [f"{a}:{s}" for a, s in no_offset])
    has_off = lambda e: isinstance(e.get("o"), int) and e["o"] >= 0  # noqa: E731
    po = collections.defaultdict(list)
    for e in by["produced"]:
        if has_off(e):
            po[(e["p"], e["o"])].append(f"{e['app']}:{e['seq']}")
    shared = [f"p{p}@{o}={v}" for (p, o), v in po.items() if len(v) > 1]
    r.add("Producers", "distinct offsets", FAIL if shared else PASS,
          "two acknowledged sends were given the same partition offset" if shared else "no offset handed out twice", shared)
    inv = []
    per_key = collections.defaultdict(list)
    for e in by["produced"]:
        per_key[(e["app"], e["key"])].append(e)
    for (app, key), es in per_key.items():
        es.sort(key=lambda x: x["seq"])
        for a, b in zip(es, es[1:]):
            if a["p"] != b["p"]:
                inv.append(f"{app}:{key} split across p{a['p']}/p{b['p']}")
        placed = [x for x in es if has_off(x)]
        for a, b in zip(placed, placed[1:]):
            if b["o"] <= a["o"]:
                inv.append(f"{app}:{key} seq {a['seq']}@{a['o']} after {b['seq']}@{b['o']}")
    r.add("Producers", "per-customer order", FAIL if inv else PASS,
          "a customer's later order was stored before an earlier one" if inv else
          f"{len(per_key)} customer keys, each on one partition in send order", inv)

    # ------------------------------------------------------------------ fresh-reader log
    # The reader's own Metadata view is a second, independent witness of the partition count: a
    # consumer learns partitions from Metadata, not from the admin describe path.
    for e in by["rb_begin"]:
        want = requested_parts.get(e.get("topic"))
        if want is None:
            continue
        name = f"`{e['topic']}` partitions ({e.get('iso')})"
        if e.get("partitions") != want:
            r.add("Log (fresh reader)", name, FAIL,
                  f"the reader's Metadata shows {e.get('partitions')} partitions but the create asked for {want}",
                  [f"{e['topic']}: {e.get('partitions')} != {want}"])
        else:
            r.add("Log (fresh reader)", name, PASS, f"{want} partitions, as created")
    rb_end = {(e["topic"], e["iso"]): e for e in by["rb_end"]}
    log_ok = ("orders", "read_uncommitted") in rb_end and rb_end[("orders", "read_uncommitted")].get("complete")
    if not log_ok:
        r.add("Log (fresh reader)", "readback complete", VOID,
              "the independent reader did not reach the end of `orders`; loss and duplicate checks cannot run")
    else:
        log = collections.defaultdict(list)
        for e in by["rb"]:
            if e["topic"] == "orders":
                log[(e["app"], e["seq"])].append((e["p"], e["o"]))
        missing = sorted(k for k in acked if k not in log)
        r.add("Log (fresh reader)", "no acknowledged loss", FAIL if missing else PASS,
              f"{len(missing)} acknowledged sends are not in the log" if missing else
              f"all {len(acked)} acknowledged sends are in the log", [f"{a}:{s}" for a, s in missing])
        dups = sorted(k for k, v in log.items() if len(v) > 1)
        r.add("Log (fresh reader)", "no duplicates in the log", FAIL if dups else PASS,
              "an idempotent send is stored more than once" if dups else "each record stored exactly once",
              [f"{a}:{s} at {log[(a, s)]}" for a, s in dups])
        moved = sorted(k for k, e in acked.items() if k in log and has_off(e) and (e["p"], e["o"]) not in log[k])
        r.add("Log (fresh reader)", "acknowledged offset holds the record", FAIL if moved else PASS,
              "the offset a send was acknowledged at holds something else" if moved else
              "every acknowledged offset holds its record" +
              (f" ({len(no_offset)} acknowledged with no offset are checked by presence and log order)" if no_offset else ""),
              [f"{a}:{s}" for a, s in moved])
        # Per-customer order as the log itself holds it — the witness that does not depend on
        # what the acknowledgements said.
        by_part = collections.defaultdict(list)
        for e in by["rb"]:
            if e["topic"] == "orders" and e.get("iso", "read_uncommitted") == "read_uncommitted" \
                    and "seq" in e and "key" in e:
                by_part[e["p"]].append(e)
        log_inv = []
        for p, es in by_part.items():
            last = {}
            for e in sorted(es, key=lambda x: x["o"]):
                k = (e["app"], e["key"])
                if k in last and e["seq"] < last[k][0]:
                    log_inv.append(f"{e['app']}:{e['key']} seq {e['seq']}@{e['o']} after {last[k][0]}@{last[k][1]} (p{p})")
                last[k] = (e["seq"], e["o"])
        r.add("Log (fresh reader)", "per-customer order in the log", FAIL if log_inv else PASS,
              "the log holds a customer's later order before an earlier one" if log_inv else
              "every customer's orders are in send order in the log", log_inv)
        extra = sorted(k for k in log if k not in acked)
        r.add("Log (fresh reader)", "stored without acknowledgement", INFO,
              f"{len(extra)} records are in the log although their send reported failure or never reported "
              "(legal for a failed send that the broker did store; watch the count)", [f"{a}:{s}" for a, s in extra])

    # ------------------------------------------------------------------ consumer groups
    groups = sorted({e["group"] for e in by["consumed"] + by["assigned"] if e.get("group", "").startswith("orders-")})
    if not groups:
        r.add("Consumer groups", "any group ran", VOID, "no consumer-group events")
    lag = collections.defaultdict(dict)
    for e in by["lag"]:
        if e.get("topic") == "orders":
            lag[e["group"]][e["p"]] = (e["committed"], e["end"])
    for g in groups:
        sec = f"Consumer group `{g}`"
        ge = [e for e in events if e.get("group") == g]
        members_assigned = {e["member"] for e in ge if e["ev"] == "assigned" and e.get("partitions")}
        revokes = [e for e in ge if e["ev"] in ("revoked", "lost") and e.get("partitions")]
        join_req = [e for e in ge if e["ev"] == "member_join_requested"]
        leave_req = [e for e in ge if e["ev"] == "member_leave_requested"]
        happened = len(members_assigned) >= 3 and revokes and join_req and leave_req
        r.add(sec, "rebalance happened", PASS if happened else VOID,
              f"{len(members_assigned)} members received partitions, {len(revokes)} revocations, "
              f"{len(leave_req)} leave, {len(join_req)} join" if happened else
              "the group never rebalanced under traffic, so nothing about rebalancing was tested")
        consumed = collections.defaultdict(list)
        for e in ge:
            if e["ev"] == "consumed":
                consumed[(e["src_app"], e["seq"])].append(e["member"])
        missing = sorted(k for k in acked if k not in consumed)
        r.add(sec, "no acknowledged loss", FAIL if missing else PASS,
              f"{len(missing)} acknowledged sends never reached any member" if missing else
              f"every one of {len(acked)} acknowledged sends reached a member", [f"{a}:{s}" for a, s in missing])
        redelivered = sum(len(v) - 1 for v in consumed.values() if len(v) > 1)
        r.add(sec, "redeliveries", INFO,
              f"{redelivered} records delivered more than once (expected around a rebalance with at-least-once commits)")
        # Ownership and in-order delivery, replayed from each member's own event order.
        owned = collections.defaultdict(set)
        last_off = {}
        bad_owner, backwards = [], []
        for e in sorted(ge, key=lambda x: (x["_file"], x["_line"])):
            m = e.get("member")
            if e["ev"] == "assigned":
                owned[m].update(e.get("partitions") or [])
                for p in e.get("partitions") or []:
                    last_off.pop((m, p), None)
            elif e["ev"] in ("revoked", "lost"):
                owned[m].difference_update(e.get("partitions") or [])
            elif e["ev"] == "consumed":
                if e["p"] not in owned[m]:
                    bad_owner.append(f"{m} p{e['p']}@{e['o']}")
                prev = last_off.get((m, e["p"]))
                if prev is not None and e["o"] <= prev:
                    backwards.append(f"{m} p{e['p']} {prev}->{e['o']}")
                last_off[(m, e["p"])] = e["o"]
        r.add(sec, "only owners consume", FAIL if bad_owner else PASS,
              "a member received records from a partition it did not hold" if bad_owner else
              "every record came from a partition its member held", bad_owner)
        r.add(sec, "in-order delivery", FAIL if backwards else PASS,
              "a member saw a partition go backwards without a reassignment" if backwards else
              "offsets only moved forward within each assignment", backwards)
        errs = [e for e in ge if e["ev"] in ("commit_failed", "poll_error", "member_error")]
        r.add(sec, "client errors", INFO, f"{len(errs)} commit/poll/member errors",
              [f"{e['ev']} {e.get('member')}: {e.get('error')}" for e in errs])
        if g in lag:
            behind = [f"p{p} committed {c} of {n}" for p, (c, n) in sorted(lag[g].items()) if c != n]
            r.add(sec, "final committed offsets", FAIL if behind else PASS,
                  "the group finished with committed offsets short of the log end" if behind else
                  f"committed = log end on all {len(lag[g])} partitions", behind)
        elif lag_required:
            r.add(sec, "final committed offsets", VOID, "no lag reading for this group")

    # ------------------------------------------------------------------ exactly-once pipelines
    enr_ok = ("orders-enriched", "read_committed") in rb_end and rb_end[("orders-enriched", "read_committed")].get("complete")
    for e in by["stable_wait"]:
        if e.get("stable"):
            r.add("Transactions left open", f"{e['topic']} settles", PASS,
                  f"every transaction on `{e['topic']}` resolved; read_committed caught up after {e['waited_ms'] / 1000:.1f}s")
        else:
            r.add("Transactions left open", f"{e['topic']} settles", FAIL,
                  f"{e['records_behind']} records still hidden from read_committed readers after "
                  f"{e['waited_ms'] / 1000:.0f}s — past any transaction timeout, so a transaction was left hanging and "
                  "consumers of these partitions are stalled", [f"p{p}" for p in e.get("partitions_behind", [])])
    for app in sorted({e["app"] for e in by["txn_committed"] + by["txn_aborted"] + by["txn_error"]}):
        sec = f"Exactly-once pipeline ({app})"
        committed = {e["txn"] for e in by["txn_committed"] if e["app"] == app}
        aborted = [e for e in by["txn_aborted"] if e["app"] == app]
        aborted_txns = {e["txn"] for e in aborted}
        wrote = sum(e.get("sent_ok", 0) for e in aborted)
        happened = committed and aborted and wrote > 0
        r.add(sec, "commits and aborts both happened", PASS if happened else VOID,
              f"{len(committed)} committed, {len(aborted)} aborted after {wrote} acknowledged sends" if happened else
              f"{len(committed)} committed, {len(aborted)} aborted, {wrote} acknowledged sends inside aborts — "
              "without both, invisibility of aborted output proves nothing")
        errs = [e for e in by["txn_error"] if e["app"] == app]
        if chaos:
            # Around a coordinator failover Apache Kafka answers a transaction's offset
            # commit with ILLEGAL_GENERATION / CommitFailed too; the pattern is abort,
            # rewind, retry — which the app does. Exactly-once is what must survive, and
            # the checks below carry that verdict.
            r.add(sec, "transaction errors", INFO,
                  f"{len(errs)} transactions failed and were aborted and retried by the application "
                  "(expected around an injected failure; the exactly-once checks decide)",
                  [f"txn {e['txn']}: {e['error']}" for e in errs])
        else:
            r.add(sec, "transaction errors", FAIL if errs else PASS,
                  f"{len(errs)} transactions failed with no injected failure to explain them" if errs else "no transaction failed",
                  [f"txn {e['txn']}: {e['error']}" for e in errs])
        if not enr_ok:
            r.add(sec, "read_committed readback complete", VOID, "the independent reader did not reach the end of `orders-enriched`")
            continue
        rc = [e for e in by["rb"] if e["topic"] == "orders-enriched" and e["iso"] == "read_committed" and e.get("app") == app]
        seen = collections.Counter((e["src_app"], e["src_seq"]) for e in rc)
        missing = sorted(k for k in acked if seen[k] == 0)
        twice = sorted(k for k, n in seen.items() if n > 1)
        visible_aborted = sorted({e["txn"] for e in rc if e["txn"] in aborted_txns})
        # A transaction whose commit call failed is in doubt to the application: the broker may
        # have committed it (output and input offsets together, so the input is not re-read) or
        # aborted it (the input is re-processed). Either outcome is correct; the exactly-once
        # checks around this one catch a transaction that was both visible and redone.
        in_doubt = {e["txn"] for e in errs} - committed - aborted_txns
        visible_in_doubt = sorted({e["txn"] for e in rc if e["txn"] in in_doubt})
        unknown_txn = sorted({e["txn"] for e in rc if e["txn"] not in committed and e["txn"] not in aborted_txns
                              and e["txn"] not in in_doubt})
        r.add(sec, "every input processed", FAIL if missing else PASS,
              f"{len(missing)} acknowledged orders have no committed output" if missing else
              f"all {len(acked)} orders have committed output", [f"{a}:{s}" for a, s in missing])
        r.add(sec, "processed exactly once", FAIL if twice else PASS,
              "an order's output was committed more than once" if twice else "no order committed twice",
              [f"{a}:{s} x{seen[(a, s)]}" for a, s in twice])
        r.add(sec, "aborted output invisible", FAIL if visible_aborted else PASS,
              "a read_committed reader sees output of aborted transactions" if visible_aborted else
              f"none of {len(aborted_txns)} aborted transactions visible to read_committed",
              [f"txn {t}" for t in visible_aborted])
        r.add(sec, "visible output traces to a commit", FAIL if unknown_txn else PASS,
              "read_committed output from a transaction the app never ran" if unknown_txn else
              "all visible output belongs to committed transactions"
              + (f", or to {len(visible_in_doubt)} whose commit call failed and the broker completed"
                 if visible_in_doubt else ""), [f"txn {t}" for t in unknown_txn])
        if in_doubt:
            r.add(sec, "transactions with an unknown outcome", INFO,
                  f"{len(in_doubt)} transactions failed at a point where the application cannot know the outcome; "
                  f"the broker committed {len(visible_in_doubt)} of them and aborted the rest",
                  [f"txn {t} committed by the broker" for t in visible_in_doubt])
        ru = [e for e in by["rb"] if e["topic"] == "orders-enriched" and e["iso"] == "read_uncommitted"
              and e.get("app") == app and e["txn"] in aborted_txns]
        r.add(sec, "aborted records retained in the log", INFO,
              f"{len(ru)} aborted records visible to read_uncommitted (Apache Kafka keeps them, filtered by "
              "transaction markers; zero means aborted writes are not retained, which hides them just as well)")
    return r


# ------------------------------------------------------------------------------------ self-test

def synthetic():
    """A small, fully correct run: 24 sends, a group that rebalances, an EOS pipeline with one abort."""
    ev = []
    L = lambda f, **kv: ev.append({"_f": f, **kv})  # noqa: E731
    for app in ("java",):
        for cmd in ("admin", "produce", "group", "eos", "readback", "lag"):
            L(f"{cmd}-{app}", ev="start", app=app, cmd=cmd)
            L(f"{cmd}-{app}", ev="client_version", app=app, cmd=cmd, lib="kafka-clients", version="4.3.1",
              commit="26b251a451ce941d", jar="kafka-clients-4.3.1.jar")
    L("version-python", ev="start", app="python", cmd="version")
    L("version-python", ev="client_version", app="python", cmd="version", lib="confluent-kafka", version="2.15.1",
      librdkafka="2.15.1")
    L("version-python", ev="end", app="python", cmd="version", rc=0)
    # admin: both topics created with the partition counts the rest of the run uses, and described so
    for topic, n in (("orders", 4), ("orders-enriched", 1)):
        L("admin-java", ev="admin", app="java", op="create_topic", target=topic, ok=True, rf=3, requested_partitions=n)
    for topic, n in (("orders", 4), ("orders-enriched", 1)):
        L("admin-java", ev="admin", app="java", op="describe_topic", target=topic, ok=True, partitions=n, replicas=3,
          attempts=1)
    # produce: 24 sends, keys cust-0..3, 4 partitions
    offs = collections.Counter()
    produced = []
    for s in range(24):
        key = f"cust-{s % 4}"
        p = s % 4
        o = offs[p]
        offs[p] += 1
        produced.append((s, key, p, o))
        L("produce-java", ev="produced", app="java", topic="orders", p=p, o=o, key=key, seq=s, codec="gzip")
    # group: m1,m2,m3 get partitions; m3 leaves; m4 joins
    g = "orders-java"
    L("group-java", ev="assigned", group=g, member="m1", partitions=[0])
    L("group-java", ev="assigned", group=g, member="m2", partitions=[1])
    L("group-java", ev="assigned", group=g, member="m3", partitions=[2, 3])
    for s, key, p, o in produced[:12]:
        m = {0: "m1", 1: "m2", 2: "m3", 3: "m3"}[p]
        L("group-java", ev="consumed", group=g, member=m, topic="orders", p=p, o=o, key=key, src_app="java", seq=s)
    L("group-java", ev="member_leave_requested", group=g, member="m3")
    L("group-java", ev="revoked", group=g, member="m3", partitions=[2, 3])
    L("group-java", ev="assigned", group=g, member="m1", partitions=[2])
    L("group-java", ev="member_join_requested", group=g, member="m4")
    L("group-java", ev="assigned", group=g, member="m4", partitions=[3])
    for s, key, p, o in produced[12:]:
        m = {0: "m1", 1: "m2", 2: "m1", 3: "m4"}[p]
        L("group-java", ev="consumed", group=g, member=m, topic="orders", p=p, o=o, key=key, src_app="java", seq=s)
    # eos: txn1 committed (seq 0-11), txn2 aborted (seq 12-23), txn3 committed (seq 12-23)
    L("eos-java", ev="txn_committed", app="java", txn=1, records=12, srcs=[f"java:{s}" for s in range(12)])
    L("eos-java", ev="txn_aborted", app="java", txn=2, records=12, sent_ok=12, srcs=[f"java:{s}" for s in range(12, 24)])
    L("eos-java", ev="txn_committed", app="java", txn=3, records=12, srcs=[f"java:{s}" for s in range(12, 24)])
    # readback
    L("readback-java", ev="rb_begin", topic="orders", iso="read_uncommitted", partitions=4, end_total=24)
    L("readback-java", ev="rb_begin", topic="orders-enriched", iso="read_committed", partitions=1, end_total=36)
    for s, key, p, o in produced:
        L("readback-java", ev="rb", topic="orders", iso="read_uncommitted", p=p, o=o, key=key, app="java", seq=s)
    L("readback-java", ev="rb_end", topic="orders", iso="read_uncommitted", records=24, complete=True)
    for s in range(24):
        L("readback-java", ev="rb", topic="orders-enriched", iso="read_committed", p=0, o=s, key="k", app="java",
          src_app="java", src_seq=s, txn=1 if s < 12 else 3)
    L("readback-java", ev="rb_end", topic="orders-enriched", iso="read_committed", records=24, complete=True)
    L("readback-java", ev="rb_end", topic="orders-enriched", iso="read_uncommitted", records=36, complete=True)
    for p in range(4):
        L("lag-java", ev="lag", group=g, topic="orders", p=p, committed=6, end=6)
    for app in ("java",):
        for cmd in ("admin", "produce", "group", "eos", "readback", "lag"):
            L(f"{cmd}-{app}", ev="end", app=app, cmd=cmd, rc=0)
    return ev


SYNTHETIC_EXPECT = {"java": "4.3.1", "python": "2.15.1"}


def write(ev, d):
    files = collections.defaultdict(list)
    for e in ev:
        e = dict(e)
        f = e.pop("_f")
        files[f].append(e)
    for f, es in files.items():
        with open(os.path.join(d, f + ".jsonl"), "w", encoding="utf-8") as fh:
            for e in es:
                fh.write(json.dumps(e) + "\n")


def self_test():
    """Rule 1 of the harness doctrine: prove each check goes RED, naming what it caught."""
    def run(ev, want=SYNTHETIC_EXPECT):
        with tempfile.TemporaryDirectory() as d:
            write(ev, d)
            return verify(load(d), expect=want)

    failures = []
    cases = [1]  # the clean run

    def expect(name, ev, check, status, must_name=None, want=SYNTHETIC_EXPECT):
        cases[0] += 1
        rep = run(ev, want)
        rows = [row for row in rep.rows if row[1] == check and row[2] == status]
        ok = bool(rows) and (must_name is None or any(must_name in str(i) for row in rows for i in row[4]))
        print(f"  {'ok ' if ok else 'BAD'} {name}: expected {check} = {status}" + (f" naming {must_name}" if must_name else ""))
        if not ok:
            failures.append(name)
            for row in rep.rows:
                if row[2] in (FAIL, VOID):
                    print(f"        got {row[1]} = {row[2]} {row[4]}")

    base = synthetic()
    rep = run(base)
    print(f"  {'ok ' if rep.verdict() == PASS else 'BAD'} clean run: verdict {rep.verdict()}")
    if rep.verdict() != PASS:
        failures.append("clean run")
        for row in rep.rows:
            if row[2] != PASS and row[2] != INFO:
                print("       ", row)

    def mutate(fn):
        ev = [dict(e) for e in base]
        fn(ev)
        return ev

    def drop_consumed(ev):  # the group never delivers seq 7
        ev[:] = [e for e in ev if not (e["ev"] == "consumed" and e["seq"] == 7)]
    expect("group loses a record", mutate(drop_consumed), "no acknowledged loss", FAIL, "java:7")

    def drop_log(ev):  # the log does not hold seq 5
        ev[:] = [e for e in ev if not (e["ev"] == "rb" and e.get("topic") == "orders" and e.get("seq") == 5)]
    expect("log loses a record", mutate(drop_log), "no acknowledged loss", FAIL, "java:5")

    def dup_log(ev):
        extra = next(e for e in ev if e["ev"] == "rb" and e.get("topic") == "orders" and e.get("seq") == 9)
        ev.append({**extra, "o": 99})
    expect("log stores a record twice", mutate(dup_log), "no duplicates in the log", FAIL, "java:9")

    def reorder(ev):  # seq 0 acknowledged at an offset after seq 4 on the same key (a real offset:
        for e in ev:   # -1 now means "acknowledged with no offset", a separate check below)
            if e["ev"] == "produced" and e["seq"] == 0:
                e["o"] = 99
    expect("producer order broken", mutate(reorder), "per-customer order", FAIL, "cust-0")

    def no_rebalance(ev):
        ev[:] = [e for e in ev if e["ev"] not in ("revoked", "member_leave_requested")]
    expect("group never rebalances", mutate(no_rebalance), "rebalance happened", VOID)

    def foreign(ev):  # m2 receives a record from p0, which it never held
        for e in ev:
            if e["ev"] == "consumed" and e["seq"] == 0:
                e["member"] = "m2"
    expect("record from an unowned partition", mutate(foreign), "only owners consume", FAIL, "m2 p0")

    def aborted_visible(ev):
        ev.append({"_f": "readback-java", "ev": "rb", "topic": "orders-enriched", "iso": "read_committed", "p": 0,
                   "o": 50, "key": "k", "app": "java", "src_app": "java", "src_seq": 13, "txn": 2})
    expect("aborted output visible", mutate(aborted_visible), "aborted output invisible", FAIL, "txn 2")

    def committed_twice(ev):
        ev.append({"_f": "readback-java", "ev": "rb", "topic": "orders-enriched", "iso": "read_committed", "p": 0,
                   "o": 51, "key": "k", "app": "java", "src_app": "java", "src_seq": 3, "txn": 1})
    expect("output committed twice", mutate(committed_twice), "processed exactly once", FAIL, "java:3")

    def no_abort(ev):
        ev[:] = [e for e in ev if e["ev"] != "txn_aborted"]
    expect("no transaction ever aborted", mutate(no_abort), "commits and aborts both happened", VOID)

    def lagging(ev):
        for e in ev:
            if e["ev"] == "lag" and e["p"] == 2:
                e["committed"] = 3
    expect("group ends behind", mutate(lagging), "final committed offsets", FAIL, "p2")

    def incomplete(ev):
        for e in ev:
            if e["ev"] == "rb_end" and e["topic"] == "orders":
                e["complete"] = False
    expect("readback incomplete", mutate(incomplete), "readback complete", VOID)

    def config_dropped(ev):
        ev.append({"_f": "admin-java", "ev": "admin", "app": "java", "op": "describe_config", "target": "orders",
                   "ok": True, "retention_ms": "86400000", "expect_retention_ms": "604800000", "attempts": 30})
    expect("config change silently dropped", mutate(config_dropped), "java describe_config orders", FAIL, None)

    def dirty(ev):
        ev.append({"_f": "admin-java", "ev": "admin", "app": "java", "op": "create_topic", "target": "orders",
                   "ok": True, "rf": 3, "error": "TopicExistsException: Topic 'orders' already exists."})
    expect("target not clean", mutate(dirty), "java create_topic orders", VOID)

    def txn_error_no_chaos(ev):
        ev.append({"_f": "eos-java", "ev": "txn_error", "app": "java", "txn": 9, "error": "ILLEGAL_GENERATION"})
    expect("transaction error with nothing injected", mutate(txn_error_no_chaos), "transaction errors", FAIL, "txn 9")

    def txn_error_under_chaos(ev):
        ev.append({"_f": "eos-java", "ev": "txn_error", "app": "java", "txn": 9, "error": "ILLEGAL_GENERATION"})
        ev.append({"_f": "chaos", "ev": "chaos", "cmd": "podman kill kmq-0", "rc": 0, "output": "killed kmq-0"})
    expect("transaction error under a real failure is not a failure", mutate(txn_error_under_chaos),
           "transaction errors", INFO, "txn 9")

    def phantom_txn(ev):
        for e in ev:
            if e["ev"] == "rb" and e.get("topic") == "orders-enriched" and e.get("src_seq") == 20:
                e["txn"] = 99
    expect("visible output from a transaction the app never ran", mutate(phantom_txn),
           "visible output traces to a commit", FAIL, "txn 99")

    def commit_in_doubt(ev):
        ev[:] = [e for e in ev if not (e["ev"] == "txn_committed" and e["txn"] == 3)]
        ev.append({"_f": "eos-java", "ev": "txn_error", "app": "java", "txn": 3, "error": "TimeoutException"})
        ev.append({"_f": "chaos", "ev": "chaos", "cmd": "podman kill kmq-0", "rc": 0, "output": "killed kmq-0"})
    expect("commit call failed but the broker committed it", mutate(commit_in_doubt),
           "visible output traces to a commit", PASS)

    def chaos_did_not_happen(ev):
        ev.append({"_f": "chaos", "ev": "chaos", "cmd": "podman kill kmq-0", "rc": 1, "output": "no leader found"})
    expect("disruption that never ran", mutate(chaos_did_not_happen), "disruption happened", VOID)

    def stamp(ev):  # the real ledgers' shape: every event carries its wall-clock time in ms
        for i, e in enumerate(ev):
            e["t"] = 1_000_000 + 10 * i
        return max(e["t"] for e in ev)

    def chaos_after_traffic(ev):  # the node died after the last send, delivery and transaction
        last = stamp(ev)
        ev.append({"_f": "chaos", "ev": "chaos", "t": last + 9_000, "cmd": "podman kill kmq-0", "rc": 0,
                   "output": "killed kmq-0"})
    expect("disruption that hit an idle cluster", mutate(chaos_after_traffic), "disruption landed under traffic", VOID)

    def chaos_mid_traffic(ev):  # positive control: killed while the group was still delivering
        stamp(ev)
        mid = next(e["t"] for e in ev if e["ev"] == "consumed" and e.get("seq") == 20)
        ev.append({"_f": "chaos", "ev": "chaos", "t": mid, "cmd": "podman kill kmq-0", "rc": 0,
                   "output": "killed kmq-0"})
    expect("disruption under traffic still passes", mutate(chaos_mid_traffic), "disruption landed under traffic", PASS)

    def acked_without_offset(ev):  # a retried idempotent batch answered DUPLICATE_SEQUENCE_NUMBER: the client says -1
        for e in ev:
            if e["ev"] == "produced" and e["seq"] in (5, 9):
                e["o"] = -1
    expect("send acknowledged with no offset", mutate(acked_without_offset), "acknowledgement carries its offset", FAIL,
           "java:5")
    expect("no offset is not a shared offset", mutate(acked_without_offset), "distinct offsets", PASS)
    expect("no offset is not a broken order", mutate(acked_without_offset), "per-customer order", PASS)

    def log_reordered(ev):  # the log holds cust-1's seq 5 after seq 9 (acknowledgements say otherwise)
        rb = {e["seq"]: e for e in ev if e["ev"] == "rb" and e.get("topic") == "orders"}
        rb[5]["o"], rb[9]["o"] = rb[9]["o"], rb[5]["o"]
    expect("customer order broken in the log", mutate(log_reordered), "per-customer order in the log", FAIL, "cust-1")

    def hanging(ev):
        ev.append({"_f": "readback-java", "ev": "stable_wait", "topic": "orders-enriched", "waited_ms": 150000,
                   "stable": False, "partitions_behind": [1, 5], "records_behind": 700, "deadline_ms": 150000})
    expect("transaction left hanging", mutate(hanging), "orders-enriched settles", FAIL, "p5")

    def killed(ev):
        ev[:] = [e for e in ev if not (e["ev"] == "end" and e["cmd"] == "group")]
    expect("process killed", mutate(killed), "java group", VOID)

    # ---- which client produced the result
    def stale_java_client(ev):  # one process ran an older kafka-clients than the run asked for
        for e in ev:
            if e["ev"] == "client_version" and e["app"] == "java" and e["cmd"] == "produce":
                e["version"] = "3.7.1"
    expect("a Java process loaded a different kafka-clients", mutate(stale_java_client),
           "java kafka-clients", FAIL, "java produce: loaded 3.7.1")

    expect("4.4.0-rc1 requested but 4.3.1 loaded", [dict(e) for e in base], "java kafka-clients", FAIL,
           "loaded 4.3.1", want={"java": "4.4.0-rc1", "python": "2.15.1"})

    def rc_loaded(ev):  # the RC jar reports the release it becomes: 4.4.0, built from the rc1 tag
        for e in ev:
            if e["ev"] == "client_version" and e["app"] == "java":
                e.update(version="4.4.0", jar="kafka-clients-4.4.0-rc1.jar", commit="812f9108eb7e2af6")
    expect("4.4.0-rc1 requested and the RC jar (4.4.0) loaded", mutate(rc_loaded), "java kafka-clients", PASS,
           want={"java": "4.4.0-rc1", "python": "2.15.1"})

    def other_440_build(ev):  # a 4.4.0 jar installed at the rc1 coordinate, but built from another commit
        for e in ev:
            if e["ev"] == "client_version" and e["app"] == "java":
                e.update(version="4.4.0", jar="kafka-clients-4.4.0-rc1.jar", commit="0123456789abcdef")
    expect("a 4.4.0 jar from another commit at the rc1 coordinate", mutate(other_440_build), "java kafka-clients",
           FAIL, "commit 0123456789abcdef", want={"java": "4.4.0-rc1", "python": "2.15.1"})

    def final_440_jar(ev):  # the final 4.4.0 release (or a snapshot) loaded where rc1 was asked for
        for e in ev:
            if e["ev"] == "client_version" and e["app"] == "java":
                e.update(version="4.4.0", jar="kafka-clients-4.4.0.jar", commit="812f9108eb7e2af6")
    expect("a kafka-clients-4.4.0.jar loaded where 4.4.0-rc1 was requested", mutate(final_440_jar),
           "java kafka-clients", FAIL, "loaded from kafka-clients-4.4.0.jar", want={"java": "4.4.0-rc1", "python": "2.15.1"})

    expect("a release candidate with no pinned commit cannot pass", mutate(rc_loaded), "java kafka-clients", VOID,
           want={"java": "4.4.0-rc9", "python": "2.15.1"})

    def stale_python_client(ev):
        for e in ev:
            if e["ev"] == "client_version" and e["app"] == "python":
                e["version"] = "2.6.1"
    expect("the Python process loaded a different confluent-kafka", mutate(stale_python_client),
           "python confluent-kafka", FAIL, "python version: loaded 2.6.1")

    def silent_client(ev):
        ev[:] = [e for e in ev if not (e["ev"] == "client_version" and e["cmd"] == "eos")]
    expect("a process never said which client it ran", mutate(silent_client), "java kafka-clients", VOID, "java eos")

    def mismatched_librdkafka(ev):  # confluent-kafka 2.15.1 over a different librdkafka
        for e in ev:
            if e["ev"] == "client_version" and e["app"] == "python":
                e["librdkafka"] = "2.14.0"
    expect("confluent-kafka 2.15.1 running over librdkafka 2.14.0", mutate(mismatched_librdkafka),
           "python confluent-kafka", FAIL, "librdkafka 2.14.0")
    expect("that pairing passes when --expect-librdkafka asks for it", mutate(mismatched_librdkafka),
           "python confluent-kafka", PASS, want={"java": "4.3.1", "python": "2.15.1", "librdkafka": "2.14.0"})

    expect("python processes ran but only the java version was requested", [dict(e) for e in base],
           "python confluent-kafka", VOID, "python version", want={"java": "4.3.1"})

    expect("the run never recorded which versions it asked for", [dict(e) for e in base],
           "requested versions recorded", VOID, want={})

    # ---- a phantom topic: the create was refused or ignored, Metadata invents 1 partition
    def phantom_describe(ev):
        for e in ev:
            if e["ev"] == "admin" and e["op"] == "describe_topic" and e["target"] == "orders":
                e["partitions"] = 1
    expect("described partition count differs from the request (phantom topic)", mutate(phantom_describe),
           "java describe_topic orders", FAIL, "orders: 1 != 4")

    def phantom_after_refused_create(ev):  # the released-v1.1.0 shape: RF>1 refused, then a 1-partition phantom
        for e in ev:
            if e["ev"] == "admin" and e["op"] == "create_topic" and e["target"] == "orders":
                e["ok"] = False
                e["error"] = "InvalidReplicationFactorException: replication factor must be 1"
            if e["ev"] == "admin" and e["op"] == "describe_topic" and e["target"] == "orders":
                e["partitions"] = 1
    expect("refused create followed by a phantom describe", mutate(phantom_after_refused_create),
           "java describe_topic orders", FAIL, "orders: 1 != 4")

    def phantom_reader(ev):
        for e in ev:
            if e["ev"] == "rb_begin" and e["topic"] == "orders":
                e["partitions"] = 1
    expect("fresh reader's Metadata shows a phantom partition count", mutate(phantom_reader),
           "`orders` partitions (read_uncommitted)", FAIL, "orders: 1 != 4")

    def no_request_recorded(ev):
        for e in ev:
            if e["ev"] == "admin" and e["op"] == "create_topic":
                e.pop("requested_partitions", None)
    expect("describe with no recorded request cannot pass", mutate(no_request_recorded),
           "java describe_topic orders", VOID)

    print(f"\nself-test: {'PASS' if not failures else 'FAIL'} ({cases[0] - len(failures)}/{cases[0]})")
    return 0 if not failures else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", nargs="?")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--title", default="")
    ap.add_argument("--expect-java-client", default="",
                    help="kafka-clients version the run requested (default: requested_kafka_clients in instrument.txt)")
    ap.add_argument("--expect-python-client", default="",
                    help="confluent-kafka version the run requested (default: requested_confluent_kafka in instrument.txt)")
    ap.add_argument("--expect-java-commit", default="",
                    help="git commit (16+ hex) the kafka-clients jar must report (default: requested_kafka_clients_commit "
                         "in instrument.txt, else the PINNED_COMMITS entry for the requested version)")
    ap.add_argument("--expect-librdkafka", default="",
                    help="librdkafka version the Python client must load (default: requested_librdkafka in instrument.txt, "
                         "else the requested confluent-kafka version, which 2.x wheels bundle)")
    ap.add_argument("--pinned-commit", default="", metavar="VERSION",
                    help="print the pinned kafka-clients commit for VERSION (empty when none) and exit")
    args = ap.parse_args()
    if args.pinned_commit:
        print(PINNED_COMMITS.get(args.pinned_commit, ""))
        sys.exit(0)
    if args.self_test:
        sys.exit(self_test())
    if not args.run_dir:
        ap.error("run_dir is required")
    want = read_expectations(args.run_dir)
    if args.expect_java_client:
        want["java"] = args.expect_java_client
    if args.expect_python_client:
        want["python"] = args.expect_python_client
    if args.expect_java_commit:
        want["java_commit"] = args.expect_java_commit
    if args.expect_librdkafka:
        want["librdkafka"] = args.expect_librdkafka
    rep = verify(load(args.run_dir), expect=want)
    md = rep.render(args.title or f"Kafka application simulator — {os.path.basename(os.path.abspath(args.run_dir))}")
    with open(os.path.join(args.run_dir, "report.md"), "w", encoding="utf-8") as f:
        f.write(md)
    print(md)
    sys.exit({PASS: 0, FAIL: 1, VOID: 2}[rep.verdict()])


if __name__ == "__main__":
    main()
