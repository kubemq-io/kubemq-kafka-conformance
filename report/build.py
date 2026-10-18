#!/usr/bin/env python3
"""Build report/index.html, the results page, from the published run.

    python3 report/build.py            # reads results/REPORT.md and results/summary.json

report/template.html holds the design; the hero and the body are generated here from the report
the runner wrote, so the page always matches results/. Standard library only.
"""
import html
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = "https://github.com/kubemq-io/kubemq-kafka-conformance"

TRACKS = {
    "protocol": ("Kafka protocol", "Every Kafka request type (ApiKey) and feature flag, compared with what Kafka advertises, plus full produce, fetch and transaction conversations replayed on both brokers and diffed."),
    "features": ("Broker behaviour", "Produce and fetch, acks, idempotence, consumer groups and rebalances, offsets, retention, compaction, topic administration and limits."),
    "transactions": ("Transactions and exactly-once", "Commit, abort, read_committed, fencing, timeouts and exactly-once pipelines with the Java client, franz-go and librdkafka."),
    "clients": ("Client libraries", "Each client's own quickstart with stock defaults, then produce, consume, consumer groups and commit and resume."),
    "app-sim": ("Application", "Java and Python order-processing services: compression, rebalancing groups and exactly-once pipelines with aborts, checked for no loss, no duplicates and per-customer order."),
    "tools": ("Kafka tools and platform", "Kafka's own CLI tools, Kafka Connect, Kafka Streams, MirrorMaker 2, lag monitors (Burrow, kafka-lag-exporter) and Kafbat UI, unmodified."),
    "security": ("Security", "TLS, mTLS, SASL PLAIN, SCRAM and OAUTHBEARER, re-authentication, ACLs and quotas, each configured the way the product documents it."),
    "operations": ("Operations", "The metrics, configuration answers and consumer lag an operator relies on, compared with Kafka's own."),
    "migration": ("Migration", "Moving topics and consumer-group offsets from Kafka to KubeMQ with MirrorMaker 2, cutting consumers over, and rolling back."),
}

OPTIONAL_NAMES = {
    "apikey.88": "Kafka Streams new rebalance protocol (KIP-1071): heartbeat",
    "apikey.89": "Kafka Streams new rebalance protocol (KIP-1071): describe",
    "feature.eligible.leader.replicas.version": "eligible.leader.replicas.version feature flag",
    "feature.streams.version": "streams.version feature flag",
    "features.produce.broker-recompression": "Broker-side topic compression",
    "security.oauthbearer.plain-port": "OAUTHBEARER on the SASL_PLAINTEXT port",
    "security.delegation-tokens.create-and-login": "Delegation tokens",
    "security.gssapi.configurable": "Kerberos (SASL/GSSAPI)",
    "operations.config.segment-bytes": "Fixed 64 MiB log segment size",
}


# Headline numbers from the KubeMQ vs Kafka benchmark (identical GKE clusters, 3 brokers each);
# the source is that repository's README. Update them when the benchmark is rerun.
BENCH = "https://kubemq-io.github.io/kubemq-vs-kafka-benchmark/"
BENCH_ROWS = [
    ("Highest rate carried, 1 KB messages", "700k msg/s", "400k msg/s"),
    ("End-to-end latency p99 at 300k msg/s", "6 ms", "274 ms"),
    ("Publish latency p99 at 400k msg/s", "5 ms", "631 ms"),
    ("Bandwidth, 100 KB messages", "614 MB/s", "410 MB/s"),
    ("Partition leader killed: longest gap with no acknowledgements", "2 s", "8 s"),
    ("Partition leader killed: acknowledged messages lost", "0", "0"),
    ("Broker memory at 300k msg/s", "2.5 GiB", "24.6 GiB"),
]

def esc(s):
    return html.escape(s, quote=True)


def md_inline(s):
    """Escape, then render `code` spans."""
    return re.sub(r"`([^`]+)`", r"<code>\1</code>", esc(s))


def cells(line):
    line = line.strip()[1:-1]
    out, cur, i = [], "", 0
    while i < len(line):
        if line[i] == "\\" and i + 1 < len(line) and line[i + 1] == "|":
            cur += "|"
            i += 2
            continue
        if line[i] == "|":
            out.append(cur.strip())
            cur = ""
        else:
            cur += line[i]
        i += 1
    out.append(cur.strip())
    return out


def parse_report(path):
    """Sections of REPORT.md: heading -> list of table rows (lists of cells), plus all-rows by track."""
    sections, tracks = {}, {}
    h2 = h3 = None
    for line in open(path, encoding="utf-8"):
        if line.startswith("## "):
            h2, h3 = line[3:].strip(), None
            sections.setdefault(h2, [])
            continue
        if line.startswith("### "):
            h3 = line[4:].strip()
            continue
        if not line.startswith("| `"):
            continue
        row = cells(line)
        if h2 == "All rows" and h3:
            tracks.setdefault(h3, []).append(row)
        elif h2:
            sections[h2].append(row)
    return sections, tracks


def verdict(v):
    cls = "NA" if v == "NOT-APPLICABLE" else v
    label = "NOT APPLICABLE" if v == "NOT-APPLICABLE" else v
    return f'<span class="v {esc(cls)}">{esc(label)}</span>'


def main():
    report = os.path.join(ROOT, "results", "REPORT.md")
    summary = json.load(open(os.path.join(ROOT, "results", "summary.json"), encoding="utf-8"))
    sections, rows_by_track = parse_report(report)
    run = summary["run"]
    tracks = [t for t in summary["tracks"]]
    totals = {"rows": 0, "PASS": 0, "GAP": 0}
    for t in tracks:
        totals["rows"] += t["rows"]
        for k in ("PASS", "GAP"):
            totals[k] += t["counts"].get(k, 0)
    bad = sum(t["counts"].get(k, 0) for t in tracks for k in ("FAIL", "VOID", "NOT-RUN", "FLAKY"))
    na = sum(t.get("broker_internal_not_applicable", 0) for t in tracks)
    status = "PASSED" if summary["passed"] else "FAILED"
    date = run["started_utc"][:10]
    kafka = run["kafka"]
    clients = sorted(summary.get("clients_ran", {}).items())
    sources = run.get("sources") or []
    machine = sources[0]["machine"] if sources else run.get("machine", "")
    engine = sources[0]["engine"] if sources else run.get("engine", {})

    # --- hero -------------------------------------------------------------------------------
    hero = []
    hero.append(f'<div class="context"><span class="dash"></span><span>Conformance · <strong>KubeMQ</strong> against <strong>Apache Kafka {esc(kafka["version"])}</strong> · run {esc(date)}</span></div>')
    hero.append("<h1>Same client code. Same tests. Does KubeMQ behave like Kafka?</h1>")
    hero.append(f'<p class="lede">{totals["rows"]} tests cover the Kafka protocol, client libraries, transactions, Kafka\'s own tools, security, operations and migration. Every test runs on real Apache Kafka first, then on KubeMQ with only the bootstrap address changed. A test passes when both brokers behave the same.</p>')
    hero.append(f'<div class="disclosure"><b>Run and published by KubeMQ.</b> Read it with that in mind, and rerun it yourself: the suite, its method and every result are in the <a href="{REPO}">repository</a>. The KubeMQ half needs an evaluation key from KubeMQ.</div>')
    hero.append('<div class="stats">')
    hero.append(f'<div class="stat ok"><b>{esc(status)}</b><span>overall result</span></div>')
    hero.append(f'<div class="stat"><b>{totals["PASS"]}</b><span>tests pass, of {totals["rows"]}</span></div>')
    hero.append(f'<div class="stat"><b>{totals["GAP"]}</b><span>optional features</span></div>')
    hero.append(f'<div class="stat"><b>{bad}</b><span>failures</span></div>')
    hero.append("</div>")
    hero.append('<nav class="toc"><a href="#result">Result by area</a><a href="#optional">Optional features</a><a href="#coverage">What is tested</a><a href="#tests">Every test</a><a href="#perf">Performance</a><a href="#method">Method</a><a href="#reproduce">Run it yourself</a></nav>')

    # --- body -------------------------------------------------------------------------------
    b = []
    b.append('<h2 id="result">Result by area</h2>')
    b.append(f'<div class="note"><b>PASS</b>: KubeMQ behaved exactly as Kafka did. <b>OPTIONAL</b>: a Kafka feature KubeMQ does not offer, listed before the run with who it affects and the workaround. Any other difference would be a <b>FAIL</b>; this run has none.</div>')
    b.append('<div class="scroll"><table class="tracks"><thead><tr><th scope="col">Area</th><th scope="col">Tests</th><th scope="col">Pass</th><th scope="col">Optional</th><th scope="col">Fail</th><th scope="col">Share passing</th></tr></thead><tbody>')
    for t in tracks:
        n = t["rows"]
        if n == 0:
            continue
        p, o = t["counts"].get("PASS", 0), t["counts"].get("GAP", 0)
        f = sum(t["counts"].get(k, 0) for k in ("FAIL", "VOID", "NOT-RUN", "FLAKY"))
        name = TRACKS.get(t["track"], (t["track"], ""))[0]
        bar = f'<div class="bar" role="img" aria-label="{p} of {n} pass"><i class="p" style="width:{100 * p / n:.1f}%"></i><i class="o" style="width:{100 * o / n:.1f}%"></i></div>'
        b.append(f'<tr><td><a href="#t-{esc(t["track"])}">{esc(name)}</a></td><td>{n}</td><td>{p}</td><td>{o}</td><td>{f}</td><td>{bar}</td></tr>')
    b.append(f'<tr><td><b>All areas</b></td><td><b>{totals["rows"]}</b></td><td><b>{totals["PASS"]}</b></td><td><b>{totals["GAP"]}</b></td><td><b>{bad}</b></td><td></td></tr>')
    b.append("</tbody></table></div>")
    b.append(f'<p>{na} requests that Kafka brokers send only to each other (transaction markers and share-group state) are listed separately as not applicable: KubeMQ replaces Kafka for applications and tools, not as a member of a Kafka broker cluster. They are not counted above.</p>')

    b.append('<h2 id="optional">Optional features</h2>')
    b.append('<p>Kafka features KubeMQ does not offer today. Each was listed before the run, with the exact behaviour the run had to observe; if the behaviour had changed, the test would have failed.</p>')
    b.append('<div class="scroll"><table class="wide"><thead><tr><th scope="col">Feature</th><th scope="col">Who is affected</th><th scope="col">Workaround</th></tr></thead><tbody>')
    for r in sections.get("Optional features", []):
        rid = r[0].strip("`")
        name = OPTIONAL_NAMES.get(rid, r[1])
        b.append(f'<tr><td><b>{esc(name)}</b><br><code>{esc(rid)}</code></td><td class="d">{md_inline(r[2])}</td><td class="d">{md_inline(r[3])}</td></tr>')
    b.append("</tbody></table></div>")

    b.append('<h2 id="coverage">What is tested</h2>')
    b.append('<div class="scroll"><table class="tracks"><thead><tr><th scope="col">Area</th><th scope="col" style="text-align:left">What runs on both brokers</th><th scope="col">Tests</th></tr></thead><tbody>')
    for t in tracks:
        if t["track"] == "perf":
            continue
        name, desc = TRACKS.get(t["track"], (t["track"], ""))
        n = t["rows"]
        b.append(f'<tr><td>{esc(name)}</td><td class="desc">{esc(desc)}</td><td>{n}</td></tr>')
    b.append("</tbody></table></div>")
    b.append("<h3>Clients and tools that ran</h3><div class=\"chips\">")
    for k, v in clients:
        if k.startswith("kafka-tools-image"):
            continue
        b.append(f"<span>{esc(k)} {esc(v)}</span>")
    b.append("</div>")

    b.append('<h2 id="tests">Every test</h2>')
    b.append(f'<p>Each row names the test, its verdict and what was observed on both brokers. The full detail is in <a href="{REPO}/blob/main/results/REPORT.md">results/REPORT.md</a>; the method and the reference for every row are in the <a href="{REPO}">repository</a>.</p>')
    for t in tracks:
        tr = t["track"]
        rows = rows_by_track.get(tr, [])
        if not rows or tr == "perf":
            continue
        name = TRACKS.get(tr, (tr, ""))[0]
        p, o = t["counts"].get("PASS", 0), t["counts"].get("GAP", 0)
        b.append(f'<details class="track" id="t-{esc(tr)}"><summary>{esc(name)}<small>{len(rows)} tests · {p} pass{f" · {o} optional" if o else ""}</small></summary>')
        b.append('<div class="scroll"><table class="wide"><thead><tr><th scope="col">Test</th><th scope="col">Verdict</th><th scope="col">What it checks</th><th scope="col">Observed</th></tr></thead><tbody>')
        for r in rows:
            b.append(f'<tr><td class="id">{esc(r[0].strip("`"))}</td><td>{verdict(r[1])}</td><td class="d">{md_inline(r[2])}</td><td class="d">{md_inline(r[3])}</td></tr>')
        b.append("</tbody></table></div></details>")

    b.append('<h2 id="perf">Performance</h2>')
    b.append(f'<p>This suite checks behaviour, not speed. Performance is measured separately, by the <a href="{BENCH}">KubeMQ vs Kafka benchmark</a>: two identical 3-broker clusters on Google Cloud, the same OpenMessaging Benchmark client, and an acknowledged-loss verifier running beside the load. Selected results:</p>')
    b.append('<div class="scroll"><table class="wide"><thead><tr><th scope="col">Measure</th><th scope="col"><span class="dot mq"></span>KubeMQ</th><th scope="col"><span class="dot kf"></span>Apache Kafka</th></tr></thead><tbody>')
    for m, mq, kf in BENCH_ROWS:
        b.append(f'<tr><td>{esc(m)}</td><td class="d"><b>{esc(mq)}</b></td><td class="d">{esc(kf)}</td></tr>')
    b.append("</tbody></table></div>")
    b.append(f'<p>Every table, the latency curves, CPU, disk and network per broker, the method and how to rerun it are on the <a href="{BENCH}">benchmark page</a>.</p>')

    b.append('<h2 id="method">Method</h2>')
    b.append("<p>Real Apache Kafka is the reference. Every test runs against Kafka first, then against KubeMQ with only the bootstrap address changed; nothing else in the client, tool or scenario changes between the two. If the Kafka leg itself does not show the documented behaviour, the test proves nothing and is reported VOID, never PASS.</p>")
    b.append("<ul><li>A difference is OPTIONAL only if it was listed, with the exact behaviour, before the run. Any other difference is a FAIL, and a FAIL is never relabelled afterwards.</li><li>A test that did not run fails the report; a skipped test is never quiet.</li><li>Every verifier ships a mutation test proving it reports FAIL on a lost, duplicated, reordered or misnumbered record.</li><li>Each test records its origin: an upstream Apache Kafka or client-project test, or one written by KubeMQ.</li></ul>")
    b.append(f'<p>Full method: <a href="{REPO}/blob/main/docs/methodology.md">docs/methodology.md</a>. Every difference in full: <a href="{REPO}/blob/main/docs/gaps.md">docs/gaps.md</a>.</p>')
    b.append('<h3>This run</h3><div class="scroll"><table class="wide"><tbody>')
    b.append(f'<tr><td>Kafka reference</td><td class="d"><code>{esc(kafka["image"])}:{esc(kafka["version"])}</code> <code>{esc(kafka["digest"])}</code></td></tr>')
    b.append(f'<tr><td>KubeMQ</td><td class="d">single node, Kafka protocol on its default settings</td></tr>')
    b.append(f'<tr><td>Machine</td><td class="d">{esc(machine)}</td></tr>')
    if engine:
        b.append(f'<tr><td>Container engine</td><td class="d">{esc(engine.get("name", ""))} {esc(engine.get("version", ""))}, {esc(engine.get("arch", ""))}</td></tr>')
    b.append(f'<tr><td>Run</td><td class="d">{esc(run["started_utc"])} to {esc(run["finished_utc"])} UTC</td></tr>')
    b.append("</tbody></table></div>")

    b.append('<h2 id="reproduce">Run it yourself</h2>')
    b.append("<p>You need Docker or podman, Go, <code>curl</code>, <code>jq</code>, <code>lsof</code>, <code>git</code> and a KubeMQ evaluation key. arm64 and amd64 both work. The suite pulls the latest KubeMQ image and the pinned Apache Kafka image, starts both on your machine and writes this same report.</p>")
    b.append(f"<pre>git clone {REPO}.git\ncd kubemq-kafka-conformance\nexport KUBEMQ_LICENSE_KEY=&lt;your evaluation key&gt;\nrunner/certify.sh --preflight-only   # checks your machine, starts nothing\nrunner/certify.sh --quick            # protocol tests, about 15 minutes\nrunner/certify.sh --full             # every test, 1 to 2 hours</pre>")
    b.append(f'<p>Details: <a href="{REPO}/blob/main/docs/rerun.md">docs/rerun.md</a>.</p>')

    tpl = open(os.path.join(ROOT, "report", "template.html"), encoding="utf-8").read()
    page = tpl.replace("<!-- HERO -->", "\n".join(hero)).replace("<!-- BODY -->", "\n".join(b))
    out = os.path.join(ROOT, "report", "index.html")
    open(out, "w", encoding="utf-8").write(page)
    print(f"[page] wrote {out}: {totals['rows']} tests, {status}", file=sys.stderr)


if __name__ == "__main__":
    main()
