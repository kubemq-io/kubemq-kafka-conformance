package report

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// Public renders the report published in the public repository: KubeMQ without a version or
// image, and no operator, command, repository commit or raw-log lines.
var Public bool

// label is how a verdict is shown. A registered GAP is a Kafka feature KubeMQ does not offer:
// it reads OPTIONAL. The results files and summary.json keep the verdict as written.
func label(v string) string {
	if v == Gap {
		return "OPTIONAL"
	}
	return v
}

// signOffText matches the sign-off clause the tracks append to a registered row's detail.
var signOffText = regexp.MustCompile(`\.?\s*(owner-approved scope exclusion|pre-registered gap), signed off(:| by) .*?\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?Z)?\.?`)

// BenchmarkURL is where the public report points for performance numbers.
const BenchmarkURL = "https://kubemq-io.github.io/kubemq-vs-kafka-benchmark/"

// publicMeasurement replaces a measurement's values in the public report: one machine's
// numbers are not KubeMQ's published performance, which the benchmark measures on identical
// cluster hardware. A rerun's own report still shows its values.
const publicMeasurement = "measured (values not published here; a rerun shows your own). Published performance: " + BenchmarkURL

// shownDetail is a row's detail as the report shows it: the sign-off clause and note left out
// (docs/gaps.md holds them), every other note kept.
func shownDetail(row Row) string {
	if Public && row.Kind == Measurement && row.Verdict == Pass {
		return publicMeasurement
	}
	d := row.Detail
	if row.Verdict == Gap || row.Verdict == NotApplicable {
		d = strings.TrimSpace(signOffText.ReplaceAllString(d, ""))
	}
	var notes []string
	for _, n := range row.Notes {
		if !strings.HasPrefix(n, signedPrefix) {
			notes = append(notes, n)
		}
	}
	if len(notes) > 0 {
		d += " (" + strings.Join(notes, "; ") + ")"
	}
	return d
}

// optionalTable lists accepted GAP rows with who is affected and the workaround, from the
// register.
func optionalTable(rows []Row) string {
	var b strings.Builder
	b.WriteString("| Row | What | Who is affected | Workaround |\n|---|---|---|---|\n")
	for _, row := range rows {
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s |\n", row.ID, cell(row.Title, 160), orUnknown(cell(row.Impact, 300)), orUnknown(cell(row.Workaround, 200)))
	}
	return b.String()
}

func kubemqName(r *Report) string {
	if Public {
		return "KubeMQ"
	}
	return "KubeMQ " + r.Run.KubeMQ.Version
}

const limitsBlock = `## Read before evaluating

- **Replication factor cannot exceed the node count.** Every topic has one copy per node; a higher replication factor is refused with ` + "`INVALID_REPLICATION_FACTOR`" + `.
- **1 MiB default maximum record size**, as in Kafka. Larger records need the topic and client limits raised, exactly as on Kafka.
`

func status(r *Report) string {
	if r.Passed {
		return "PASSED"
	}
	return "FAILED"
}

func header(r *Report) string {
	if len(r.Run.Sources) > 0 {
		return mergedHeader(r)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "| | |\n|---|---|\n")
	fmt.Fprintf(&b, "| Status | **%s** |\n", status(r))
	fmt.Fprintf(&b, "| Run | %s to %s UTC, mode `%s` |\n", r.Run.StartedUTC, r.Run.FinishedUTC, r.Run.Mode)
	kafkaTag := r.Run.Kafka.Version
	if r.Run.KafkaPreview != "" {
		kafkaTag = r.Run.KafkaPreview + " (release candidate)"
	}
	fmt.Fprintf(&b, "| Kafka oracle | `%s:%s` `%s` |\n", r.Run.Kafka.Image, kafkaTag, r.Run.Kafka.Digest)
	if !Public {
		fmt.Fprintf(&b, "| KubeMQ | `%s:%s` `%s` |\n", r.Run.KubeMQ.Image, r.Run.KubeMQ.Version, r.Run.KubeMQ.Digest)
	}
	var clients []string
	for _, k := range sortedKeys(r.Clients) {
		clients = append(clients, fmt.Sprintf("%s %s", k, r.Clients[k]))
	}
	if len(clients) == 0 {
		clients = []string{"none recorded"}
	}
	fmt.Fprintf(&b, "| Clients that ran | %s |\n", strings.Join(clients, ", "))
	fmt.Fprintf(&b, "| Engine | %s %s (runtime %s), %s, host %s |\n", r.Run.Engine.Name, r.Run.Engine.Version, r.Run.Engine.Runtime, r.Run.Engine.Arch, r.Run.Engine.HostOS)
	if Public {
		fmt.Fprintf(&b, "| Machine | %s |\n", orUnknown(cell(r.Run.Machine, 0)))
		b.WriteString(amendmentsBlock(r))
		return b.String()
	}
	fmt.Fprintf(&b, "| Operator | %s |\n", orUnknown(cell(r.Run.Operator, 0)))
	fmt.Fprintf(&b, "| Machine | %s |\n", orUnknown(cell(r.Run.Machine, 0)))
	fmt.Fprintf(&b, "| Command | `%s` |\n", orUnknown(strings.ReplaceAll(r.Run.Command, "|", "\\|")))
	commit := orUnknown(r.Run.Repo.Commit)
	if r.Run.Repo.Dirty {
		commit += " (with uncommitted changes)"
	}
	fmt.Fprintf(&b, "| Repository commit | `%s` |\n", commit)
	logs := "not recorded"
	if r.Run.RawLogs.SHA256 != "" {
		logs = fmt.Sprintf("`%s` `%s` (published outside git, see docs/release-cert.md)", r.Run.RawLogs.File, r.Run.RawLogs.SHA256)
	}
	fmt.Fprintf(&b, "| Raw logs | %s |\n", logs)
	b.WriteString(amendmentsBlock(r))
	return b.String()
}

// mergedHeader is the header of a release report merged from several runs: what they share, then
// one line per run saying which tracks it ran, where, by whom, from which commit, and its logs.
func mergedHeader(r *Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "| | |\n|---|---|\n")
	fmt.Fprintf(&b, "| Status | **%s** |\n", status(r))
	fmt.Fprintf(&b, "| Runs | %d runs, %s to %s UTC; one line per run below |\n", len(r.Run.Sources), r.Run.StartedUTC, r.Run.FinishedUTC)
	kafkaTag := r.Run.Kafka.Version
	if r.Run.KafkaPreview != "" {
		kafkaTag = r.Run.KafkaPreview + " (release candidate)"
	}
	fmt.Fprintf(&b, "| Kafka oracle | `%s:%s` `%s` (every run) |\n", r.Run.Kafka.Image, kafkaTag, r.Run.Kafka.Digest)
	if !Public {
		fmt.Fprintf(&b, "| KubeMQ | `%s:%s` `%s` (every run) |\n", r.Run.KubeMQ.Image, r.Run.KubeMQ.Version, r.Run.KubeMQ.Digest)
	}
	var clients []string
	for _, k := range sortedKeys(r.Clients) {
		clients = append(clients, fmt.Sprintf("%s %s", k, r.Clients[k]))
	}
	if len(clients) == 0 {
		clients = []string{"none recorded"}
	}
	fmt.Fprintf(&b, "| Clients that ran | %s |\n", strings.Join(clients, ", "))
	if Public {
		b.WriteString("\n| Tracks | Machine | Engine | Started to finished (UTC) |\n|---|---|---|---|\n")
		for _, s := range r.Run.Sources {
			fmt.Fprintf(&b, "| %s | %s | %s %s (runtime %s), %s | %s to %s |\n", strings.Join(s.Tracks, ", "),
				orUnknown(cell(s.Machine, 0)), s.Engine.Name, s.Engine.Version, s.Engine.Runtime, s.Engine.Arch, s.StartedUTC, s.FinishedUTC)
		}
		b.WriteString(amendmentsBlock(r))
		return b.String()
	}
	commit := "`" + orUnknown(r.Run.Repo.Commit) + "` (every run)"
	if r.Run.MixedCommits {
		commit = "**different commits per run** (merged with --allow-mixed-commits; see the commit of each run below)"
	}
	if r.Run.Repo.Dirty {
		commit += "; **at least one run had uncommitted changes**"
	}
	fmt.Fprintf(&b, "| Repository commit | %s |\n", commit)
	if r.Run.FinalCommit != "" {
		fmt.Fprintf(&b, "| Release report | final: every run used kit commit `%s`, clean, for this released KubeMQ (merge --final) |\n", r.Run.FinalCommit)
	}
	b.WriteString("\n| Tracks | Machine | Engine | Operator | Started to finished (UTC) | Commit | Command | Raw logs |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	for _, s := range r.Run.Sources {
		c := orUnknown(s.Repo.Commit)
		if s.Repo.Dirty {
			c += " (with uncommitted changes)"
		}
		logs := "not recorded"
		if s.RawLogs.SHA256 != "" {
			logs = fmt.Sprintf("`%s` `%s`", s.RawLogs.File, s.RawLogs.SHA256)
		}
		fmt.Fprintf(&b, "| %s | %s | %s %s (runtime %s), %s | %s | %s to %s | `%s` | `%s` | %s |\n",
			sourceTracks(s), orUnknown(cell(s.Machine, 0)), s.Engine.Name, s.Engine.Version, s.Engine.Runtime, s.Engine.Arch,
			orUnknown(cell(s.Operator, 0)), s.StartedUTC, s.FinishedUTC, c, orUnknown(strings.ReplaceAll(s.Command, "|", "\\|")), logs)
	}
	b.WriteString("\nRaw logs are published outside git, one tarball per run (see docs/release-cert.md).\n")
	b.WriteString(amendmentsBlock(r))
	return b.String()
}

// amendmentsBlock lists every verdict a person reclassified after the run, so no amendment is
// ever invisible in the report.
func amendmentsBlock(r *Report) string {
	type row struct {
		run string
		a   Amendment
	}
	var all []row
	for _, a := range r.Run.Amendments {
		all = append(all, row{"this run", a})
	}
	for _, s := range r.Run.Sources {
		for _, a := range s.Amendments {
			all = append(all, row{s.Label, a})
		}
	}
	if len(all) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\n**%d verdicts were amended after the run** (the as-run results are in that run's raw-logs tarball):\n\n", len(all))
	b.WriteString("| Row | Run | As run | Amended to | Cause | Decided by | When (UTC) |\n|---|---|---|---|---|---|---|\n")
	for _, x := range all {
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s | %s | %s |\n", x.a.Row, x.run, x.a.From, x.a.To, cell(x.a.Reason, 240), cell(x.a.DecidedBy, 0), x.a.Time)
	}
	return b.String()
}

// sourceTracks: the tracks a run contributes, then any it ran whose results a later run replaced.
func sourceTracks(s RunSource) string {
	t := strings.Join(s.Tracks, ", ")
	if len(s.Superseded) > 0 {
		t += " (" + strings.Join(s.Superseded, ", ") + ": superseded by a later run)"
	}
	return t
}

func summaryTable(r *Report) string {
	var b strings.Builder
	b.WriteString("| Track | Rows | PASS | OPTIONAL | FAIL | VOID | NOT-RUN | FLAKY | Upstream / KubeMQ-authored | Result |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|\n")
	for _, t := range r.Tracks {
		res := "passed"
		if !t.Passed {
			res = "**failed**"
		}
		void := fmt.Sprint(t.Counts[Void])
		if t.VoidOver {
			void = fmt.Sprintf("**%d (over budget)**", t.Counts[Void])
		} else if len(t.VoidNoNote) > 0 {
			void = fmt.Sprintf("**%d (no root cause)**", t.Counts[Void])
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %s | %s | %s | %d | %d / %d | %s |\n",
			t.Track, t.Rows, t.Counts[Pass], t.Counts[Gap], bold(t.Counts[Fail]), void, bold(t.Counts[NotRun]),
			t.Counts[Flaky], t.Upstream, t.KubeMQOwned, res)
	}
	return b.String()
}

func bold(n int) string {
	if n > 0 {
		return fmt.Sprintf("**%d**", n)
	}
	return "0"
}

func rowsWith(r *Report, verdicts ...string) []Row {
	want := map[string]bool{}
	for _, v := range verdicts {
		want[v] = true
	}
	var out []Row
	for _, t := range r.Order {
		for _, row := range r.RowsByTr[t] {
			if want[row.Verdict] && row.Kind != Measurement {
				out = append(out, row)
			}
		}
	}
	return out
}

func rowTable(rows []Row, limit int) string {
	var b strings.Builder
	b.WriteString("| Row | Verdict | What | Detail |\n|---|---|---|---|\n")
	for _, row := range rows {
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s |\n", row.ID, label(row.Verdict), cell(row.Title, 160), cell(shownDetail(row), limit))
	}
	return b.String()
}

func cell(s string, limit int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.Join(strings.Fields(s), " ")
	if limit > 0 && len(s) > limit {
		s = s[:limit] + " ..."
	}
	return s
}

// ReportMarkdown is reports/<version>/REPORT.md.
func ReportMarkdown(r *Report) string {
	var b strings.Builder
	if r.Run.KafkaPreview != "" {
		fmt.Fprintf(&b, "# Preview report: KubeMQ %s against Apache Kafka %s (release candidate)\n\n", r.Run.KubeMQ.Version, r.Run.KafkaPreview)
		b.WriteString("**Preview, not a certification.** The oracle is a Kafka release candidate. This report never changes `COMPATIBILITY.md` or a certified verdict; it shows what the next Kafka release would change.\n\n")
	} else {
		fmt.Fprintf(&b, "# Certification report: %s\n\n", kubemqName(r))
	}
	if Public {
		b.WriteString("Generated by `runner/report`. Every row ran on Apache Kafka first, then on KubeMQ with the same client code.\n\n")
	} else {
		b.WriteString("Generated by `runner/report`. Every run is published, including failing ones.\n\n")
	}
	b.WriteString(header(r))
	if !r.Passed {
		b.WriteString("\nWhy this report failed:\n\n")
		for _, reason := range r.Reasons {
			fmt.Fprintf(&b, "- %s\n", reason)
		}
	}
	b.WriteString("\n## Summary\n\n")
	b.WriteString(summaryTable(r))
	b.WriteString(measurementsLine(r))
	b.WriteString(brokerInternalBlock(r))
	if bad := rowsWith(r, Fail, NotRun); len(bad) > 0 {
		b.WriteString("\n## FAIL and NOT-RUN rows\n\n")
		b.WriteString(rowTable(bad, 400))
	}
	if v := rowsWith(r, Void); len(v) > 0 {
		b.WriteString("\n## VOID rows (the oracle leg failed)\n\n")
		b.WriteString(rowTable(v, 400))
	}
	if g := rowsWith(r, Gap); len(g) > 0 {
		b.WriteString("\n## Optional features\n\nKafka features KubeMQ does not offer, listed in [`docs/gaps.md`](" + toRoot(r) + "docs/gaps.md) before the run.\n\n")
		b.WriteString(optionalTable(g))
	}
	if f := rowsWith(r, Flaky); len(f) > 0 {
		b.WriteString("\n## Quarantined rows (FLAKY)\n\n")
		b.WriteString(rowTable(f, 400))
	}
	b.WriteString("\n## All rows\n")
	for _, t := range r.Order {
		fmt.Fprintf(&b, "\n### %s\n\n", t)
		b.WriteString(rowTable(r.RowsByTr[t], 240))
	}
	return b.String()
}

// CompatibilityMarkdown is COMPATIBILITY.md at the repository root. Summary first, in this
// order: per-track counts, the limits to read before evaluating, the provenance ratio, the gaps
// that will affect a user, then the detail (run record and every row).
func CompatibilityMarkdown(r *Report) string {
	var b strings.Builder
	b.WriteString("<!-- GENERATED by runner/report from the latest run. Do not edit. -->\n")
	b.WriteString("# KubeMQ Kafka compatibility\n\n")
	full := filepath.ToSlash(filepath.Join(PublishDir(r), "REPORT.md"))
	fmt.Fprintf(&b, "Latest run: %s against Apache Kafka %s, **%s**. %s Badges and this page follow the latest run, not the latest green one. Full report: [`%s`](%s).\n",
		kubemqName(r), r.Run.Kafka.Version, status(r), totalsSentence(r), full, full)

	b.WriteString("\n## Summary\n\n")
	b.WriteString(countsTable(r))
	b.WriteString(measurementsLine(r))
	b.WriteString(brokerInternalBlock(r))
	b.WriteString(optInBlock(r))
	if !r.Passed && len(r.Reasons) > 0 {
		b.WriteString("\nWhy this run failed:\n\n")
		for _, reason := range r.Reasons {
			fmt.Fprintf(&b, "- %s\n", reason)
		}
	}

	b.WriteString("\n")
	b.WriteString(limitsBlock)

	b.WriteString("\n## Provenance\n\n")
	b.WriteString(provenanceBlock(r))

	b.WriteString("\n## Differences from Kafka that will affect you\n\n")
	b.WriteString(gapsBlock(r))

	b.WriteString("\n## Detail\n\n### Run\n\n")
	b.WriteString(header(r))
	for _, t := range r.Order {
		fmt.Fprintf(&b, "\n### %s\n\n", t)
		b.WriteString(rowTable(r.RowsByTr[t], 200))
	}
	b.WriteString("\nMethodology: [`docs/methodology.md`](docs/methodology.md). Rerun it yourself: [`docs/rerun.md`](docs/rerun.md).\n")
	return b.String()
}

// totalsSentence: "<n> rows: <p> PASS, <g> OPTIONAL, ..." over every track, zero counts left out.
func totalsSentence(r *Report) string {
	rows := 0
	var parts []string
	for _, v := range VerdictOrder {
		n := 0
		for _, t := range r.Tracks {
			if !t.OptIn {
				n += t.Counts[v]
			}
		}
		rows += n
		if n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, label(v)))
		}
	}
	if rows == 0 {
		return "No rows."
	}
	return fmt.Sprintf("%d rows: %s.", rows, strings.Join(parts, ", "))
}

// countsTable: verdict counts per track and a total row; NOT-RUN and FAIL in bold so a skipped
// or failing track is never quiet.
func countsTable(r *Report) string {
	var b strings.Builder
	b.WriteString("| Track | Rows | PASS | OPTIONAL | FAIL | VOID | NOT-RUN | FLAKY | Result |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|\n")
	total := map[string]int{}
	rows := 0
	for _, t := range r.Tracks {
		if t.OptIn {
			continue
		}
		b.WriteString(countsRow(t))
		for _, v := range VerdictOrder {
			total[v] += t.Counts[v]
		}
		rows += t.Rows
	}
	fmt.Fprintf(&b, "| **all** | **%d** | %d | %d | %s | %d | %s | %d | **%s** |\n",
		rows, total[Pass], total[Gap], bold(total[Fail]), total[Void], bold(total[NotRun]), total[Flaky], strings.ToLower(status(r)))
	return b.String()
}

// measurementsLine: measurement rows (performance numbers) are listed in the detail with their
// values and only counted here, outside every verdict count.
func measurementsLine(r *Report) string {
	var parts []string
	for _, t := range r.Tracks {
		if t.Measurements == 0 {
			continue
		}
		p := fmt.Sprintf("%s %d", t.Track, t.Measurements)
		if t.MeasurementsIncomplete > 0 {
			p += fmt.Sprintf(" (**%d did not complete**)", t.MeasurementsIncomplete)
		}
		if t.MeasurementsNotRun > 0 {
			p += fmt.Sprintf(" (%d measurement not run)", t.MeasurementsNotRun)
		}
		parts = append(parts, p)
	}
	if len(parts) == 0 {
		return ""
	}
	where := "values in the detail"
	if Public {
		where = "published performance: " + BenchmarkURL
	}
	return "\nMeasurements, outside the counts above (" + where + "): " + strings.Join(parts, ", ") + ".\n"
}

// countsRow is one track's line of a counts table.
func countsRow(t TrackSummary) string {
	res := "passed"
	if !t.Passed {
		res = "**failed**"
	}
	void := fmt.Sprint(t.Counts[Void])
	if t.VoidOver {
		void = fmt.Sprintf("**%d (over budget)**", t.Counts[Void])
	} else if len(t.VoidNoNote) > 0 {
		void = fmt.Sprintf("**%d (no root cause)**", t.Counts[Void])
	}
	return fmt.Sprintf("| %s | %d | %d | %d | %s | %s | %s | %d | %s |\n",
		t.Track, t.Rows, t.Counts[Pass], t.Counts[Gap], bold(t.Counts[Fail]), void, bold(t.Counts[NotRun]), t.Counts[Flaky], res)
}

// optInBlock: opt-in tracks (separately licensed software, run only on request), outside the
// counts above. One that ran has its own counts and its verdicts count toward the run's status;
// one that did not run is listed, and is not NOT-RUN.
func optInBlock(r *Report) string {
	var ran []TrackSummary
	for _, t := range r.Tracks {
		if t.OptIn {
			ran = append(ran, t)
		}
	}
	if len(ran) == 0 && len(r.OptInNotRun) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("\nOpt-in tracks run only on request, because the software they test is under a separate license. They are not part of the counts above.\n\n")
	if len(ran) > 0 {
		b.WriteString("| Opt-in track | Rows | PASS | OPTIONAL | FAIL | VOID | NOT-RUN | FLAKY | Result |\n")
		b.WriteString("|---|---|---|---|---|---|---|---|---|\n")
		for _, t := range ran {
			b.WriteString(countsRow(t))
		}
	}
	if len(r.OptInNotRun) > 0 {
		if len(ran) > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "Not run in this run: %s.\n", strings.Join(r.OptInNotRun, ", "))
	}
	return b.String()
}

// provenanceBlock: how many rows that ran port an upstream Apache Kafka test and how many KubeMQ
// wrote, overall and per track. Rows that did not run carry no test and are left out; a row that
// ran without a provenance is counted and shown, never dropped.
func provenanceBlock(r *Report) string {
	type count struct{ up, own, none int }
	per := map[string]*count{}
	var all count
	for _, t := range r.Order {
		c := &count{}
		for _, row := range r.RowsByTr[t] {
			if row.Verdict == NotRun {
				continue
			}
			switch row.Provenance {
			case "upstream":
				c.up++
			case "kubemq-authored":
				c.own++
			default:
				c.none++
			}
		}
		per[t] = c
		all.up += c.up
		all.own += c.own
		all.none += c.none
	}
	ran := all.up + all.own + all.none
	var b strings.Builder
	if ran == 0 {
		b.WriteString("No rows ran.\n")
		return b.String()
	}
	fmt.Fprintf(&b, "Of the %d rows that ran, %d (%s) port an upstream Apache Kafka test and %d (%s) were written by KubeMQ.",
		ran, all.up, pct(all.up, ran), all.own, pct(all.own, ran))
	if all.none > 0 {
		fmt.Fprintf(&b, " **%d (%s) record no provenance.**", all.none, pct(all.none, ran))
	}
	b.WriteString(" Every row runs on real Apache Kafka first; KubeMQ then runs the same client code with only the bootstrap address changed.\n\n")
	b.WriteString("| Track | Upstream Kafka test | KubeMQ-authored | No provenance |\n|---|---|---|---|\n")
	for _, t := range r.Order {
		c := per[t]
		if c.up+c.own+c.none == 0 {
			continue
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %s |\n", t, c.up, c.own, bold(c.none))
	}
	return b.String()
}

func pct(n, of int) string {
	return fmt.Sprintf("%d%%", (n*100+of/2)/of)
}

const signedPrefix = "pre-registered gap, signed off: "

// gapsBlock: what differs from Kafka, worst first. Unregistered differences, rows that did not
// run and inconclusive rows come before the registered gaps, each in its own table.
func gapsBlock(r *Report) string {
	var b strings.Builder
	fails, notRun, unsure, gaps := rowsWith(r, Fail), rowsWith(r, NotRun), rowsWith(r, Void, Flaky), rowsWith(r, Gap)
	if len(fails)+len(notRun)+len(unsure)+len(gaps) == 0 {
		b.WriteString("None in the tracks this run covered.\n")
		return b.String()
	}
	b.WriteString("Workarounds and roadmap for every known difference: [`docs/gaps.md`](docs/gaps.md).\n")
	if len(fails) > 0 {
		fmt.Fprintf(&b, "\n### Differences from Kafka without a registration (FAIL): %d\n\n", len(fails))
		b.WriteString(rowTable(fails, 300))
	}
	if len(notRun) > 0 {
		fmt.Fprintf(&b, "\n### Rows that did not run (NOT-RUN): %d\n\nA row that did not run is not evidence either way and fails the report.\n\n", len(notRun))
		b.WriteString(rowTable(notRun, 300))
	}
	if len(unsure) > 0 {
		fmt.Fprintf(&b, "\n### Inconclusive rows (VOID, FLAKY): %d\n\nVOID: the Kafka oracle leg itself failed, so the row proves nothing. FLAKY: a quarantined row that failed this time.\n\n", len(unsure))
		b.WriteString(rowTable(unsure, 300))
	}
	if len(gaps) > 0 {
		fmt.Fprintf(&b, "\n### Optional features: %d\n\nKafka features KubeMQ does not offer, listed before the run.\n\n", len(gaps))
		b.WriteString(optionalTable(gaps))
	}
	return b.String()
}

// SummaryJSON is reports/<version>/summary.json: counts, verdict per row, no raw output.
func SummaryJSON(r *Report) ([]byte, error) {
	type rowVerdict struct {
		ID         string `json:"id"`
		Verdict    string `json:"verdict"`
		Provenance string `json:"provenance,omitempty"`
	}
	rep := r
	if Public {
		rep = publicCopy(r)
	}
	out := struct {
		*Report
		Rows map[string][]rowVerdict `json:"rows"`
	}{Report: rep, Rows: map[string][]rowVerdict{}}
	for _, t := range r.Order {
		for _, row := range r.RowsByTr[t] {
			out.Rows[t] = append(out.Rows[t], rowVerdict{row.ID, row.Verdict, row.Provenance})
		}
	}
	b, err := json.MarshalIndent(out, "", "  ")
	return append(b, '\n'), err
}

// publicCopy is the report as summary.json shows it in the public repository: the same lines
// the public header leaves out (KubeMQ image, operator, command, kit commit, raw logs) are empty.
func publicCopy(r *Report) *Report {
	c := *r
	c.Run.KubeMQ = Image{}
	c.Run.Operator, c.Run.Command, c.Run.FinalCommit = "", "", ""
	c.Run.Repo, c.Run.RawLogs = Repo{}, RawLogs{}
	c.Run.Sources = make([]RunSource, len(r.Run.Sources))
	for i, s := range r.Run.Sources {
		s.Label, s.Operator, s.Command = "", "", ""
		s.Repo, s.RawLogs = Repo{}, RawLogs{}
		c.Run.Sources[i] = s
	}
	return &c
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func orUnknown(s string) string {
	if strings.TrimSpace(s) == "" {
		return "not recorded"
	}
	return s
}

// PublishDir is where a published run's REPORT.md and summary.json go, relative to the
// repository root: reports/<kubemq-version>/ for a certification run, reports/preview/
// kafka-<rc>-kubemq-<kubemq-version>/ for a Kafka release-candidate preview.
func PublishDir(r *Report) string {
	if Public {
		return "results"
	}
	if r.Run.KafkaPreview != "" {
		return filepath.Join("reports", "preview", "kafka-"+r.Run.KafkaPreview+"-kubemq-"+r.Run.KubeMQ.Version)
	}
	return filepath.Join("reports", r.Run.KubeMQ.Version)
}

// toRoot is the relative path from the published report back to the repository root.
func toRoot(r *Report) string {
	return strings.Repeat("../", len(strings.Split(filepath.ToSlash(PublishDir(r)), "/")))
}

// brokerInternalBlock keeps every scope exclusion visible beside the main counts.
func brokerInternalBlock(r *Report) string {
	rows := rowsWith(r, NotApplicable)
	if len(rows) == 0 {
		return ""
	}
	return fmt.Sprintf("\n## Broker-internal requests outside application compatibility: %d\n\nThese requests are NOT-APPLICABLE to application replacement. Kafka broker interoperability remains unsupported. They are excluded from the PASS and OPTIONAL counts.\n\n", len(rows)) + rowTable(rows, 0)
}
