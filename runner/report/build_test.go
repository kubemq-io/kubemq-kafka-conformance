package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fixture struct {
	t    *testing.T
	root string
	run  string
}

func newFixture(t *testing.T, matrixRows []string, tracks ...string) *fixture {
	t.Helper()
	root := t.TempDir()
	f := &fixture{t: t, root: root, run: filepath.Join(root, "run")}
	must(t, os.MkdirAll(filepath.Join(root, "matrix"), 0o755))
	must(t, os.MkdirAll(filepath.Join(root, "docs"), 0o755))
	must(t, os.MkdirAll(filepath.Join(f.run, "results"), 0o755))
	var b strings.Builder
	b.WriteString("track: protocol\nrows:\n")
	for _, id := range matrixRows {
		fmt.Fprintf(&b, "  - id: %s\n", id)
	}
	must(t, os.WriteFile(filepath.Join(root, "matrix", "protocol.yaml"), []byte(b.String()), 0o644))
	run := Run{Tracks: tracks, KubeMQ: Image{Version: "v9.9.9"}, Kafka: Image{Version: "4.3.1"},
		Command: "runner/certify.sh --quick", Operator: "tester", Machine: "test machine, arm64",
		Repo: Repo{Commit: "abc123", Dirty: true}, RawLogs: RawLogs{File: "raw.tar.gz", SHA256: "sha256:ff"},
		Engine: Engine{Name: "engine-x", Runtime: "engine-x", Arch: "arm64", Version: "6.1"}}
	writeJSON(t, filepath.Join(f.run, "run.json"), run)
	return f
}

// results writes a track's results. A row without a provenance gets "kubemq-authored"; "-" means
// the row really records none.
func (f *fixture) results(track string, rows ...Row) {
	for i := range rows {
		switch rows[i].Provenance {
		case "":
			rows[i].Provenance = "kubemq-authored"
		case "-":
			rows[i].Provenance = ""
		}
	}
	writeJSON(f.t, filepath.Join(f.run, "results", track+".json"), TrackResults{Schema: 1, Track: track, Rows: rows})
}

func (f *fixture) file(rel, content string) {
	must(f.t, os.WriteFile(filepath.Join(f.root, rel), []byte(content), 0o644))
}

func (f *fixture) inputs() Inputs {
	return Inputs{
		RunDir: f.run, MatrixDir: filepath.Join(f.root, "matrix"),
		GapsPath: filepath.Join(f.root, "docs", "gaps.md"), Quarantine: filepath.Join(f.root, "matrix", "quarantine.yaml"),
		VoidNotes: filepath.Join(f.root, "matrix", "void-notes.yaml"), ReportsDir: filepath.Join(f.root, "reports"),
		TracksFile: filepath.Join(f.root, "tracks.txt"),
	}
}

func (f *fixture) build() (*Report, error) { return Build(f.inputs()) }

// published writes a past report's summary in which row id was FLAKY.
func (f *fixture) published(version, id string) {
	dir := filepath.Join(f.root, "reports", version)
	must(f.t, os.MkdirAll(dir, 0o755))
	sum := map[string]any{"run": Run{KubeMQ: Image{Version: version}}, "rows": map[string][]map[string]string{"protocol": {{"id": id, "verdict": Flaky}}}}
	writeJSON(f.t, filepath.Join(dir, "summary.json"), sum)
}

func verdictOf(r *Report, id string) string {
	for _, rows := range r.RowsByTr {
		for _, row := range rows {
			if row.ID == id {
				return row.Verdict
			}
		}
	}
	return "absent"
}

func TestAllPass(t *testing.T) {
	f := newFixture(t, []string{"a", "b"}, "protocol")
	f.results("protocol", Row{ID: "a", Verdict: Pass, Provenance: "upstream", Source: "ducktape produce_bench_test"}, Row{ID: "b", Verdict: Pass, Provenance: "kubemq-authored"})
	r, err := f.build()
	must(t, err)
	if !r.Passed || r.Totals[Pass] != 2 {
		t.Fatalf("want passed with 2 PASS, got %+v", r.Totals)
	}
	if r.Tracks[0].Upstream != 1 || r.Tracks[0].KubeMQOwned != 1 {
		t.Fatalf("provenance counts wrong: %+v", r.Tracks[0])
	}
}

func TestMissingRowIsNotRunAndFails(t *testing.T) {
	f := newFixture(t, []string{"a", "b"}, "protocol")
	f.results("protocol", Row{ID: "a", Verdict: Pass})
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "b") != NotRun || r.Passed {
		t.Fatalf("missing row must be NOT-RUN and fail the report")
	}
}

func TestMissingResultsFileMakesEveryRowNotRun(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "a") != NotRun || r.Passed {
		t.Fatalf("no results file must yield NOT-RUN")
	}
}

func TestTrackWithoutMatrixGetsSyntheticNotRun(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol", "security")
	f.results("protocol", Row{ID: "a", Verdict: Pass})
	writeJSON(t, filepath.Join(f.run, "results", "security.json"), TrackResults{Track: "security", NotRunReason: "track not implemented"})
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "security.track") != NotRun || r.Passed {
		t.Fatalf("unimplemented track must be NOT-RUN and fail the report")
	}
}

func TestUnregisteredGapBecomesFail(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.results("protocol", Row{ID: "a", Verdict: Gap})
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "a") != Fail || r.Passed {
		t.Fatalf("unregistered GAP must be FAIL")
	}
}

func TestRegisteredGapStaysGapAndPasses(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.file("docs/gaps.md", "| Row | Deviation | Signed off |\n|---|---|---|\n| `a` | x | reviewer 2026-10-05 |\n")
	f.results("protocol", Row{ID: "a", Verdict: Gap, Deviation: "x"})
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "a") != Gap || !r.Passed {
		t.Fatalf("registered GAP must stay GAP and pass")
	}
}

func TestUnsignedGapEntryIsNotRegistered(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.file("docs/gaps.md", "| Row | Deviation | Signed off |\n|---|---|---|\n| `a` | x | - |\n")
	f.results("protocol", Row{ID: "a", Verdict: Gap, Deviation: "x"})
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "a") != Fail {
		t.Fatalf("unsigned gap entry must not register the row")
	}
}

func TestQuarantinedFailIsFlakyThenExpires(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.results("protocol", Row{ID: "a", Verdict: Fail})
	f.file("matrix/quarantine.yaml", "quarantine:\n  - row: a\n    reason: timing\n    signed_off: reviewer 2026-10-05\n")
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "a") != Flaky || !r.Passed {
		t.Fatalf("quarantined FAIL in its first release must be FLAKY and not fail the report")
	}
	f.published("v9.9.8", "a")
	r, err = f.build()
	must(t, err)
	if verdictOf(r, "a") != Flaky {
		t.Fatalf("second release in quarantine must still be FLAKY")
	}
	f.published("v9.9.7", "a")
	r, err = f.build()
	must(t, err)
	if verdictOf(r, "a") != Fail || r.Passed {
		t.Fatalf("third release in quarantine, counted from published reports, must be FAIL")
	}
}

func TestQuarantineDoesNotTouchPass(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.results("protocol", Row{ID: "a", Verdict: Pass})
	f.file("matrix/quarantine.yaml", "quarantine:\n  - row: a\n    reason: timing\n")
	f.published("v1", "a")
	f.published("v2", "a")
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "a") != Pass {
		t.Fatalf("a PASS stays PASS whatever the quarantine says")
	}
}

func TestVoidBudget(t *testing.T) {
	notes := "notes:\n"
	for i := 0; i < 3; i++ {
		notes += fmt.Sprintf("  - {row: r%d, kubemq_version: v9.9.9, oracle_log: \"https://example/log@sha256:%s\", note: \"oracle timed out\"}\n", i, strings.Repeat("a", 64))
	}
	ids := make([]string, 100)
	rows := make([]Row, 100)
	for i := range ids {
		ids[i] = fmt.Sprintf("r%d", i)
		rows[i] = Row{ID: ids[i], Verdict: Pass}
	}
	rows[0].Verdict, rows[1].Verdict = Void, Void
	f := newFixture(t, ids, "protocol")
	f.file("matrix/void-notes.yaml", notes)
	f.results("protocol", rows...)
	r, err := f.build()
	must(t, err)
	if !r.Passed {
		t.Fatalf("2 VOID in 100 rows is within the 2%% budget: %v", r.Reasons)
	}
	rows[2].Verdict = Void
	f.results("protocol", rows...)
	r, err = f.build()
	must(t, err)
	if r.Passed || !r.Tracks[0].VoidOver {
		t.Fatalf("3 VOID in 100 rows exceeds the budget")
	}
}

func TestVoidBudgetCountsBehaviourRowsOnly(t *testing.T) {
	// 1 behaviour row VOID next to 99 advertisement rows: 100% of behaviour rows, not 1%.
	ids := []string{"b"}
	rows := []Row{{ID: "b", Kind: "behaviour", Verdict: Void}}
	for i := 0; i < 99; i++ {
		id := fmt.Sprintf("apikey.%d", i)
		ids = append(ids, id)
		rows = append(rows, Row{ID: id, Kind: "advertisement", Verdict: Pass})
	}
	f := newFixture(t, ids, "protocol")
	f.file("matrix/void-notes.yaml", "notes:\n  - {row: b, kubemq_version: v9.9.9, oracle_log: x, note: y}\n")
	f.results("protocol", rows...)
	r, err := f.build()
	must(t, err)
	if r.Passed || !r.Tracks[0].VoidOver {
		t.Fatal("advertisement rows must not dilute the VOID budget")
	}
}

func TestVoidWithoutRootCauseFails(t *testing.T) {
	ids := make([]string, 100)
	rows := make([]Row, 100)
	for i := range ids {
		ids[i] = fmt.Sprintf("r%d", i)
		rows[i] = Row{ID: ids[i], Verdict: Pass}
	}
	rows[0].Verdict = Void
	f := newFixture(t, ids, "protocol")
	f.results("protocol", rows...)
	r, err := f.build()
	must(t, err)
	if r.Passed || len(r.Tracks[0].VoidNoNote) != 1 {
		t.Fatal("a VOID without a root-cause note must fail the report")
	}
	f.file("matrix/void-notes.yaml", "notes:\n  - {row: r0, kubemq_version: v9.9.8, oracle_log: x, note: y}\n")
	r, err = f.build()
	must(t, err)
	if r.Passed {
		t.Fatal("a note for another KubeMQ version does not explain this run's VOID")
	}
}

func TestPublishedReportCoversEveryTrack(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.results("protocol", Row{ID: "a", Verdict: Pass})
	f.file("tracks.txt", "# registry\nprotocol quick\nsecurity full\n")
	in := f.inputs()
	in.AllTracks = true
	r, err := Build(in)
	must(t, err)
	if r.Passed || verdictOf(r, "security.track") != NotRun {
		t.Fatal("a published report of a partial run must show the missing tracks as NOT-RUN")
	}
}

func TestSignOffMustBeDated(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.file("docs/gaps.md", "| Row | Deviation | Signed off |\n|---|---|---|\n| `a` | x | pending |\n")
	f.results("protocol", Row{ID: "a", Verdict: Gap, Deviation: "x"})
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "a") != Fail {
		t.Fatal("a sign-off without a name and date is not a sign-off")
	}
}

func TestMalformedInputsAreErrors(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.results("protocol", Row{ID: "a", Verdict: Pass}, Row{ID: "a", Verdict: Fail})
	if _, err := f.build(); err == nil {
		t.Fatal("duplicate row must be an error")
	}
	f.results("protocol", Row{ID: "a", Verdict: "MAYBE"})
	if _, err := f.build(); err == nil {
		t.Fatal("unknown verdict must be an error")
	}
	f.results("protocol", Row{Verdict: Pass})
	if _, err := f.build(); err == nil {
		t.Fatal("row without id must be an error")
	}
}

func TestExtraRowIsKeptWithNote(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.results("protocol", Row{ID: "a", Verdict: Pass}, Row{ID: "x", Verdict: Fail})
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "x") != Fail || r.Passed {
		t.Fatal("a row outside the matrix is still reported and counts")
	}
}

func TestApikeysMatrixOwnsMatrixCurrentRow(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.file("matrix/apikeys.yaml", "keys:\n  - id: apikey.0\nfeatures:\n  - id: feature.x\n")
	f.results("protocol", Row{ID: "a", Verdict: Pass}, Row{ID: "apikey.0", Verdict: Pass}, Row{ID: "feature.x", Verdict: Pass})
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "apikeys.matrix-current") != NotRun || r.Passed {
		t.Fatal("the matrix-current row is owned by the protocol track and must not be skippable")
	}
}

func TestRenderedOutputsCarryVerdictsAndHeader(t *testing.T) {
	f := newFixture(t, []string{"a", "b"}, "protocol")
	f.results("protocol", Row{ID: "a", Verdict: Pass}, Row{ID: "b", Verdict: Fail, Detail: "differs | with pipe\nand newline"})
	r, err := f.build()
	must(t, err)
	md := ReportMarkdown(r)
	for _, want := range []string{"**FAILED**", "## FAIL and NOT-RUN rows", "`b`", "differs \\| with pipe and newline", "v9.9.9",
		"| Operator | tester |", "| Machine | test machine, arm64 |", "`runner/certify.sh --quick`", "abc123 (with uncommitted changes)", "sha256:ff", "engine-x 6.1"} {
		if !strings.Contains(md, want) {
			t.Errorf("REPORT.md lacks %q", want)
		}
	}
	compat := CompatibilityMarkdown(r)
	for _, want := range []string{"Replication factor cannot exceed", "| Operator | tester |", "Why this run failed"} {
		if !strings.Contains(compat, want) {
			t.Errorf("COMPATIBILITY.md lacks %q", want)
		}
	}
	assertOrder(t, compat, "## Summary", "## Read before evaluating", "## Provenance", "## Differences from Kafka that will affect you", "## Detail", "### Run", "### protocol")
	b, err := SummaryJSON(r)
	must(t, err)
	var back map[string]any
	must(t, json.Unmarshal(b, &back))
	if back["passed"] != false {
		t.Error("summary.json must record the failed status")
	}
}

// assertOrder: every heading is present, in this order.
func assertOrder(t *testing.T, doc string, headings ...string) {
	t.Helper()
	last := -1
	for _, h := range headings {
		i := strings.Index(doc, "\n"+h+"\n")
		if i < 0 {
			t.Fatalf("missing heading %q", h)
		}
		if i < last {
			t.Fatalf("heading %q is out of order", h)
		}
		last = i
	}
}

func TestCompatibilitySummaryFirst(t *testing.T) {
	f := newFixture(t, []string{"up", "own", "none", "g", "x", "skip"}, "protocol")
	f.file("docs/gaps.md", "| Row | Deviation | Sign-off |\n|---|---|---|\n| `g` | differs | reviewer 2026-10-06 |\n")
	f.results("protocol",
		Row{ID: "up", Verdict: Pass, Provenance: "upstream", Source: "ducktape produce_bench_test"},
		Row{ID: "own", Verdict: Pass, Provenance: "kubemq-authored"},
		Row{ID: "none", Verdict: Pass, Provenance: "-"},
		Row{ID: "g", Verdict: Gap, Provenance: "kubemq-authored", Title: "gap row", Detail: "differs", Deviation: "differs"},
		Row{ID: "x", Verdict: Fail, Provenance: "kubemq-authored", Title: "fail row"})
	r, err := f.build()
	must(t, err)
	compat := CompatibilityMarkdown(r)
	assertOrder(t, compat, "## Summary", "## Read before evaluating", "## Provenance", "## Differences from Kafka that will affect you",
		"### Differences from Kafka without a registration (FAIL): 1", "### Rows that did not run (NOT-RUN): 1", "## Detail")
	for _, want := range []string{
		"6 rows: 3 PASS, 1 OPTIONAL, 1 FAIL, 1 NOT-RUN.",
		"| **all** | **6** | 3 | 1 | **1** | 0 | **1** | 0 | **failed** |",
		// NOT-RUN rows carry no test and are left out of the ratio; the unrecorded one is shown.
		"Of the 5 rows that ran, 1 (20%) port an upstream Apache Kafka test and 3 (60%) were written by KubeMQ. **1 (20%) record no provenance.**",
		"| protocol | 1 | 3 | **1** |",
	} {
		if !strings.Contains(compat, want) {
			t.Errorf("COMPATIBILITY.md lacks %q", want)
		}
	}
	if verdictOf(r, "g") != Gap {
		t.Fatal("fixture gap not registered")
	}
	if !strings.Contains(compat, "### Optional features: 1") || !strings.Contains(compat, "| `g` | gap row | not recorded | not recorded |") {
		t.Error("a signed gap must be listed as an optional feature")
	}
	if strings.Contains(compat, "reviewer 2026-10-06") {
		t.Error("the sign-off belongs in docs/gaps.md, not in the report")
	}
	if !strings.Contains(compat, "| `g` | OPTIONAL | gap row | differs |") {
		t.Error("a signed gap must read OPTIONAL in the row tables")
	}
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	must(t, err)
	must(t, os.WriteFile(path, b, 0o644))
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestRowWithoutProvenanceFailsTheReport(t *testing.T) {
	f := newFixture(t, []string{"a", "b", "c"}, "protocol")
	f.results("protocol", Row{ID: "a", Verdict: Pass, Provenance: "-"}, Row{ID: "b", Verdict: Pass, Provenance: "upstream"},
		Row{ID: "c", Verdict: Pass, Provenance: "made-up"})
	r, err := f.build()
	must(t, err)
	if r.Passed || len(r.Tracks[0].NoProvenance) != 3 {
		t.Fatalf("no provenance, upstream without its source and an unknown provenance must each fail the report: %v", r.Tracks[0].NoProvenance)
	}
	f.results("protocol", Row{ID: "a", Verdict: Pass}, Row{ID: "b", Verdict: Pass}, Row{ID: "c", Verdict: NotRun, Provenance: "-"})
	r, err = f.build()
	must(t, err)
	if len(r.Tracks[0].NoProvenance) != 0 {
		t.Fatal("a NOT-RUN row carries no test and needs no provenance")
	}
}

func TestGapSignedAfterTheRunIsFail(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	run := Run{Tracks: []string{"protocol"}, StartedUTC: "2026-10-05T10:00:00Z", KubeMQ: Image{Version: "v9.9.9"}}
	writeJSON(t, filepath.Join(f.run, "run.json"), run)
	f.file("docs/gaps.md", "| Row | Deviation | Signed off |\n|---|---|---|\n| `a` | x | reviewer 2026-10-06 |\n")
	f.results("protocol", Row{ID: "a", Verdict: Gap, Deviation: "x"})
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "a") != Fail || r.Passed {
		t.Fatal("a sign-off dated after the run is not a pre-registration")
	}
	f.file("docs/gaps.md", "| Row | Deviation | Signed off |\n|---|---|---|\n| `a` | x | reviewer 2026-10-05 |\n")
	r, err = f.build()
	must(t, err)
	if verdictOf(r, "a") != Gap || !r.Passed {
		t.Fatal("a sign-off on the day of the run is a pre-registration")
	}
}

func TestVoidNoteNeedsDigestPinnedOracleLog(t *testing.T) {
	ids := make([]string, 100)
	rows := make([]Row, 100)
	for i := range ids {
		ids[i] = fmt.Sprintf("r%d", i)
		rows[i] = Row{ID: ids[i], Verdict: Pass}
	}
	rows[0].Verdict = Void
	f := newFixture(t, ids, "protocol")
	f.results("protocol", rows...)
	f.file("matrix/void-notes.yaml", "notes:\n  - {row: r0, kubemq_version: v9.9.9, oracle_log: https://example/oracle.log, note: timed out}\n")
	r, err := f.build()
	must(t, err)
	if r.Passed {
		t.Fatal("a VOID note must link the oracle log by digest")
	}
	f.file("matrix/void-notes.yaml", "notes:\n  - {row: r0, kubemq_version: v9.9.9, oracle_log: \"https://example/oracle.log@sha256:"+strings.Repeat("0", 64)+"\", note: timed out}\n")
	r, err = f.build()
	must(t, err)
	if !r.Passed {
		t.Fatalf("a digest-pinned note explains the VOID: %v", r.Reasons)
	}
}

func TestQuarantineNeedsReasonAndSignOff(t *testing.T) {
	for _, entry := range []string{
		"  - row: a\n    reason: timing\n",
		"  - row: a\n    signed_off: reviewer 2026-10-05\n",
		"  - row: a\n    reason: timing\n    signed_off: pending\n",
	} {
		f := newFixture(t, []string{"a"}, "protocol")
		f.results("protocol", Row{ID: "a", Verdict: Fail})
		f.file("matrix/quarantine.yaml", "quarantine:\n"+entry)
		r, err := f.build()
		must(t, err)
		if verdictOf(r, "a") != Fail {
			t.Fatalf("quarantine entry %q must not hide a FAIL", entry)
		}
	}
}

func TestPublishedReportCoversUnregisteredMatrixTrack(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.file("tracks.txt", "protocol quick\n")
	f.file("matrix/operations.yaml", "track: operations\nrows:\n  - id: ops.a\n")
	f.results("protocol", Row{ID: "a", Verdict: Pass})
	in := f.inputs()
	in.AllTracks = true
	r, err := Build(in)
	must(t, err)
	if verdictOf(r, "ops.a") != NotRun || r.Passed {
		t.Fatal("rows of a matrix track that runner/tracks.txt does not register must be NOT-RUN in a published report")
	}
}

func TestGapDeviationMustMatchTheSignOff(t *testing.T) {
	RequireDeviation = false // the transition behaviour first; the switch is on by default
	defer func() { RequireDeviation = true }()
	f := newFixture(t, []string{"a", "b", "c"}, "protocol")
	f.file("docs/gaps.md", "| Row | Deviation | Signed off |\n|---|---|---|\n"+
		"| `a` | advertised v3-v9 \\| Kafka v0-v13 | reviewer 2026-10-05 |\n"+
		"| `b` | x | reviewer 2026-10-05 |\n"+
		"| `c` | y | reviewer 2026-10-05 |\n")
	f.results("protocol",
		Row{ID: "a", Verdict: Gap, Deviation: "advertised v3-v9 | Kafka v0-v13"},
		Row{ID: "b", Verdict: Gap, Deviation: "x but narrower"},
		Row{ID: "c", Verdict: Gap})
	r, err := f.build()
	must(t, err)
	if verdictOf(r, "a") != Gap {
		t.Error("the signed deviation, with an escaped pipe, must match")
	}
	if verdictOf(r, "b") != Fail {
		t.Error("a deviation that changed since sign-off must be FAIL")
	}
	if verdictOf(r, "c") != Gap || len(r.Tracks[0].GapNoDeviation) != 1 || r.Tracks[0].GapNoDeviation[0] != "c" {
		t.Errorf("a GAP without a deviation stays GAP and is listed while RequireDeviation is off: %v", r.Tracks[0].GapNoDeviation)
	}
	RequireDeviation = true
	r, err = f.build()
	must(t, err)
	if verdictOf(r, "c") != Fail {
		t.Error("with RequireDeviation a GAP without a deviation is FAIL")
	}
}

func TestKafkaPreviewNeverCertifies(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	run := Run{Tracks: []string{"protocol"}, KubeMQ: Image{Version: "v9.9.9"}, Kafka: Image{Version: "4.4.0", Image: "docker.io/apache/kafka"}, KafkaPreview: "4.4.0-rc4"}
	writeJSON(t, filepath.Join(f.run, "run.json"), run)
	f.results("protocol", Row{ID: "a", Verdict: Pass})
	r, err := f.build()
	must(t, err)
	if got := PublishDir(r); got != filepath.Join("reports", "preview", "kafka-4.4.0-rc4-kubemq-v9.9.9") {
		t.Errorf("preview publishes to %s", got)
	}
	md := ReportMarkdown(r)
	for _, want := range []string{"# Preview report: KubeMQ v9.9.9 against Apache Kafka 4.4.0-rc4 (release candidate)", "Preview, not a certification", "4.4.0-rc4 (release candidate)"} {
		if !strings.Contains(md, want) {
			t.Errorf("preview REPORT.md lacks %q", want)
		}
	}
	// A preview's FLAKY rows never count toward a certified row's quarantine history.
	dir := filepath.Join(f.root, "reports", "v1")
	must(t, os.MkdirAll(dir, 0o755))
	writeJSON(t, filepath.Join(dir, "summary.json"), map[string]any{"run": Run{KubeMQ: Image{Version: "v1"}, KafkaPreview: "4.4.0-rc1"},
		"rows": map[string][]map[string]string{"protocol": {{"id": "a", "verdict": Flaky}}}})
	h, err := flakyHistory(filepath.Join(f.root, "reports"))
	must(t, err)
	if len(h["a"]) != 0 {
		t.Error("a preview summary must not count as a release in quarantine")
	}
	if got := PublishDir(&Report{Run: Run{KubeMQ: Image{Version: "v9.9.9"}}}); got != filepath.Join("reports", "v9.9.9") {
		t.Errorf("certification publishes to %s", got)
	}
}

func TestOptInTrack(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.file("tracks.txt", "protocol quick\nschema-registry opt-in\n")
	f.file("matrix/schema-registry.yaml", "track: schema-registry\nrows:\n  - id: sr.a\n")
	f.results("protocol", Row{ID: "a", Verdict: Pass})
	in := f.inputs()
	in.AllTracks = true
	r, err := Build(in)
	must(t, err)
	if !r.Passed || verdictOf(r, "sr.a") != "absent" || len(r.OptInNotRun) != 1 || r.OptInNotRun[0] != "schema-registry" {
		t.Fatalf("an opt-in track that did not run is listed, not NOT-RUN, and does not fail the report: passed=%v %v %v", r.Passed, r.Reasons, r.OptInNotRun)
	}
	compat := CompatibilityMarkdown(r)
	if !strings.Contains(compat, "Not run in this run: schema-registry.") || strings.Contains(compat, "| schema-registry |") {
		t.Error("COMPATIBILITY.md lists the opt-in track as not run, outside the counts")
	}

	run := Run{Tracks: []string{"protocol", "schema-registry"}, KubeMQ: Image{Version: "v9.9.9"}}
	writeJSON(t, filepath.Join(f.run, "run.json"), run)
	f.results("schema-registry", Row{ID: "sr.a", Verdict: Fail})
	r, err = Build(in)
	must(t, err)
	compat = CompatibilityMarkdown(r)
	if r.Passed || len(r.OptInNotRun) != 0 {
		t.Fatal("an opt-in track that ran counts toward the run's status")
	}
	if !strings.Contains(compat, "| Opt-in track | Rows |") || !strings.Contains(compat, "| schema-registry | 1 | 0 | 0 | **1** |") {
		t.Errorf("a ran opt-in track has its own counts table:\n%s", compat)
	}
	if !strings.Contains(compat, "| **all** | **1** | 1 |") || !strings.Contains(compat, "1 rows: 1 PASS.") {
		t.Errorf("the main counts and totals leave the opt-in track out:\n%s", compat[:600])
	}
}

func TestGapSignOffTime(t *testing.T) {
	for _, c := range []struct {
		signed, started string
		gap             bool
	}{
		{"reviewer 2026-10-07", "2026-10-07T15:06:00Z", true},         // grandfathered date-only
		{"reviewer 2026-10-08", "2026-10-08T15:06:00Z", false},        // later date-only: end of day
		{"reviewer 2026-10-08T16:40Z", "2026-10-08T15:06:00Z", false}, // signed after the start
		{"reviewer 2026-10-08T14:00Z", "2026-10-08T15:06:00Z", true},
	} {
		f := newFixture(t, []string{"a"}, "protocol")
		writeJSON(t, filepath.Join(f.run, "run.json"), Run{Tracks: []string{"protocol"}, StartedUTC: c.started, KubeMQ: Image{Version: "v9.9.9"}})
		f.file("docs/gaps.md", "| Row | Deviation | Signed off |\n|---|---|---|\n| `a` | x | "+c.signed+" |\n")
		f.results("protocol", Row{ID: "a", Verdict: Gap, Deviation: "x"})
		r, err := f.build()
		must(t, err)
		if got := verdictOf(r, "a") == Gap; got != c.gap {
			t.Errorf("%q for a run started %s: GAP=%v, want %v", c.signed, c.started, got, c.gap)
		}
	}
}

// The renderer and the tracks' shared reader (suites/protocol/gaps, a separate module) carry the
// same sign-off rule; the cutoff of the grandfather clause must be the same in both.
func TestGrandfatherCutoffMatchesSharedReader(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "suites", "protocol", "gaps", "gaps.go"))
	must(t, err)
	want := `const DateOnlyGrandfatheredThrough = "` + DateOnlyGrandfatheredThrough + `"`
	if !strings.Contains(string(b), want) {
		t.Fatalf("suites/protocol/gaps/gaps.go must declare %s, as runner/report does", want)
	}
}

func TestMeasurementRowsAreOutsideTheCounts(t *testing.T) {
	f := newFixture(t, []string{"a", "m1", "m2"}, "protocol")
	f.results("protocol", Row{ID: "a", Verdict: Pass},
		Row{ID: "m1", Kind: Measurement, Verdict: Pass, Detail: "throughput 120 MB/s"},
		Row{ID: "m2", Kind: Measurement, Verdict: Void, Detail: "did not complete"})
	r, err := f.build()
	must(t, err)
	ts := r.Tracks[0]
	if !r.Passed || ts.Rows != 1 || ts.Counts[Pass] != 1 || ts.Counts[Void] != 0 || ts.Measurements != 2 || ts.MeasurementsIncomplete != 1 || ts.VoidOver {
		t.Fatalf("measurement rows stay out of rows, counts, the VOID budget and pass/fail: %+v %v", ts, r.Reasons)
	}
	compat := CompatibilityMarkdown(r)
	for _, want := range []string{"Measurements, outside the counts above (values in the detail): protocol 2 (**1 did not complete**).", "throughput 120 MB/s", "1 rows: 1 PASS."} {
		if !strings.Contains(compat, want) {
			t.Errorf("COMPATIBILITY.md lacks %q", want)
		}
	}
	if strings.Contains(compat, "Inconclusive rows") {
		t.Error("a measurement that did not complete is not listed as an inconclusive verdict")
	}
}

func TestMissingMeasurementRowIsNotNotRun(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.file("matrix/perf.yaml", "track: perf\nrows:\n  - {id: c.ok}\n  - {id: c.perf, kind: produce, measurement: true}\n")
	run := Run{Tracks: []string{"protocol", "perf"}, KubeMQ: Image{Version: "v9.9.9"}}
	writeJSON(t, filepath.Join(f.run, "run.json"), run)
	f.results("protocol", Row{ID: "a", Verdict: Pass})
	f.results("perf", Row{ID: "c.ok", Verdict: Pass})
	r, err := f.build()
	must(t, err)
	var cl TrackSummary
	for _, ts := range r.Tracks {
		if ts.Track == "perf" {
			cl = ts
		}
	}
	if !r.Passed || cl.Counts[NotRun] != 0 || cl.Measurements != 1 || cl.MeasurementsNotRun != 1 {
		t.Fatalf("a measurement row that never ran stays outside the counts: %+v %v", cl, r.Reasons)
	}
	if !strings.Contains(CompatibilityMarkdown(r), "perf 1 (1 measurement not run)") {
		t.Error("the measurements line says it was not run")
	}
	if verdictOf(r, "c.perf") != NotRun {
		t.Error("the row itself still reads NOT-RUN in the detail")
	}
}

func TestOptionalCarriesImpactAndWorkaroundAndPublicHidesVersion(t *testing.T) {
	f := newFixture(t, []string{"g"}, "protocol")
	f.file("docs/gaps.md", "| Row | Deviation | Impact | Workaround | Roadmap | Signed off |\n|---|---|---|---|---|---|\n"+
		"| `g` | differs | Kerberos fleets | use SCRAM | On demand. | KubeMQ 2026-10-06 |\n")
	f.results("protocol", Row{ID: "g", Verdict: Gap, Provenance: "kubemq-authored", Title: "gap row",
		Detail: "differs. pre-registered gap, signed off by KubeMQ 2026-10-06.", Deviation: "differs"})
	r, err := f.build()
	must(t, err)
	rep := ReportMarkdown(r)
	if !strings.Contains(rep, "| `g` | gap row | Kerberos fleets | use SCRAM |") {
		t.Errorf("optional table lacks impact and workaround:\n%s", rep)
	}
	if strings.Contains(rep, "signed off") {
		t.Errorf("the sign-off clause must not be shown:\n%s", rep)
	}
	Public = true
	defer func() { Public = false }()
	rep = ReportMarkdown(r)
	if !strings.Contains(rep, "# Certification report: KubeMQ\n") || strings.Contains(rep, "| KubeMQ |") || strings.Contains(rep, "| Operator |") || strings.Contains(rep, "| Command |") {
		t.Errorf("public report must not carry the KubeMQ version, image, operator or command:\n%s", rep)
	}
	if PublishDir(r) != "results" {
		t.Errorf("public report goes to results/, got %s", PublishDir(r))
	}
}
