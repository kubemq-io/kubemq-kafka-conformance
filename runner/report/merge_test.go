package report

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runDir writes a run directory for one machine: run.json and one results file per track.
func runDir(t *testing.T, parent, name, commit, kubemqDigest string, tracks map[string][]Row) string {
	t.Helper()
	d := filepath.Join(parent, name)
	must(t, os.MkdirAll(filepath.Join(d, "results"), 0o755))
	var names []string
	for tr, rows := range tracks {
		names = append(names, tr)
		for i := range rows {
			if rows[i].Provenance == "" {
				rows[i].Provenance = "kubemq-authored"
			}
		}
		writeJSON(t, filepath.Join(d, "results", tr+".json"), TrackResults{Schema: 1, Track: tr, Rows: rows})
	}
	writeJSON(t, filepath.Join(d, "run.json"), Run{Tracks: names, StartedUTC: "2026-10-07T10:00:00Z", FinishedUTC: "2026-10-07T10:30:00Z",
		Operator: "op-" + name, Machine: "machine " + name, Command: "runner/certify.sh --track x", Repo: Repo{Commit: commit},
		RawLogs: RawLogs{File: "raw-" + name + ".tar.gz", SHA256: "sha256:" + name},
		Kafka:   Image{Version: "4.3.1", Digest: "sha256:k"}, KubeMQ: Image{Version: "v9.9.9", Digest: kubemqDigest},
		Engine: Engine{Name: "engine-" + name, Arch: "amd64"}})
	return d
}

func TestMergeRuns(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.file("tracks.txt", "protocol quick\nfeatures full\nperf full\n")
	f.file("matrix/features.yaml", "track: features\nrows:\n  - id: b\n")
	w1 := runDir(t, f.root, "w1", "c1", "sha256:q", map[string][]Row{"protocol": {{ID: "a", Verdict: Pass}}})
	w2 := runDir(t, f.root, "w2", "c1", "sha256:q", map[string][]Row{"features": {{ID: "b", Verdict: Pass}}})
	out := filepath.Join(f.root, "merged")
	m, err := MergeRuns([]string{w1, w2}, out, false, nil)
	must(t, err)
	if len(m.Sources) != 2 || m.MixedCommits || m.Repo.Commit != "c1" {
		t.Fatalf("merged run: %+v", m)
	}
	in := f.inputs()
	in.RunDir, in.AllTracks = out, true
	r, err := Build(in)
	must(t, err)
	if verdictOf(r, "a") != Pass || verdictOf(r, "b") != Pass || verdictOf(r, "perf.track") != NotRun {
		t.Fatal("each track's verdicts come from the run that ran it; a track no run ran is NOT-RUN")
	}
	md := ReportMarkdown(r)
	for _, want := range []string{"| Runs | 2 runs, ", "| protocol | machine w1 | engine-w1", "`c1` (every run)",
		"`raw-w2.tar.gz` `sha256:w2`", "| features | machine w2 |"} {
		if !strings.Contains(md, want) {
			t.Errorf("merged REPORT.md lacks %q", want)
		}
	}
}

func TestMergeRunsRefusals(t *testing.T) {
	p := t.TempDir()
	a := runDir(t, p, "a", "c1", "sha256:q", map[string][]Row{"protocol": {{ID: "x", Verdict: Pass}}})
	otherImage := runDir(t, p, "b", "c1", "sha256:other", map[string][]Row{"features": {{ID: "y", Verdict: Pass}}})
	sameTrack := runDir(t, p, "c", "c1", "sha256:q", map[string][]Row{"protocol": {{ID: "x", Verdict: Pass}}})
	otherCommit := runDir(t, p, "d", "c2", "sha256:q", map[string][]Row{"features": {{ID: "y", Verdict: Pass}}})
	for name, dirs := range map[string][]string{
		"different KubeMQ image": {a, otherImage},
		"one track in two runs":  {a, sameTrack},
		"different kit commits":  {a, otherCommit},
	} {
		if _, err := MergeRuns(dirs, filepath.Join(p, "out-"+strings.ReplaceAll(name, " ", "-")), false, nil); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
	m, err := MergeRuns([]string{a, otherCommit}, filepath.Join(p, "mixed"), true, nil)
	must(t, err)
	if !m.MixedCommits || m.Repo.Commit != "mixed" {
		t.Fatal("--allow-mixed-commits merges and marks the run")
	}
	r := &Report{Run: m}
	if !strings.Contains(header(r), "**different commits per run**") {
		t.Error("the header must say the commits differ")
	}
	if _, err := MergeRuns([]string{filepath.Join(p, "mixed")}, filepath.Join(p, "again"), false, nil); err == nil {
		t.Error("a merged run must not be merged again")
	}
}

func TestMergeSplitTrack(t *testing.T) {
	p := t.TempDir()
	main := runDir(t, p, "main", "c1", "sha256:q", map[string][]Row{"migration": {
		{ID: "c.a", Verdict: Pass}, {ID: "c.b", Verdict: NotRun, Detail: "no compose on the driver"}, {ID: "c.c", Verdict: Fail}}})
	follow := runDir(t, p, "follow", "c1", "sha256:q", map[string][]Row{"migration": {{ID: "c.b", Verdict: Pass}}})
	out := filepath.Join(p, "out")
	m, err := MergeRuns([]string{main, follow}, out, false, nil)
	must(t, err)
	if len(m.Tracks) != 1 || len(m.Sources) != 2 {
		t.Fatalf("one track listed once, both runs in the table: %+v", m)
	}
	var tr TrackResults
	must(t, readJSON(filepath.Join(out, "results", "migration.json"), &tr))
	got := map[string]string{}
	for _, r := range tr.Rows {
		got[r.ID] = r.Verdict
	}
	if len(tr.Rows) != 3 || got["c.a"] != Pass || got["c.b"] != Pass || got["c.c"] != Fail {
		t.Fatalf("the follow-up's verdict fills the NOT-RUN row, nothing else changes: %v", got)
	}

	overlap := runDir(t, p, "overlap", "c1", "sha256:q", map[string][]Row{"migration": {{ID: "c.c", Verdict: Pass}}})
	if _, err := MergeRuns([]string{main, overlap}, filepath.Join(p, "o1"), false, nil); err == nil || !strings.Contains(err.Error(), "c.c") {
		t.Errorf("a row with a verdict in both runs must be refused: %v", err)
	}
	otherCommit := runDir(t, p, "other", "c2", "sha256:q", map[string][]Row{"migration": {{ID: "c.b", Verdict: Pass}}})
	if _, err := MergeRuns([]string{main, otherCommit}, filepath.Join(p, "o2"), true, nil); err == nil {
		t.Error("one track's rows from different commits must be refused, even with --allow-mixed-commits")
	}
}

func TestMergeSuperseded(t *testing.T) {
	p := t.TempDir()
	first := runDir(t, p, "first", "c1", "sha256:q", map[string][]Row{
		"protocol": {{ID: "x", Verdict: Fail}}, "features": {{ID: "y", Verdict: Pass}}})
	rerun := runDir(t, p, "rerun", "c2", "sha256:q", map[string][]Row{"protocol": {{ID: "x", Verdict: Pass}}})
	out := filepath.Join(p, "out")
	sup := map[string][]string{filepath.Clean(first): {"protocol"}}
	m, err := MergeRuns([]string{first, rerun}, out, true, sup)
	must(t, err)
	var tr TrackResults
	must(t, readJSON(filepath.Join(out, "results", "protocol.json"), &tr))
	if len(tr.Rows) != 1 || tr.Rows[0].Verdict != Pass {
		t.Fatalf("the rerun's protocol results replace the superseded ones: %+v", tr.Rows)
	}
	if !strings.Contains(header(&Report{Run: m}), "| features (protocol: superseded by a later run) |") {
		t.Error("the per-run table names the superseded track")
	}
	if _, err := MergeRuns([]string{first}, filepath.Join(p, "o1"), true, sup); err == nil {
		t.Error("superseding a track no other run covers must be refused")
	}
	if _, err := MergeRuns([]string{first, rerun}, filepath.Join(p, "o2"), true, map[string][]string{filepath.Clean(rerun): {"features"}}); err == nil {
		t.Error("superseding a track the run did not run must be refused")
	}
	if _, err := MergeRuns([]string{first, rerun}, filepath.Join(p, "o3"), true, nil); err == nil {
		t.Error("without --superseded the overlapping protocol verdicts are still refused")
	}
}

func TestAmendments(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.file("tracks.txt", "protocol quick\n")
	ok := Amendment{Row: "a", From: Fail, To: NotRun, Reason: "driver SSH session dropped (infrastructure)", DecidedBy: "reviewer", Time: "2026-10-08T05:00:00Z"}
	build := func(rows []Row, am ...Amendment) (*Report, error) {
		d := runDir(t, f.root, "w9", "c1", "sha256:q", map[string][]Row{"protocol": rows})
		var run Run
		must(t, readJSON(filepath.Join(d, "run.json"), &run))
		run.Amendments = am
		writeJSON(t, filepath.Join(d, "run.json"), run)
		out := filepath.Join(f.root, "merged")
		must(t, os.RemoveAll(out))
		if _, err := MergeRuns([]string{d}, out, false, nil); err != nil {
			return nil, err
		}
		in := f.inputs()
		in.RunDir, in.AllTracks = out, true
		return Build(in)
	}
	r, err := build([]Row{{ID: "a", Verdict: Fail}}, ok)
	must(t, err)
	if verdictOf(r, "a") != NotRun {
		t.Fatal("the amendment, not an edit of the results, turns the as-run FAIL into NOT-RUN")
	}
	md := ReportMarkdown(r)
	for _, want := range []string{"**1 verdicts were amended after the run**", "| `a` | w9 | FAIL | NOT-RUN | driver SSH session dropped (infrastructure) | reviewer | 2026-10-08T05:00:00Z |", "amended from FAIL to NOT-RUN by reviewer"} {
		if !strings.Contains(md, want) {
			t.Errorf("REPORT.md lacks %q", want)
		}
	}
	bad := ok
	bad.From, bad.To = Fail, Pass
	if _, err := build([]Row{{ID: "a", Verdict: Fail}}, bad); err == nil {
		t.Error("FAIL to PASS must never be an amendment")
	}
	bad = ok
	bad.Reason = ""
	if _, err := build([]Row{{ID: "a", Verdict: Fail}}, bad); err == nil {
		t.Error("an amendment without its cause must be refused")
	}
	if _, err := build([]Row{{ID: "a", Verdict: NotRun}}, ok); err == nil {
		t.Error("an amendment whose from does not match the as-run verdict (an edited results file) must be refused")
	}
	bad = ok
	bad.Time = "yesterday"
	if _, err := build([]Row{{ID: "a", Verdict: Fail}}, bad); err == nil {
		t.Error("an amendment needs a UTC time")
	}
}

// A gap signed between two machines' run starts binds for the track run after it, not the other.
func TestMergedSignOffComparedWithTheTracksOwnRun(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.file("tracks.txt", "protocol quick\nfeatures full\n")
	f.file("matrix/features.yaml", "track: features\nrows:\n  - id: b\n")
	f.file("docs/gaps.md", "| Row | Deviation | Signed off |\n|---|---|---|\n| `b` | x | reviewer 2026-10-08T01:30Z |\n")
	early := runDir(t, f.root, "early", "c1", "sha256:q", map[string][]Row{"protocol": {{ID: "a", Verdict: Pass}}})
	late := runDir(t, f.root, "late", "c1", "sha256:q", map[string][]Row{"features": {{ID: "b", Verdict: Gap, Deviation: "x"}}})
	setStart := func(d, s string) {
		var r Run
		must(t, readJSON(filepath.Join(d, "run.json"), &r))
		r.StartedUTC = s
		writeJSON(t, filepath.Join(d, "run.json"), r)
	}
	setStart(early, "2026-10-08T00:51:47Z")
	setStart(late, "2026-10-08T01:36:36Z")
	out := filepath.Join(f.root, "merged")
	_, err := MergeRuns([]string{early, late}, out, false, nil)
	must(t, err)
	in := f.inputs()
	in.RunDir, in.AllTracks = out, true
	r, err := Build(in)
	must(t, err)
	if verdictOf(r, "b") != Gap {
		t.Fatal("the sign-off precedes the run that ran features, so the row is GAP")
	}
}

func TestNotRunReason(t *testing.T) {
	f := newFixture(t, []string{"a"}, "protocol")
	f.file("tracks.txt", "protocol quick\nmigration full\n")
	f.file("matrix/migration.yaml", "track: migration\nrows:\n  - id: c\n")
	d := runDir(t, f.root, "w1", "c1", "sha256:q", map[string][]Row{"protocol": {{ID: "a", Verdict: Pass}}})
	out := filepath.Join(f.root, "merged")
	_, err := MergeRuns([]string{d}, out, false, nil)
	must(t, err)
	if err := SetNotRunReasons(out, map[string]string{"protocol": "x"}); err == nil {
		t.Error("a reason for a track a run covered must be refused")
	}
	must(t, SetNotRunReasons(out, map[string]string{"migration": "migration run deferred to a follow-up run"}))
	in := f.inputs()
	in.RunDir, in.AllTracks = out, true
	r, err := Build(in)
	must(t, err)
	if verdictOf(r, "c") != NotRun || !strings.Contains(ReportMarkdown(r), "track not run in this run: migration run deferred to a follow-up run") {
		t.Error("the reason is shown on the NOT-RUN rows")
	}
}

func TestCheckFinal(t *testing.T) {
	p := t.TempDir()
	mk := func(name, commit, version string, dirty bool) string {
		d := runDir(t, p, name, commit, "sha256:q", map[string][]Row{name: {{ID: name + ".a", Verdict: Pass}}})
		var r Run
		must(t, readJSON(filepath.Join(d, "run.json"), &r))
		r.KubeMQ.Version, r.Repo.Dirty = version, dirty
		writeJSON(t, filepath.Join(d, "run.json"), r)
		return d
	}
	merged := func(dirs ...string) string {
		out := filepath.Join(p, "out-"+filepath.Base(dirs[len(dirs)-1]))
		_, err := MergeRuns(dirs, out, true, nil)
		must(t, err)
		return out
	}
	a := mk("a", "abcdef0123", "v3.6.17", false)
	b := mk("b", "abcdef0123", "v3.6.17", false)
	out := merged(a, b)
	must(t, CheckFinal(out, "abcdef0"))
	var m Run
	must(t, readJSON(filepath.Join(out, "run.json"), &m))
	if m.FinalCommit != "abcdef0123" || !strings.Contains(header(&Report{Run: m}), "| Release report | final: every run used kit commit `abcdef0123`") {
		t.Fatal("the final report records and shows its single commit")
	}
	if err := CheckFinal(merged(a, mk("c", "0000000999", "v3.6.17", false)), "abcdef0"); err == nil {
		t.Error("a run from another commit must be refused")
	}
	if err := CheckFinal(merged(mk("d", "abcdef0123", "v3.6.17-pre.3", false)), "abcdef0"); err == nil {
		t.Error("a pre-release is never a final report")
	}
	if err := CheckFinal(merged(mk("e", "abcdef0123", "v3.6.17", true)), "abcdef0"); err == nil {
		t.Error("a run with uncommitted changes must be refused")
	}
}
