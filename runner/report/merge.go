package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// RunSource is one run directory of a merged release report: which tracks it ran and where.
type RunSource struct {
	Label  string   `json:"label"` // the run directory's base name
	Tracks []string `json:"tracks"`
	// Superseded lists tracks this run ran whose results were replaced by a later run (merge
	// --superseded); they are listed, not used.
	Superseded []string `json:"superseded,omitempty"`
	// Amendments are this run's recorded verdict reclassifications (Run.Amendments).
	Amendments  []Amendment `json:"amendments,omitempty"`
	StartedUTC  string      `json:"started_utc"`
	FinishedUTC string      `json:"finished_utc"`
	Command     string      `json:"command"`
	Operator    string      `json:"operator"`
	Machine     string      `json:"machine"`
	Repo        Repo        `json:"repo"`
	RawLogs     RawLogs     `json:"raw_logs"`
	Engine      Engine      `json:"engine"`
}

// MergeRuns combines the run directories of one release, run on several machines, into a single
// run directory at out (run.json plus results/), ready for Build with AllTracks. It refuses:
//   - runs of different KubeMQ or Kafka images (digest), or a preview mixed with a certification;
//   - a track that two run directories ran, unless their verdicts are on disjoint rows (a row
//     NOT-RUN in one run is absent there) and both runs used the same kit commit: then the
//     rows are combined, for example a follow-up run of the rows a first run could not reach;
//   - a directory that is already a merge;
//   - runs from different kit commits, unless allowMixed; the merged run then says so.
//
// superseded maps a run directory to tracks whose results in it are not used, because a later
// run in the same merge replaces them (for example a rerun after a fix). Each such track must
// have been run by that directory and must be covered by another run of the merge.
func MergeRuns(dirs []string, out string, allowMixed bool, superseded map[string][]string) (Run, error) {
	if len(dirs) == 0 {
		return Run{}, fmt.Errorf("no run directories to merge")
	}
	var runs []Run
	owners := map[string][]int{}
	for i, d := range dirs {
		var r Run
		if err := readJSON(filepath.Join(d, "run.json"), &r); err != nil {
			return Run{}, fmt.Errorf("%s: run.json: %w", d, err)
		}
		if len(r.Sources) > 0 {
			return Run{}, fmt.Errorf("%s is already a merged run; merge the original run directories", d)
		}
		drop := map[string]bool{}
		for _, t := range superseded[filepath.Clean(d)] {
			if !contains(r.Tracks, t) {
				return Run{}, fmt.Errorf("--superseded %s:%s: that run did not run track %s", d, t, t)
			}
			drop[t] = true
		}
		var used []string
		for _, t := range r.Tracks {
			if drop[t] {
				continue
			}
			owners[t] = append(owners[t], i)
			used = append(used, t)
		}
		r.Tracks = used
		runs = append(runs, r)
	}
	for d, ts := range superseded {
		known := false
		for _, x := range dirs {
			known = known || filepath.Clean(x) == d
		}
		if !known {
			return Run{}, fmt.Errorf("--superseded names %s, which is not one of the run directories", d)
		}
		for _, t := range ts {
			if len(owners[t]) == 0 {
				return Run{}, fmt.Errorf("--superseded %s:%s: no other run in this merge ran %s", d, t, t)
			}
		}
	}
	first := runs[0]
	commits := map[string]bool{}
	for i, r := range runs {
		switch {
		case r.KubeMQ.Digest != first.KubeMQ.Digest || r.KubeMQ.Version != first.KubeMQ.Version:
			return Run{}, fmt.Errorf("%s ran KubeMQ %s %s, %s ran %s %s: one report covers one KubeMQ image", dirs[i], r.KubeMQ.Version, r.KubeMQ.Digest, dirs[0], first.KubeMQ.Version, first.KubeMQ.Digest)
		case r.Kafka.Digest != first.Kafka.Digest || r.KafkaPreview != first.KafkaPreview:
			return Run{}, fmt.Errorf("%s ran Kafka %s %s, %s ran %s %s: one report has one oracle", dirs[i], r.Kafka.Version, r.Kafka.Digest, dirs[0], first.Kafka.Version, first.Kafka.Digest)
		}
		commits[r.Repo.Commit] = true
	}
	if len(commits) > 1 && !allowMixed {
		var list []string
		for i, r := range runs {
			list = append(list, filepath.Base(dirs[i])+"="+r.Repo.Commit)
		}
		return Run{}, fmt.Errorf("the runs used different kit commits (%s); rerun on one commit, or pass --allow-mixed-commits and the header will say so", strings.Join(list, ", "))
	}

	m := Run{Mode: "merged", Kafka: first.Kafka, KubeMQ: first.KubeMQ, KafkaPreview: first.KafkaPreview, Clients: map[string]string{}}
	m.MixedCommits = len(commits) > 1
	if m.MixedCommits {
		m.Repo.Commit = "mixed"
	} else {
		m.Repo.Commit = first.Repo.Commit
	}
	operators, machines := map[string]bool{}, map[string]bool{}
	for i, r := range runs {
		if m.StartedUTC == "" || r.StartedUTC < m.StartedUTC {
			m.StartedUTC = r.StartedUTC
		}
		if r.FinishedUTC > m.FinishedUTC {
			m.FinishedUTC = r.FinishedUTC
		}
		m.Tracks = append(m.Tracks, r.Tracks...)
		m.Repo.Dirty = m.Repo.Dirty || r.Repo.Dirty
		operators[r.Operator] = true
		machines[r.Machine] = true
		m.Sources = append(m.Sources, RunSource{Label: filepath.Base(filepath.Clean(dirs[i])), Tracks: r.Tracks, Superseded: superseded[filepath.Clean(dirs[i])], Amendments: r.Amendments,
			StartedUTC: r.StartedUTC, FinishedUTC: r.FinishedUTC, Command: r.Command, Operator: r.Operator,
			Machine: r.Machine, Repo: r.Repo, RawLogs: r.RawLogs, Engine: r.Engine})
	}
	for _, t := range sortedKeysInts(owners) {
		tr, err := combineTrack(t, owners[t], dirs, runs)
		if err != nil {
			return Run{}, err
		}
		if tr == nil {
			continue // no results file: Build reports the track NOT-RUN
		}
		if err := writeJSONFile(filepath.Join(out, "results", t+".json"), tr); err != nil {
			return Run{}, err
		}
	}
	seen := map[string]bool{}
	var tracks []string
	for _, t := range m.Tracks {
		if !seen[t] {
			seen[t] = true
			tracks = append(tracks, t)
		}
	}
	m.Tracks = tracks
	m.Operator = strings.Join(sortedSet(operators), "; ")
	m.Machine = fmt.Sprintf("%d machines, see Runs", len(machines))
	m.Engine = Engine{Name: "per run, see Runs"}
	m.Command = "runner/report/merge.sh"
	if err := writeJSONFile(filepath.Join(out, "run.json"), m); err != nil {
		return Run{}, err
	}
	return m, nil
}

// combineTrack returns the track's results from the runs that ran it. One run: its results. Two
// or more: their rows combined, which is allowed only when every row has a verdict in at most
// one of them (NOT-RUN counts as no verdict) and they used the same kit commit.
func combineTrack(track string, idx []int, dirs []string, runs []Run) (*TrackResults, error) {
	var parts []*TrackResults
	var from []int
	for _, i := range idx {
		var tr TrackResults
		err := readJSON(filepath.Join(dirs[i], "results", track+".json"), &tr)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("%s: results/%s.json: %w", dirs[i], track, err)
		}
		parts = append(parts, &tr)
		from = append(from, i)
	}
	if len(parts) <= 1 {
		if len(parts) == 0 {
			return nil, nil
		}
		return parts[0], nil
	}
	out := &TrackResults{Schema: parts[0].Schema, Track: track, Clients: map[string]string{}}
	pos := map[string]int{}
	verdictFrom := map[string]int{}
	for k, tr := range parts {
		i := from[k]
		if runs[i].Repo.Commit != runs[from[0]].Repo.Commit {
			return nil, fmt.Errorf("track %s ran in %s and %s from different kit commits; one track's rows must come from one commit", track, dirs[from[0]], dirs[i])
		}
		for c, v := range tr.Clients {
			out.Clients[c] = v
		}
		for _, row := range tr.Rows {
			p, seen := pos[row.ID]
			switch {
			case !seen:
				pos[row.ID] = len(out.Rows)
				out.Rows = append(out.Rows, row)
				if row.Verdict != NotRun {
					verdictFrom[row.ID] = i
				}
			case row.Verdict == NotRun:
				// absent in this run; keep what is there
			case out.Rows[p].Verdict != NotRun:
				return nil, fmt.Errorf("track %s row %s has a verdict in both %s and %s; keep one run", track, row.ID, dirs[verdictFrom[row.ID]], dirs[i])
			default:
				out.Rows[p] = row
				verdictFrom[row.ID] = i
			}
		}
	}
	return out, nil
}

func sortedKeysInts(m map[string][]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// CheckFinal makes a merged run a final release report, single-commit by construction: every
// run used the given kit commit (full hash or a prefix of at least 7 characters) with no
// uncommitted changes, and KubeMQ is a release (vX.Y.Z, not a pre-release). It records the commit.
func CheckFinal(out, commit string) error {
	if len(commit) < 7 {
		return fmt.Errorf("--final needs the kit commit (at least 7 characters)")
	}
	var m Run
	if err := readJSON(filepath.Join(out, "run.json"), &m); err != nil {
		return err
	}
	if !releaseTag.MatchString(m.KubeMQ.Version) {
		return fmt.Errorf("--final: KubeMQ %s is not a release (vX.Y.Z)", m.KubeMQ.Version)
	}
	if m.KafkaPreview != "" {
		return fmt.Errorf("--final: a Kafka release-candidate preview is never a release report")
	}
	for _, s := range m.Sources {
		if !strings.HasPrefix(s.Repo.Commit, commit) {
			return fmt.Errorf("--final: run %s used kit commit %s, not %s", s.Label, s.Repo.Commit, commit)
		}
		if s.Repo.Dirty {
			return fmt.Errorf("--final: run %s had uncommitted changes", s.Label)
		}
	}
	m.FinalCommit = m.Repo.Commit
	return writeJSONFile(filepath.Join(out, "run.json"), m)
}

var releaseTag = regexp.MustCompile(`^v\d+\.\d+\.\d+$`)

// SetNotRunReasons records, in the merged run at out, why tracks that no run covered were not
// run (merge --not-run-reason). A reason for a track some run did cover is refused: it would
// hide nothing but would mislead.
func SetNotRunReasons(out string, reasons map[string]string) error {
	if len(reasons) == 0 {
		return nil
	}
	var m Run
	if err := readJSON(filepath.Join(out, "run.json"), &m); err != nil {
		return err
	}
	for t, r := range reasons {
		if contains(m.Tracks, t) {
			return fmt.Errorf("--not-run-reason %s: a run in this merge ran %s", t, t)
		}
		if strings.TrimSpace(r) == "" {
			return fmt.Errorf("--not-run-reason %s: the reason is empty", t)
		}
	}
	m.NotRunReasons = reasons
	return writeJSONFile(filepath.Join(out, "run.json"), m)
}

func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
