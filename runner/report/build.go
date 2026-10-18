package report

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Inputs locates everything Build reads.
type Inputs struct {
	RunDir     string // run.json and results/*.json
	MatrixDir  string // matrix/*.yaml
	GapsPath   string // docs/gaps.md
	Quarantine string // matrix/quarantine.yaml
	VoidNotes  string // matrix/void-notes.yaml
	ReportsDir string // reports/, the published history (for quarantine expiry)
	TracksFile string // runner/tracks.txt
	// AllTracks makes the report cover every track in TracksFile; tracks that did not run are
	// NOT-RUN. Required for a published report.
	AllTracks bool
}

// ExpectedRows returns, per track, the row ids the matrices own. Every one of them must get a
// verdict in a run of that track.
func ExpectedRows(matrixDir string) (map[string][]string, error) {
	out := map[string][]string{}
	files, err := filepath.Glob(filepath.Join(matrixDir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	for _, f := range files {
		base := filepath.Base(f)
		if base == "apikey-impacts.yaml" || base == "quarantine.yaml" || base == "void-notes.yaml" {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var doc struct {
			Track string `yaml:"track"`
			Rows  []struct {
				ID string `yaml:"id"`
			} `yaml:"rows"`
			Keys []struct {
				ID string `yaml:"id"`
			} `yaml:"keys"`
			Features []struct {
				ID string `yaml:"id"`
			} `yaml:"features"`
		}
		if err := yaml.Unmarshal(b, &doc); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		track := doc.Track
		if base == "apikeys.yaml" {
			// Generated from live ApiVersions; its rows belong to the protocol track, which also
			// owns the row proving the committed file matches the brokers.
			track = "protocol"
			out[track] = append(out[track], "apikeys.matrix-current")
		}
		if track == "" {
			return nil, fmt.Errorf("%s: no track", f)
		}
		for _, list := range [][]struct {
			ID string `yaml:"id"`
		}{doc.Rows, doc.Keys, doc.Features} {
			for _, r := range list {
				out[track] = append(out[track], r.ID)
			}
		}
	}
	return out, nil
}

// amendmentsOf: a run's amendments, or every source run's for a merged report.
func amendmentsOf(run Run) []Amendment {
	out := append([]Amendment(nil), run.Amendments...)
	for _, s := range run.Sources {
		out = append(out, s.Amendments...)
	}
	return out
}

// applyAmendments applies every amendment that names a row of this track: the results stay
// exactly as the run wrote them, and the amendment changes the verdict here, with a note on the
// row. Only FAIL to NOT-RUN is allowed, with a reason, the decider and a UTC time, and the
// recorded verdict must be the amendment's "from"; anything else makes the report unbuildable.
func applyAmendments(track string, rows []Row, all []Amendment) ([]Row, error) {
	idx := map[string]int{}
	for i, r := range rows {
		idx[r.ID] = i
	}
	for _, a := range all {
		i, ok := idx[a.Row]
		if !ok {
			continue
		}
		switch {
		case a.From != Fail || a.To != NotRun:
			return nil, fmt.Errorf("track %s: amendment of %s from %s to %s: only FAIL to NOT-RUN, for an infrastructure cause, may be amended", track, a.Row, a.From, a.To)
		case strings.TrimSpace(a.Reason) == "" || strings.TrimSpace(a.DecidedBy) == "":
			return nil, fmt.Errorf("track %s: amendment of %s needs the infrastructure cause and who decided it", track, a.Row)
		case rows[i].Verdict != a.From:
			return nil, fmt.Errorf("track %s: amendment of %s is from %s, but the results record %s (results stay as run; the amendment does the change)", track, a.Row, a.From, rows[i].Verdict)
		}
		if _, err := time.Parse(time.RFC3339, a.Time); err != nil {
			return nil, fmt.Errorf("track %s: amendment of %s needs a UTC time (RFC 3339): %v", track, a.Row, err)
		}
		rows[i].Verdict = a.To
		rows[i].Notes = append(rows[i].Notes, fmt.Sprintf("amended from %s to %s by %s at %s: %s", a.From, a.To, a.DecidedBy, a.Time, a.Reason))
	}
	return rows, nil
}

// MeasurementRows returns the ids of matrix rows marked `measurement: true` (performance numbers,
// see Row.Kind). A track sets the kind on the rows it runs; the matrix mark covers rows that
// never ran. (A matrix row's own `kind` field is the track's scenario type, not this.)
func MeasurementRows(matrixDir string) (map[string]bool, error) {
	out := map[string]bool{}
	files, err := filepath.Glob(filepath.Join(matrixDir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var doc struct {
			Rows []struct {
				ID          string `yaml:"id"`
				Measurement bool   `yaml:"measurement"`
			} `yaml:"rows"`
		}
		if err := yaml.Unmarshal(b, &doc); err != nil {
			continue // ExpectedRows reports malformed matrices
		}
		for _, r := range doc.Rows {
			if r.Measurement {
				out[r.ID] = true
			}
		}
	}
	return out, nil
}

// QuarantineEntry is one row in matrix/quarantine.yaml.
type QuarantineEntry struct {
	Row       string `yaml:"row"`
	Reason    string `yaml:"reason"`
	SignedOff string `yaml:"signed_off"`
}

// MaxQuarantineReleases is how many releases a row may be FLAKY in before it is FAIL.
const MaxQuarantineReleases = 2

func loadQuarantine(path string) (map[string]QuarantineEntry, error) {
	out := map[string]QuarantineEntry{}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	var doc struct {
		Quarantine []QuarantineEntry `yaml:"quarantine"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	for _, q := range doc.Quarantine {
		// An entry without a reason or a "<who> <YYYY-MM-DD>" sign-off quarantines nothing: the
		// row stays FAIL.
		if strings.TrimSpace(q.Reason) != "" && gapSignOff.MatchString(strings.TrimSpace(q.SignedOff)) {
			out[q.Row] = q
		}
	}
	return out, nil
}

// flakyHistory returns, per row, the KubeMQ versions whose published summary shows it FLAKY.
// Nobody maintains this by hand, so a quarantine cannot outlive its limit by neglect.
func flakyHistory(reportsDir string) (map[string]map[string]bool, error) {
	out := map[string]map[string]bool{}
	if reportsDir == "" {
		return out, nil
	}
	files, err := filepath.Glob(filepath.Join(reportsDir, "*", "summary.json"))
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		var sum struct {
			Run  Run `json:"run"`
			Rows map[string][]struct {
				ID      string `json:"id"`
				Verdict string `json:"verdict"`
			} `json:"rows"`
		}
		if err := readJSON(f, &sum); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		if sum.Run.KafkaPreview != "" {
			continue // a preview is never part of the certified history
		}
		for _, rows := range sum.Rows {
			for _, r := range rows {
				if r.Verdict == Flaky {
					if out[r.ID] == nil {
						out[r.ID] = map[string]bool{}
					}
					out[r.ID][sum.Run.KubeMQ.Version] = true
				}
			}
		}
	}
	return out, nil
}

// VoidNote is one root-cause note in matrix/void-notes.yaml.
type VoidNote struct {
	Row           string `yaml:"row"`
	KubeMQVersion string `yaml:"kubemq_version"`
	OracleLog     string `yaml:"oracle_log"` // link to the published oracle log, with its digest
	Note          string `yaml:"note"`
}

func loadVoidNotes(path string) (map[string]VoidNote, error) {
	out := map[string]VoidNote{}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) || path == "" {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	var doc struct {
		Notes []VoidNote `yaml:"notes"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	for _, n := range doc.Notes {
		if n.Note != "" && oracleLogRef.MatchString(n.OracleLog) {
			out[n.Row+"@"+n.KubeMQVersion] = n
		}
	}
	return out, nil
}

var (
	gapRowID = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)
	// "<who> <YYYY-MM-DD>" or "<who> <YYYY-MM-DDTHH:MMZ>" (seconds optional), UTC.
	gapSignOff = regexp.MustCompile(`^\S.*\S\s+\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?Z)?$`)
	// oracleLogRef: a VOID note links the published oracle log by digest.
	oracleLogRef = regexp.MustCompile(`\S@sha256:[0-9a-f]{64}$`)
)

// DateOnlyGrandfatheredThrough: date-only sign-offs dated on or before this day count from
// 00:00:00Z of their day (all were given before their row ran, except
// security.delegation-tokens, handled by a fresh run); later date-only sign-offs count from
// 23:59:59Z, and sign-offs with a UTC time are exact. Same rule as suites/protocol/gaps.
const DateOnlyGrandfatheredThrough = "2026-10-07"

// signOffTime is when a sign-off was made (see DateOnlyGrandfatheredThrough).
func signOffTime(signed string) (time.Time, bool) {
	f := strings.Fields(signed)
	if len(f) < 2 {
		return time.Time{}, false
	}
	last := f[len(f)-1]
	if t, err := time.Parse("2006-01-02", last); err == nil {
		if last <= DateOnlyGrandfatheredThrough {
			return t, true
		}
		return t.Add(24*time.Hour - time.Second), true
	}
	for _, layout := range []string{"2006-01-02T15:04Z", "2006-01-02T15:04:05Z"} {
		if t, err := time.Parse(layout, last); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// signedAfter: the sign-off was made after start (or cannot be read).
func signedAfter(signed string, start time.Time) bool {
	t, ok := signOffTime(signed)
	return !ok || t.After(start)
}

// trackStart is when the run that produced a track started: the run itself, or, in a report
// merged from several runs, the source run that ran the track (the earliest one when two runs
// share a track, so a sign-off must precede every run that contributed rows). A gap's sign-off
// is compared with this, never with the start of some other machine's run.
func trackStart(run Run, track string) time.Time {
	var earliest time.Time
	for _, s := range run.Sources {
		if !contains(s.Tracks, track) {
			continue
		}
		t, err := time.Parse(time.RFC3339, s.StartedUTC)
		if err != nil {
			continue
		}
		if earliest.IsZero() || t.Before(earliest) {
			earliest = t
		}
	}
	if !earliest.IsZero() {
		return earliest
	}
	return runStart(run)
}

// runStart is when the run started, or the zero time when run.json does not record it.
func runStart(run Run) time.Time {
	t, err := time.Parse(time.RFC3339, run.StartedUTC)
	if err != nil {
		return time.Time{}
	}
	return t
}

func sortedTracks(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// gapEntry is one signed register line: the exact deviation signed off and the sign-off.
type gapEntry struct {
	Deviation  string
	Impact     string
	Workaround string
	Signed     string
}

// RequireDeviation makes a GAP row without an observed deviation FAIL: every track reports the
// deviation it observed, so a GAP that cannot be matched to its sign-off is not a GAP. On by
// default; off, such rows are only listed in TrackSummary.GapNoDeviation.
var RequireDeviation = true

// loadGaps reads the register table of docs/gaps.md: first cell the row id, second cell the
// exact deviation signed off, last cell the sign-off, which must read "<who> <YYYY-MM-DD>".
// Unsigned lines are left out. Cells split on unescaped pipes; "\|" is a literal pipe.
func loadGaps(path string) (map[string]gapEntry, error) {
	out := map[string]gapEntry{}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "|") || !strings.HasSuffix(line, "|") {
			continue
		}
		cells := splitCells(line)
		if len(cells) < 2 {
			continue
		}
		id := strings.Trim(cells[0], "`")
		signed := cells[len(cells)-1]
		e := gapEntry{Signed: signed}
		if len(cells) >= 3 {
			e.Deviation = cells[1]
		}
		// The full register line: Row, Deviation, Impact, Workaround, Roadmap, Signed off.
		if len(cells) >= 6 {
			e.Impact, e.Workaround = cells[2], cells[3]
		}
		if gapRowID.MatchString(id) && gapSignOff.MatchString(signed) {
			out[id] = e
		}
	}
	return out, sc.Err()
}

// splitCells splits a Markdown table line on unescaped pipes, unescapes "\|" and trims each cell
// (the same rule as suites/protocol/gaps, which the tracks use).
func splitCells(line string) []string {
	line = strings.TrimSuffix(strings.TrimPrefix(line, "|"), "|")
	var cells []string
	var cur strings.Builder
	for i := 0; i < len(line); i++ {
		if line[i] == '\\' && i+1 < len(line) && line[i+1] == '|' {
			cur.WriteByte('|')
			i++
			continue
		}
		if line[i] == '|' {
			cells = append(cells, strings.TrimSpace(cur.String()))
			cur.Reset()
			continue
		}
		cur.WriteByte(line[i])
	}
	return append(cells, strings.TrimSpace(cur.String()))
}

var validVerdict = map[string]bool{Pass: true, Fail: true, Gap: true, Void: true, NotRun: true, Flaky: true, NotApplicable: true}

// Build reads a run and applies every rule. An error means the inputs are malformed (the report
// cannot be produced at all), not that the report failed.
func Build(in Inputs) (*Report, error) {
	var run Run
	if err := readJSON(filepath.Join(in.RunDir, "run.json"), &run); err != nil {
		return nil, fmt.Errorf("run.json: %w", err)
	}
	if len(run.Tracks) == 0 {
		return nil, fmt.Errorf("run.json lists no tracks")
	}
	expected, err := ExpectedRows(in.MatrixDir)
	if err != nil {
		return nil, err
	}
	measurementRows, err := MeasurementRows(in.MatrixDir)
	if err != nil {
		return nil, err
	}
	gaps, err := loadGaps(in.GapsPath)
	if err != nil {
		return nil, err
	}
	quarantine, err := loadQuarantine(in.Quarantine)
	if err != nil {
		return nil, err
	}
	history, err := flakyHistory(in.ReportsDir)
	if err != nil {
		return nil, err
	}
	notes, err := loadVoidNotes(in.VoidNotes)
	if err != nil {
		return nil, err
	}

	tracks := run.Tracks
	unregistered := map[string]bool{}
	ran := map[string]bool{}
	for _, t := range run.Tracks {
		ran[t] = true
	}
	tiers := trackTiers(in.TracksFile)
	var optInNotRun []string
	if in.AllTracks {
		all, err := registeredTracks(in.TracksFile)
		if err != nil {
			return nil, err
		}
		// An opt-in track (Schema Registry and other separately licensed software) that did not
		// run is not NOT-RUN: it is listed as not run and never fails the report.
		tracks = nil
		for _, t := range all {
			if tiers[t] == "opt-in" && !ran[t] {
				optInNotRun = append(optInNotRun, t)
				continue
			}
			tracks = append(tracks, t)
		}
		for _, t := range run.Tracks {
			if !contains(all, t) {
				return nil, fmt.Errorf("run.json lists track %q, which runner/tracks.txt does not register", t)
			}
		}
		// A matrix whose track is not registered would otherwise never be run or reported: its
		// rows are NOT-RUN, never silently absent.
		for _, t := range sortedTracks(expected) {
			if !contains(all, t) {
				tracks = append(tracks, t)
				unregistered[t] = true
			}
		}
	}

	rep := &Report{Run: run, Totals: map[string]int{}, RowsByTr: map[string][]Row{}, Clients: map[string]string{}, OptInNotRun: optInNotRun}
	for _, track := range tracks {
		var tr TrackResults
		path := filepath.Join(in.RunDir, "results", track+".json")
		if unregistered[track] {
			tr = TrackResults{Track: track, NotRunReason: "matrix rows exist for track " + track + " but runner/tracks.txt does not register it"}
		} else if !ran[track] {
			reason := "track not run in this run (mode " + run.Mode + ")"
			if r := strings.TrimSpace(run.NotRunReasons[track]); r != "" {
				reason = "track not run in this run: " + r
			}
			tr = TrackResults{Track: track, NotRunReason: reason}
		} else if err := readJSON(path, &tr); err != nil {
			tr = TrackResults{Track: track, NotRunReason: "no results file: " + err.Error()}
		}
		for k, v := range tr.Clients {
			rep.Clients[k] = v
		}
		rows, err := reconcile(track, tr, expected[track], measurementRows, gaps, quarantine, history, run.KubeMQ.Version, trackStart(run, track))
		if err != nil {
			return nil, fmt.Errorf("track %s: %w", track, err)
		}
		rows, err = applyAmendments(track, rows, amendmentsOf(run))
		if err != nil {
			return nil, err
		}
		rep.Order = append(rep.Order, track)
		rep.RowsByTr[track] = rows
		ts := summarize(track, rows, tr.NotRunReason, notes, run.KubeMQ.Version)
		ts.OptIn = tiers[track] == "opt-in"
		rep.Tracks = append(rep.Tracks, ts)
		for v, n := range ts.Counts {
			rep.Totals[v] += n
		}
		if !ts.Passed {
			rep.Reasons = append(rep.Reasons, trackFailureReason(ts))
		}
	}
	rep.Passed = len(rep.Reasons) == 0
	return rep, nil
}

func reconcile(track string, tr TrackResults, expected []string, measurementRows map[string]bool, gaps map[string]gapEntry, quarantine map[string]QuarantineEntry, history map[string]map[string]bool, version string, start time.Time) ([]Row, error) {
	got := map[string]Row{}
	var order []string
	for _, r := range tr.Rows {
		if r.ID == "" {
			return nil, fmt.Errorf("a row has no id")
		}
		if _, dup := got[r.ID]; dup {
			return nil, fmt.Errorf("row %s has more than one verdict", r.ID)
		}
		if !validVerdict[r.Verdict] {
			return nil, fmt.Errorf("row %s has unknown verdict %q", r.ID, r.Verdict)
		}
		got[r.ID] = r
		order = append(order, r.ID)
	}

	var rows []Row
	seen := map[string]bool{}
	for _, id := range expected {
		seen[id] = true
		r, ok := got[id]
		if !ok {
			reason := "no verdict produced for this matrix row"
			if tr.NotRunReason != "" {
				reason = tr.NotRunReason
			}
			r = Row{ID: id, Verdict: NotRun, Detail: reason}
			if measurementRows[id] {
				// The matrix marks it a measurement: a missing one is "measurement not run",
				// outside the verdict counts like every measurement.
				r.Kind = Measurement
				r.Detail = "measurement not run: " + reason
			}
		}
		rows = append(rows, applyRules(r, gaps, quarantine, history, version, start))
	}
	for _, id := range order {
		if seen[id] {
			continue
		}
		r := got[id]
		r.Notes = append(r.Notes, "row is not in the committed matrix")
		rows = append(rows, applyRules(r, gaps, quarantine, history, version, start))
	}
	if len(rows) == 0 {
		reason := tr.NotRunReason
		if reason == "" {
			reason = "track has no matrix rows and produced no results"
		}
		rows = append(rows, Row{ID: track + ".track", Title: "Track " + track, Verdict: NotRun, Detail: reason})
	}
	return rows, nil
}

func applyRules(r Row, gaps map[string]gapEntry, quarantine map[string]QuarantineEntry, history map[string]map[string]bool, version string, start time.Time) Row {
	if r.Verdict == NotApplicable {
		allowed := r.ID == "apikey.27" || r.ID == "apikey.83" || r.ID == "apikey.84" || r.ID == "apikey.85" || r.ID == "apikey.86" || r.ID == "apikey.87"
		e, signed := gaps[r.ID]
		_, validTime := signOffTime(e.Signed)
		if !allowed || r.Kind != BrokerInternal || !signed || !validTime || start.IsZero() || signedAfter(e.Signed, start) || strings.TrimSpace(r.Deviation) == "" || strings.TrimSpace(r.Deviation) != strings.TrimSpace(e.Deviation) {
			r.Verdict, r.Kind = Fail, "advertisement"
			r.Notes = append(r.Notes, "broker-internal scope exclusion is unapproved, changed, unsigned, or signed after this run")
		} else {
			r.Notes = append(r.Notes, signedPrefix+e.Signed)
			r.Impact, r.Workaround = e.Impact, e.Workaround
		}
	}
	if r.Verdict == Gap {
		e, ok := gaps[r.ID]
		signed := e.Signed
		observed := strings.TrimSpace(r.Deviation)
		switch {
		case ok && !start.IsZero() && signedAfter(signed, start):
			r.Verdict = Fail
			r.Notes = append(r.Notes, "reported GAP but signed off ("+signed+") after the run started at "+start.UTC().Format(time.RFC3339)+"; a date-only sign-off counts as the end of its day: shown as FAIL")
		case ok && observed != "" && observed != strings.TrimSpace(e.Deviation):
			r.Verdict = Fail
			r.Notes = append(r.Notes, fmt.Sprintf("reported GAP but the observed deviation %q differs from the one signed off (%q): shown as FAIL until signed off again", observed, e.Deviation))
		case ok && observed == "" && RequireDeviation:
			r.Verdict = Fail
			r.Notes = append(r.Notes, "reported GAP without the observed deviation, so it cannot be matched to the sign-off: shown as FAIL")
		case ok:
			r.Notes = append(r.Notes, signedPrefix+signed)
			r.Impact, r.Workaround = e.Impact, e.Workaround
		default:
			r.Verdict = Fail
			r.Notes = append(r.Notes, "reported GAP but not signed off in docs/gaps.md: shown as FAIL")
		}
	}
	if q, ok := quarantine[r.ID]; ok && (r.Verdict == Fail || r.Verdict == Flaky) {
		releases := map[string]bool{version: true}
		for v := range history[r.ID] {
			releases[v] = true
		}
		if len(releases) > MaxQuarantineReleases {
			r.Verdict = Fail
			r.Notes = append(r.Notes, fmt.Sprintf("quarantine expired: FLAKY in %d releases, the limit is %d: shown as FAIL", len(releases), MaxQuarantineReleases))
		} else {
			r.Verdict = Flaky
			r.Notes = append(r.Notes, fmt.Sprintf("quarantined (%s), release %d of %d", q.Reason, len(releases), MaxQuarantineReleases))
		}
	}
	return r
}

func summarize(track string, rows []Row, notRun string, notes map[string]VoidNote, version string) TrackSummary {
	ts := TrackSummary{Track: track, Counts: map[string]int{}, NotRun: notRun}
	voids := 0
	for _, r := range rows {
		if r.Verdict == NotApplicable && r.Kind == BrokerInternal {
			ts.NotApplicable++
			if r.Provenance != "kubemq-authored" && !(r.Provenance == "upstream" && strings.TrimSpace(r.Source) != "") {
				ts.NoProvenance = append(ts.NoProvenance, r.ID)
			}
			continue // Visible separately; neither a passing test nor an application gap.
		}
		if r.Kind == Measurement {
			// A performance number, not a verdict: outside the counts, the VOID budget and
			// pass/fail. Its provenance is still recorded.
			ts.Measurements++
			switch r.Verdict {
			case Pass:
			case NotRun:
				ts.MeasurementsNotRun++
			default:
				ts.MeasurementsIncomplete++
			}
			if r.Verdict != NotRun && r.Provenance != "kubemq-authored" && !(r.Provenance == "upstream" && strings.TrimSpace(r.Source) != "") {
				ts.NoProvenance = append(ts.NoProvenance, r.ID)
			}
			continue
		}
		ts.Rows++
		ts.Counts[r.Verdict]++
		if r.Verdict == Gap && strings.TrimSpace(r.Deviation) == "" {
			ts.GapNoDeviation = append(ts.GapNoDeviation, r.ID)
		}
		switch {
		case r.Verdict == NotRun:
		case r.Provenance == "upstream" && strings.TrimSpace(r.Source) != "":
			ts.Upstream++
		case r.Provenance == "kubemq-authored":
			ts.KubeMQOwned++
		default:
			// Every row that ran records where its scenario comes from; an upstream row names
			// the upstream test.
			ts.NoProvenance = append(ts.NoProvenance, r.ID)
		}
		if r.Kind != "advertisement" {
			ts.Behaviour++
			if r.Verdict == Void {
				voids++
			}
		}
		if r.Verdict == Void {
			if _, ok := notes[r.ID+"@"+version]; !ok {
				ts.VoidNoNote = append(ts.VoidNoNote, r.ID)
			}
		}
	}
	if ts.Behaviour > 0 {
		ts.VoidShare = float64(voids) / float64(ts.Behaviour)
	}
	ts.VoidOver = ts.VoidShare > VoidBudget
	ts.Passed = ts.Counts[Fail] == 0 && ts.Counts[NotRun] == 0 && !ts.VoidOver && len(ts.VoidNoNote) == 0 && len(ts.NoProvenance) == 0
	return ts
}

func trackFailureReason(ts TrackSummary) string {
	var parts []string
	if n := ts.Counts[Fail]; n > 0 {
		parts = append(parts, fmt.Sprintf("%d FAIL", n))
	}
	if n := ts.Counts[NotRun]; n > 0 {
		parts = append(parts, fmt.Sprintf("%d NOT-RUN", n))
	}
	if ts.VoidOver {
		parts = append(parts, fmt.Sprintf("VOID %.1f%% of behaviour rows, over the %.0f%% budget", ts.VoidShare*100, VoidBudget*100))
	}
	if len(ts.VoidNoNote) > 0 {
		parts = append(parts, fmt.Sprintf("%d VOID without a root-cause note in matrix/void-notes.yaml (%s)", len(ts.VoidNoNote), strings.Join(ts.VoidNoNote, ", ")))
	}
	if len(ts.NoProvenance) > 0 {
		parts = append(parts, fmt.Sprintf("%d rows without provenance (upstream with its source, or kubemq-authored): %s", len(ts.NoProvenance), strings.Join(ts.NoProvenance, ", ")))
	}
	return ts.Track + ": " + strings.Join(parts, ", ")
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// trackTiers reads the tier of every track in runner/tracks.txt; a missing file means no tiers.
func trackTiers(path string) map[string]string {
	out := map[string]string{}
	b, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && !strings.HasPrefix(f[0], "#") {
			out[f[0]] = f[1]
		}
	}
	return out
}

// registeredTracks reads runner/tracks.txt: "<name> <tier>" per line.
func registeredTracks(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("track registry: %w", err)
	}
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 || strings.HasPrefix(f[0], "#") {
			continue
		}
		out = append(out, f[0])
	}
	return out, nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
