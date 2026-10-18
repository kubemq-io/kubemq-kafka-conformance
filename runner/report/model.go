// Package report turns a run's results into COMPATIBILITY.md, reports/<version>/REPORT.md and
// summary.json, and decides whether the report passed.
//
// The rules are those of docs/methodology.md:
//   - every matrix-owned row must have exactly one verdict; a missing one is NOT-RUN;
//   - a row reported GAP must be registered in docs/gaps.md, or it is shown as FAIL;
//   - a GAP must carry a sign-off of the form "<who> <YYYY-MM-DD>" (the track also checks the
//     deviation and the date against the run);
//   - each track may have at most 2% VOID rows, counted over behaviour rows (advertisement
//     rows, which only compare ApiVersions, cannot be VOID-ed into a pass);
//   - every VOID row needs a root-cause note for this KubeMQ version in matrix/void-notes.yaml;
//   - a FAIL on a quarantined row (matrix/quarantine.yaml) is shown as FLAKY until it has been
//     FLAKY in more than two releases, counted from the published reports/*/summary.json;
//   - a published report covers every track in runner/tracks.txt; a track that did not run is
//     NOT-RUN;
//   - every row that ran records its provenance (upstream with its source, or kubemq-authored);
//   - a GAP signed off after the run started is shown as FAIL;
//   - a GAP whose observed deviation differs from the one signed off is shown as FAIL;
//   - a VOID root-cause note links the oracle log by digest (<link>@sha256:<hex>);
//   - a quarantine entry needs a reason and a "<who> <YYYY-MM-DD>" sign-off;
//   - a published report also covers matrix tracks runner/tracks.txt does not register (NOT-RUN);
//   - any FAIL, any NOT-RUN, a VOID budget overrun, an unexplained VOID or a row without
//     provenance fails the report.
package report

// Verdicts, exactly as defined in docs/methodology.md.
const (
	Pass           = "PASS"
	Fail           = "FAIL"
	Gap            = "GAP"
	Void           = "VOID"
	NotRun         = "NOT-RUN"
	Flaky          = "FLAKY"
	NotApplicable  = "NOT-APPLICABLE"
	BrokerInternal = "broker-internal"
)

// VerdictOrder is the column order used everywhere.
var VerdictOrder = []string{Pass, Gap, Fail, Void, NotRun, Flaky}

// Amendment records a verdict a person reclassified after the run. A human never edits a verdict
// silently: the only allowed amendment is FAIL to NOT-RUN, for an infrastructure cause named in
// Reason, decided by a reviewer; the as-run results travel in the raw-logs tarball. Every
// amendment is shown in the report header and on the row.
type Amendment struct {
	Row       string `json:"row"`
	From      string `json:"from"`
	To        string `json:"to"`
	Reason    string `json:"reason"`
	DecidedBy string `json:"decided_by"`
	Time      string `json:"time"` // RFC 3339, UTC
}

// Measurement is the row kind of a performance number (see Row.Kind).
const Measurement = "measurement"

// VoidBudget is the maximum share of VOID rows per track.
const VoidBudget = 0.02

// Leg is one broker's side of a row, as written by the tracks.
type Leg struct {
	OK       bool     `json:"ok"`
	Events   int      `json:"events"`
	Error    string   `json:"error,omitempty"`
	Problems []string `json:"problems,omitempty"`
	Client   string   `json:"client_version,omitempty"`
	MS       int64    `json:"duration_ms"`
}

// Row is one verdict. Tracks write it; the renderer may add Notes and change Verdict only in
// the directions the rules allow (missing to NOT-RUN, unregistered GAP to FAIL, quarantined
// FAIL to FLAKY, expired FLAKY to FAIL).
type Row struct {
	ID string `json:"id"`
	// Kind is "behaviour" (default), "advertisement" (compares ApiVersions only) or "measurement"
	// (a performance number: listed with its value, never a verdict that counts), or
	// "broker-internal" (an approved NOT-APPLICABLE scope exclusion, visible separately).
	Kind       string `json:"kind,omitempty"`
	Title      string `json:"title"`
	Verdict    string `json:"verdict"`
	Provenance string `json:"provenance"`
	Source     string `json:"source"`
	Detail     string `json:"detail,omitempty"`
	// Deviation is what the run observed, for a GAP row: exactly the text of the register's
	// Deviation cell when the gap is the one signed off.
	Deviation string `json:"deviation,omitempty"`
	// Impact and Workaround come from the register's line for an accepted GAP (shown as
	// OPTIONAL) or NOT-APPLICABLE row: who is affected and what to do instead.
	Impact     string   `json:"impact,omitempty"`
	Workaround string   `json:"workaround,omitempty"`
	Oracle     *Leg     `json:"oracle,omitempty"`
	KubeMQ     *Leg     `json:"kubemq,omitempty"`
	DurationMS int64    `json:"duration_ms"`
	Notes      []string `json:"notes,omitempty"`
}

// TrackResults is one results/<track>.json.
type TrackResults struct {
	Schema       int               `json:"schema"`
	Track        string            `json:"track"`
	NotRunReason string            `json:"not_run_reason,omitempty"`
	Rows         []Row             `json:"rows"`
	Clients      map[string]string `json:"clients,omitempty"`
}

// Run is run.json, written by runner/certify.sh.
type Run struct {
	StartedUTC  string   `json:"started_utc"`
	FinishedUTC string   `json:"finished_utc"`
	Mode        string   `json:"mode"`
	Tracks      []string `json:"tracks"`
	Command     string   `json:"command"`
	Operator    string   `json:"operator"`
	Machine     string   `json:"machine"`
	Repo        Repo     `json:"repo"`
	RawLogs     RawLogs  `json:"raw_logs"`
	Kafka       Image    `json:"kafka"`
	KubeMQ      Image    `json:"kubemq"`
	// KafkaPreview is the Apache Kafka release-candidate tag of a preview run, empty for a
	// certification run. A preview never writes COMPATIBILITY.md or reports/<version>/.
	KafkaPreview string            `json:"kafka_preview,omitempty"`
	Clients      map[string]string `json:"clients"`
	Engine       Engine            `json:"engine"`
	// Sources lists the run directories a release report was merged from (runner/report/merge.sh),
	// one per machine; empty for a single run. MixedCommits says they used different kit commits.
	Sources      []RunSource `json:"sources,omitempty"`
	MixedCommits bool        `json:"mixed_commits,omitempty"`
	// Amendments are recorded reclassifications of this run's verdicts (see Amendment).
	Amendments []Amendment `json:"amendments,omitempty"`
	// NotRunReasons gives, for a merged report, why a track no run covered was not run
	// (merge --not-run-reason); it replaces the generic reason on those rows.
	NotRunReasons map[string]string `json:"not_run_reasons,omitempty"`
	// FinalCommit is set on a final release report (merge --final <commit>): every run used
	// exactly this kit commit, clean, for a released (not pre-release) KubeMQ.
	FinalCommit string `json:"final_commit,omitempty"`
}

type Image struct {
	Version string `json:"version"`
	Image   string `json:"image"`
	Digest  string `json:"digest"`
}

type Engine struct {
	Name    string `json:"name"`
	Runtime string `json:"runtime"`
	Arch    string `json:"arch"`
	Version string `json:"version"`
	HostOS  string `json:"host_os"`
}

// Repo is the commit of this repository the run used.
type Repo struct {
	Commit string `json:"commit"`
	Dirty  bool   `json:"dirty"`
}

// RawLogs is the tarball of logs, results and transcripts the operator publishes outside git.
type RawLogs struct {
	File   string `json:"file"`
	SHA256 string `json:"sha256"`
}

// TrackSummary is the per-track outcome.
type TrackSummary struct {
	Track        string         `json:"track"`
	Counts       map[string]int `json:"counts"`
	Rows         int            `json:"rows"`
	Behaviour    int            `json:"behaviour_rows"`
	VoidShare    float64        `json:"void_share_of_behaviour_rows"`
	VoidOver     bool           `json:"void_over_budget"`
	VoidNoNote   []string       `json:"void_without_root_cause,omitempty"`
	NotRun       string         `json:"not_run_reason,omitempty"`
	Upstream     int            `json:"provenance_upstream"`
	KubeMQOwned  int            `json:"provenance_kubemq_authored"`
	NoProvenance []string       `json:"rows_without_provenance,omitempty"`
	// GapNoDeviation lists GAP rows whose track did not report the observed deviation, so only
	// the track matched it to the sign-off (see RequireDeviation).
	GapNoDeviation []string `json:"gap_rows_without_deviation,omitempty"`
	// OptIn marks a track of the opt-in tier (runner/tracks.txt) that ran. COMPATIBILITY.md shows
	// it in its own section, outside the main counts.
	OptIn bool `json:"opt_in,omitempty"`
	// Measurements counts the track's measurement rows, which are outside Rows, Counts, the VOID
	// budget and pass/fail; MeasurementsIncomplete those whose measurement did not complete.
	Measurements           int  `json:"measurements,omitempty"`
	MeasurementsIncomplete int  `json:"measurements_incomplete,omitempty"`
	MeasurementsNotRun     int  `json:"measurements_not_run,omitempty"`
	NotApplicable          int  `json:"broker_internal_not_applicable,omitempty"`
	Passed                 bool `json:"passed"`
}

// Report is everything the templates need; summary.json is a subset of it.
type Report struct {
	Run     Run               `json:"run"`
	Passed  bool              `json:"passed"`
	Reasons []string          `json:"failure_reasons,omitempty"`
	Tracks  []TrackSummary    `json:"tracks"`
	Totals  map[string]int    `json:"totals"`
	Clients map[string]string `json:"clients_ran"`
	// OptInNotRun lists the opt-in tracks that did not run; they are not NOT-RUN.
	OptInNotRun []string         `json:"opt_in_tracks_not_run,omitempty"`
	RowsByTr    map[string][]Row `json:"-"`
	Order       []string         `json:"-"`
}
