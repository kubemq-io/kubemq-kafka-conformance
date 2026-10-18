// Command protocol-track runs the protocol track: every row of matrix/protocol.yaml, first on
// the Kafka oracle, then on KubeMQ with only the bootstrap address changed, and writes one
// verdict per row to a results JSON file.
//
// Verdicts (docs/methodology.md):
//
//	PASS     KubeMQ's normalized transcript equals the oracle's and every invariant holds
//	         (full-transcript rows), or the round-trip succeeded on both (smoke rows).
//	FAIL     KubeMQ differs, errors, or breaks an invariant.
//	VOID     the oracle leg failed or broke a scenario invariant.
//	NOT-RUN  the row names a scenario or driver this suite does not implement, or (full-transcript
//	         rows) the rig self-check failed: the oracle and franz-go's in-memory kfake broker
//	         disagree, so a diff against KubeMQ could not be trusted.
//
// Every row gets exactly one verdict; the command never skips a row.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/apikeys"
	c "github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/conformance"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
)

type matrixRow struct {
	ID         string `yaml:"id"`
	Title      string `yaml:"title"`
	Scenario   string `yaml:"scenario"`
	Driver     string `yaml:"driver"`
	Provenance string `yaml:"provenance"`
	Source     string `yaml:"source"`
}

type matrix struct {
	Track string      `yaml:"track"`
	Rows  []matrixRow `yaml:"rows"`
}

// Leg is one broker's side of a row.
type Leg struct {
	OK       bool     `json:"ok"`
	Events   int      `json:"events"`
	Error    string   `json:"error,omitempty"`
	Problems []string `json:"problems,omitempty"`
	Client   string   `json:"client_version,omitempty"`
	MS       int64    `json:"duration_ms"`
}

// Row is one verdict in the results file. The schema is shared by every track and read by
// runner/report.
type Row struct {
	ID string `json:"id"`
	// Kind is "behaviour" (a scenario ran on both brokers) or "advertisement" (what each broker
	// advertises in ApiVersions). The VOID budget counts behaviour rows only.
	Kind       string `json:"kind"`
	Title      string `json:"title"`
	Verdict    string `json:"verdict"`
	Provenance string `json:"provenance"`
	Source     string `json:"source"`
	Detail     string `json:"detail,omitempty"`
	// Deviation is the observed deviation on a GAP row, exactly as docs/gaps.md signs it off;
	// the renderer checks it against the register again.
	Deviation  string `json:"deviation,omitempty"`
	Oracle     *Leg   `json:"oracle,omitempty"`
	KubeMQ     *Leg   `json:"kubemq,omitempty"`
	DurationMS int64  `json:"duration_ms"`
}

type Results struct {
	Schema int    `json:"schema"`
	Track  string `json:"track"`
	Rows   []Row  `json:"rows"`
	// Clients are the client libraries this track actually ran, read from the binary's build
	// information, not from configuration.
	Clients map[string]string `json:"clients"`
}

var scenarios = map[string]func() c.Scenario{
	"core":           c.ScenarioCore,
	"multipartition": c.ScenarioMultiPartition,
	"transactions":   c.ScenarioTransactions,
}

var drivers = map[string]func() c.Driver{
	"kgo":       func() c.Driver { return c.NewFranzGoDriver() },
	"sarama":    func() c.Driver { return c.NewSaramaDriver() },
	"segmentio": func() c.Driver { return c.NewSegmentioDriver() },
}

func main() {
	matrixPath := flag.String("matrix", "", "path to matrix/protocol.yaml")
	oracleAddr := flag.String("oracle", os.Getenv("ORACLE_BOOTSTRAP"), "Kafka oracle bootstrap address")
	kubemqAddr := flag.String("kubemq", os.Getenv("KUBEMQ_BOOTSTRAP"), "KubeMQ bootstrap address")
	out := flag.String("out", os.Getenv("TRACK_RESULTS"), "results JSON to write")
	dumpDir := flag.String("dump-dir", "", "where to write raw transcripts (not committed); empty disables")
	only := flag.String("only", "", "comma-separated row ids to run; the rest are NOT-RUN")
	rowTimeout := flag.Duration("row-timeout", 4*time.Minute, "bound on one row (both legs)")
	apikeysPath := flag.String("apikeys", "", "committed matrix/apikeys.yaml; empty skips the ApiKey rows")
	impactsPath := flag.String("apikey-impacts", "", "matrix/apikey-impacts.yaml")
	gapsPath := flag.String("gaps", "", "docs/gaps.md, the pre-registered gap register")
	oracleVer := flag.String("oracle-version", os.Getenv("KAFKA_VERSION"), "Kafka version (recorded in the ApiKey matrix)")
	kubemqVer := flag.String("kubemq-version", os.Getenv("KUBEMQ_VERSION"), "KubeMQ version (recorded in the ApiKey matrix)")
	flag.Parse()

	if *matrixPath == "" || *out == "" {
		fatal("need -matrix and -out (or TRACK_RESULTS)")
	}
	m, err := loadMatrix(*matrixPath)
	if err != nil {
		fatal(err.Error())
	}
	onlySet := map[string]bool{}
	for _, id := range strings.Split(*only, ",") {
		if id = strings.TrimSpace(id); id != "" {
			onlySet[id] = true
		}
	}

	startedAt := time.Now().UTC()
	res := Results{Schema: 1, Track: m.Track, Clients: clientVersions()}
	for _, r := range m.Rows {
		row := Row{ID: r.ID, Kind: "behaviour", Title: r.Title, Provenance: r.Provenance, Source: r.Source}
		start := time.Now()
		switch {
		case len(onlySet) > 0 && !onlySet[r.ID]:
			row.Verdict, row.Detail = "NOT-RUN", "excluded by -only"
		case scenarios[r.Scenario] == nil:
			row.Verdict, row.Detail = "NOT-RUN", fmt.Sprintf("scenario %q is not implemented by this suite", r.Scenario)
		case drivers[r.Driver] == nil:
			row.Verdict, row.Detail = "NOT-RUN", fmt.Sprintf("driver %q is not implemented by this suite", r.Driver)
		default:
			ctx, cancel := context.WithTimeout(context.Background(), *rowTimeout)
			runRow(ctx, &row, scenarios[r.Scenario](), drivers[r.Driver](), *oracleAddr, *kubemqAddr, *dumpDir)
			cancel()
		}
		row.DurationMS = time.Since(start).Milliseconds()
		fmt.Fprintf(os.Stderr, "[protocol] %-34s %-7s %6dms %s\n", row.ID, row.Verdict, row.DurationMS, firstLine(row.Detail))
		res.Rows = append(res.Rows, row)
	}

	if *apikeysPath != "" {
		reg, err := gaps.Load(*gapsPath)
		if err != nil {
			fatal("gap register: " + err.Error())
		}
		runDate := startedAt
		if v := os.Getenv("CERT_RUN_STARTED"); v != "" {
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				runDate = t
			}
		}
		for _, row := range apiKeyRows(*apikeysPath, *impactsPath, *oracleAddr, *kubemqAddr, *oracleVer, *kubemqVer, reg, runDate) {
			if len(onlySet) > 0 && !onlySet[row.ID] {
				row = Row{ID: row.ID, Kind: row.Kind, Title: row.Title, Provenance: row.Provenance, Source: row.Source, Verdict: "NOT-RUN", Detail: "excluded by -only"}
			}
			res.Rows = append(res.Rows, row)
		}
		fmt.Fprintf(os.Stderr, "[protocol] ApiKey and feature rows: %s\n", tally(res.Rows, "apikey.", "feature.", "apikeys."))
	}

	if err := writeJSON(*out, res); err != nil {
		fatal(err.Error())
	}
}

func runRow(ctx context.Context, row *Row, sc c.Scenario, d c.Driver, oracleAddr, kubemqAddr, dumpDir string) {
	// Oracle leg first. Any failure here makes the row VOID: the oracle did not give us a
	// reference to compare against.
	oracleTr, oracleLeg := runLeg(ctx, sc, d, c.OracleTargetName, oracleAddr)
	row.Oracle = oracleLeg
	dump(dumpDir, row.ID, c.OracleTargetName, oracleTr)
	if !oracleLeg.OK {
		row.Verdict, row.Detail = "VOID", "oracle leg failed: "+oracleLeg.Error
		return
	}
	full := d.Kind() == c.DriverKindFullTranscript
	if full {
		if p := c.InvariantProblems(sc.Name, oracleTr); len(p) > 0 {
			oracleLeg.Problems = p
			row.Verdict, row.Detail = "VOID", "oracle broke a scenario invariant: "+strings.Join(p, "; ")
			return
		}
		// Rig self-check: the oracle against franz-go's in-memory kfake broker must read
		// identical, or this rig cannot tell a real difference from its own noise.
		// A disagreement here is not an oracle failure, so it is not VOID: the rig itself cannot
		// measure this row, which is NOT-RUN and fails the report as prominently as a FAIL.
		if detail, ok := rigSelfCheck(ctx, sc, d, oracleTr, dumpDir, row.ID); !ok {
			row.Verdict, row.Detail = "NOT-RUN", "rig self-check failed: the oracle and franz-go's kfake broker disagree, so this rig cannot measure the row: "+detail
			return
		}
	}

	kubemqTr, kubemqLeg := runLeg(ctx, sc, d, c.KubeMQTargetName, kubemqAddr)
	row.KubeMQ = kubemqLeg
	dump(dumpDir, row.ID, c.KubeMQTargetName, kubemqTr)
	if !kubemqLeg.OK {
		row.Verdict, row.Detail = "FAIL", "KubeMQ leg failed: "+kubemqLeg.Error
		return
	}
	if !full {
		row.Verdict = "PASS"
		row.Detail = fmt.Sprintf("round-trip succeeded on both brokers (%d oracle events, %d KubeMQ events)", oracleLeg.Events, kubemqLeg.Events)
		return
	}
	var problems []string
	if p := c.InvariantProblems(sc.Name, kubemqTr); len(p) > 0 {
		kubemqLeg.Problems = p
		problems = append(problems, p...)
	}
	diff := c.CompareTranscripts(oracleTr, kubemqTr)
	if diff.Verdict != c.VerdictIdentical {
		problems = append(problems, "transcript differs from the oracle (- oracle, + KubeMQ):\n"+diff.Diff)
		problems = append(problems, diff.InvariantErrors...)
	}
	if len(problems) > 0 {
		row.Verdict, row.Detail = "FAIL", strings.Join(problems, "\n")
		return
	}
	row.Verdict = "PASS"
	row.Detail = fmt.Sprintf("normalized transcript identical to the oracle (%d events); invariants hold", kubemqLeg.Events)
}

func runLeg(ctx context.Context, sc c.Scenario, d c.Driver, name, addr string) (c.Transcript, *Leg) {
	leg := &Leg{}
	start := time.Now()
	defer func() { leg.MS = time.Since(start).Milliseconds() }()
	target, err := c.NewAddrTarget(ctx, name, addr)
	if err != nil {
		leg.Error = err.Error()
		return c.Transcript{}, leg
	}
	defer target.Close()
	tr, err := runDriver(ctx, d, sc, target)
	if err != nil {
		leg.Error = err.Error()
		if errors.Is(err, c.ErrInfra) {
			leg.Error = "infrastructure: " + leg.Error
		}
		return tr, leg
	}
	leg.OK, leg.Events, leg.Client = true, len(tr.Events), tr.ClientVersion
	return tr, leg
}

func rigSelfCheck(ctx context.Context, sc c.Scenario, d c.Driver, oracleTr c.Transcript, dumpDir, rowID string) (string, bool) {
	kf, err := c.NewKfakeTarget()
	if err != nil {
		return err.Error(), false
	}
	defer kf.Close()
	kfTr, err := runDriver(ctx, d, sc, kf)
	dump(dumpDir, rowID, "kfake", kfTr)
	if err != nil {
		return "kfake leg: " + err.Error(), false
	}
	diff := c.CompareTranscripts(oracleTr, kfTr)
	if diff.Verdict != c.VerdictIdentical {
		return diff.Diff + strings.Join(diff.InvariantErrors, "; "), false
	}
	return "", true
}

func runDriver(ctx context.Context, d c.Driver, sc c.Scenario, t c.Target) (c.Transcript, error) {
	if cd, ok := d.(c.ContextDriver); ok {
		return cd.RunContext(ctx, sc, t)
	}
	return d.Run(sc, t)
}

func loadMatrix(path string) (matrix, error) {
	var m matrix
	b, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	if err := yaml.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("parse %s: %w", path, err)
	}
	if m.Track == "" || len(m.Rows) == 0 {
		return m, fmt.Errorf("%s: no track or no rows", path)
	}
	seen := map[string]bool{}
	for _, r := range m.Rows {
		if r.ID == "" || seen[r.ID] {
			return m, fmt.Errorf("%s: empty or duplicate row id %q", path, r.ID)
		}
		if r.Provenance != "upstream" && r.Provenance != "kubemq-authored" {
			return m, fmt.Errorf("%s: row %s: provenance must be upstream or kubemq-authored", path, r.ID)
		}
		seen[r.ID] = true
	}
	return m, nil
}

func dump(dir, rowID, target string, tr c.Transcript) {
	if dir == "" {
		return
	}
	_ = os.MkdirAll(dir, 0o755)
	b, _ := json.MarshalIndent(tr, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, rowID+"."+target+".json"), b, 0o644)
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i] + " ..."
	}
	return s
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "[protocol] ERROR:", msg)
	os.Exit(2)
}

// apiKeyRows gives one verdict per ApiKey and per ApiVersions feature, from a live query of
// both brokers, plus one row proving the committed matrix still matches the brokers.
func apiKeyRows(committedPath, impactsPath, oracleAddr, kubemqAddr, oracleVer, kubemqVer string, reg map[string]gaps.Entry, runDate time.Time) []Row {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	start := time.Now()
	committed, rerr := os.ReadFile(committedPath)
	if kubemqVer == "latest" {
		// The public kit runs the latest image: compare what both brokers advertise, not the
		// version name the committed matrix was generated for.
		kubemqVer = committedKubeMQVersion(committed)
	}
	doc, missing, err := apikeys.Generate(ctx, oracleAddr, kubemqAddr, impactsPath, oracleVer, kubemqVer)
	if err != nil {
		v := "FAIL"
		if strings.HasPrefix(err.Error(), "oracle:") {
			v = "VOID"
		}
		return []Row{{ID: "apikeys.matrix-current", Kind: "advertisement", Title: matrixCurrentTitle, Provenance: "kubemq-authored", Source: apiKeySource,
			Verdict: v, Detail: "ApiVersions query failed: " + err.Error(), DurationMS: time.Since(start).Milliseconds()}}
	}
	live, err := apikeys.Render(doc)
	if err != nil {
		fatal(err.Error())
	}
	rows := classifyAPIKeys(doc, missing, live, committed, rerr, reg, runDate)
	rows[0].DurationMS = time.Since(start).Milliseconds()
	return rows
}

// committedKubeMQVersion is the version in the committed matrix's "kubemq: kubemq-next <v>" line.
func committedKubeMQVersion(committed []byte) string {
	for _, line := range strings.Split(string(committed), "\n") {
		if v, ok := strings.CutPrefix(line, "kubemq: kubemq-next "); ok {
			return strings.TrimSpace(v)
		}
	}
	return "latest"
}

const (
	apiKeySource       = "live ApiVersions of both brokers (suites/protocol/apikeys)"
	matrixCurrentTitle = "Committed matrix/apikeys.yaml matches the live brokers"
)

// classifyAPIKeys is the pure verdict logic of apiKeyRows, unit-tested without brokers.
//
//	apikeys.matrix-current  PASS when the committed matrix equals the live one and every differing
//	                        key has a curated impact; FAIL otherwise.
//	apikey.<n>              PASS when KubeMQ advertises at least the oracle's range (or a key the
//	                        oracle does not have); otherwise GAP when docs/gaps.md registers this
//	                        exact deviation with a sign-off dated no later than the run, else FAIL.
//	feature.<name>          the same rule for ApiVersions feature flags.
func classifyAPIKeys(doc apikeys.Doc, missing []int16, live, committed []byte, readErr error, reg map[string]gaps.Entry, runDate time.Time) []Row {
	current := Row{ID: "apikeys.matrix-current", Kind: "advertisement", Title: matrixCurrentTitle, Provenance: "kubemq-authored", Source: apiKeySource}
	switch {
	case readErr != nil:
		current.Verdict, current.Detail = "FAIL", "cannot read committed matrix: "+readErr.Error()
	case len(missing) > 0:
		current.Verdict, current.Detail = "FAIL", fmt.Sprintf("keys %v differ from the oracle and have no curated impact in matrix/apikey-impacts.yaml", missing)
	case string(live) != string(committed):
		current.Verdict, current.Detail = "FAIL", "committed matrix is stale: regenerate with suites/protocol/cmd/apikeys-gen"
	default:
		current.Verdict, current.Detail = "PASS", fmt.Sprintf("matrix matches: %v", doc.Summary)
	}
	rows := []Row{current}

	gapOrFail := func(id, observed string) (string, string) {
		if ok, reason := gaps.Registered(reg, id, observed, runDate); ok {
			return "GAP", reason
		} else {
			return "FAIL", reason
		}
	}
	for _, k := range doc.Keys {
		r := Row{ID: k.ID, Kind: "advertisement", Title: fmt.Sprintf("ApiKey %d %s: advertised versions", k.Key, k.Name),
			Provenance: k.Provenance, Source: apiKeySource}
		kq := "not advertised"
		if k.KubeMQMin != nil {
			kq = fmt.Sprintf("v%d-v%d", *k.KubeMQMin, *k.KubeMQMax)
		}
		switch k.Status {
		case "not-applicable":
			dev := apikeys.BrokerInternalDeviation(k)
			ok, why := gaps.Registered(reg, k.ID, dev, runDate)
			if ok && k.Scope == "broker-internal" && apikeys.IsBrokerInternalKey(k.Key) && k.KubeMQMin == nil && k.KubeMQMax == nil {
				r.Kind, r.Verdict, r.Deviation = "broker-internal", "NOT-APPLICABLE", dev
				why = "owner-approved scope exclusion, signed off: " + reg[k.ID].Raw
			} else {
				r.Verdict = "FAIL"
			}
			r.Detail = dev + ". " + why
		case "full":
			r.Verdict = "PASS"
			r.Detail = fmt.Sprintf("advertisement check only: KubeMQ advertises %s, covering the oracle's v%d-v%d", kq, k.OracleMin, k.OracleMax)
		case "kubemq-only":
			r.Verdict, r.Detail = "PASS", "advertisement check only: advertised by KubeMQ ("+kq+"); the oracle does not advertise it"
		default:
			dev := apikeys.KeyDeviation(k)
			var why string
			r.Verdict, why = gapOrFail(k.ID, dev)
			if r.Verdict == "GAP" {
				r.Deviation = dev
			}
			r.Detail = fmt.Sprintf("%s. Breaks: %s. %s %s.", dev, k.Breaks, k.Impact, why)
		}
		rows = append(rows, r)
	}
	for _, f := range doc.Features {
		r := Row{ID: f.ID, Kind: "advertisement", Title: "ApiVersions feature " + f.Feature, Provenance: f.Provenance, Source: apiKeySource}
		if f.Status == "same" || f.Status == "kubemq-only" {
			r.Verdict = "PASS"
			r.Detail = fmt.Sprintf("advertisement check only: KubeMQ supports %s, finalized %s; oracle supports %s, finalized %s", f.KubeMQSupported, f.KubeMQFinalized, f.OracleSupported, f.OracleFinalized)
		} else {
			dev := apikeys.FeatureDeviation(f)
			var why string
			r.Verdict, why = gapOrFail(f.ID, dev)
			if r.Verdict == "GAP" {
				r.Deviation = dev
			}
			r.Detail = dev + ". " + why + "."
		}
		rows = append(rows, r)
	}
	return rows
}

// clientVersions reads the client libraries compiled into this binary.
func clientVersions() map[string]string {
	out := map[string]string{}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return out
	}
	names := map[string]string{
		"github.com/twmb/franz-go":      "franz-go",
		"github.com/IBM/sarama":         "sarama",
		"github.com/segmentio/kafka-go": "segmentio-kafka-go",
	}
	for _, d := range bi.Deps {
		if n, ok := names[d.Path]; ok {
			out[n] = d.Version
		}
	}
	return out
}

func tally(rows []Row, prefixes ...string) string {
	n := map[string]int{}
	for _, r := range rows {
		for _, p := range prefixes {
			if strings.HasPrefix(r.ID, p) {
				n[r.Verdict]++
				break
			}
		}
	}
	return fmt.Sprintf("PASS %d, GAP %d, FAIL %d, VOID %d, NOT-RUN %d; broker-internal NOT-APPLICABLE %d", n["PASS"], n["GAP"], n["FAIL"], n["VOID"], n["NOT-RUN"], n["NOT-APPLICABLE"])
}
