// Command features-track runs the broker-feature rows of matrix/features.yaml (Track B). Each
// row's probe runs on the Kafka oracle, then on KubeMQ with only the bootstrap address changed.
//
//	PASS     both observations are identical
//	GAP      they differ exactly as a deviation pre-registered and signed off in docs/gaps.md
//	FAIL     they differ otherwise, or the KubeMQ probe could not complete
//	VOID     the oracle probe could not complete
//	NOT-RUN  the row names a probe this suite does not implement
//
// The deviation text is the field-by-field difference from probe.Diff, so a registration names
// the exact observed behaviour and turns FAIL again if the behaviour changes.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
)

type matrixRow struct {
	ID    string `yaml:"id"`
	Title string `yaml:"title"`
	Group string `yaml:"group"`
	Probe string `yaml:"probe"`
	// OptIn, when set, is the reason the row runs only with -include-opt-in; without the flag
	// the row is NOT-RUN with that reason, so the report still fails visibly.
	OptIn string `yaml:"opt_in"`
	// LastResult records the last observed outcome of an opt-in row, shown with its NOT-RUN.
	LastResult string `yaml:"last_result"`
	Provenance string `yaml:"provenance"`
	Source     string `yaml:"source"`
}

type matrix struct {
	Track string      `yaml:"track"`
	Rows  []matrixRow `yaml:"rows"`
}

// Leg is one broker's side of a row.
type Leg struct {
	OK          bool              `json:"ok"`
	Error       string            `json:"error,omitempty"`
	Observation probe.Observation `json:"observation,omitempty"`
	MS          int64             `json:"duration_ms"`
}

// Row is one verdict; same schema as every other track.
type Row struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	Title      string `json:"title"`
	Verdict    string `json:"verdict"`
	Provenance string `json:"provenance"`
	Source     string `json:"source"`
	Detail     string `json:"detail,omitempty"`
	// Deviation is the observed difference on a GAP row, exactly as docs/gaps.md signs it off;
	// the renderer checks it against the register again.
	Deviation  string `json:"deviation,omitempty"`
	Oracle     *Leg   `json:"oracle,omitempty"`
	KubeMQ     *Leg   `json:"kubemq,omitempty"`
	DurationMS int64  `json:"duration_ms"`
}

type Results struct {
	Schema  int               `json:"schema"`
	Track   string            `json:"track"`
	Rows    []Row             `json:"rows"`
	Clients map[string]string `json:"clients"`
}

func main() {
	matrixPath := flag.String("matrix", "", "matrix/features.yaml")
	oracleAddr := flag.String("oracle", os.Getenv("ORACLE_BOOTSTRAP"), "Kafka oracle bootstrap address")
	kubemqAddr := flag.String("kubemq", os.Getenv("KUBEMQ_BOOTSTRAP"), "KubeMQ bootstrap address")
	out := flag.String("out", os.Getenv("TRACK_RESULTS"), "results JSON to write")
	gapsPath := flag.String("gaps", "", "docs/gaps.md")
	only := flag.String("only", "", "comma-separated row ids to run; the rest are NOT-RUN")
	includeOptIn := flag.Bool("include-opt-in", false, "also run rows marked opt_in in the matrix")
	rowTimeout := flag.Duration("row-timeout", 4*time.Minute, "bound on one leg of one row")
	flag.StringVar(&oracleContainer, "oracle-container", os.Getenv("ORACLE_CONTAINER"), "Kafka oracle container (for container clients)")
	flag.StringVar(&kubemqContainer, "kubemq-container", os.Getenv("KUBEMQ_CONTAINER"), "KubeMQ container (for container clients)")
	flag.StringVar(&probe.JavaClient.ClientRun, "client-run", "", "runner/client-run.sh, which starts container clients")
	flag.StringVar(&probe.JavaClient.Image, "java-image", "", "Kafka image (by digest) whose kafka-console-consumer is the Java consumer")
	flag.Parse()
	if *matrixPath == "" || *out == "" {
		fatal("need -matrix and -out")
	}
	m, err := loadMatrix(*matrixPath)
	if err != nil {
		fatal(err.Error())
	}
	reg, err := gaps.Load(*gapsPath)
	if err != nil {
		fatal("gap register: " + err.Error())
	}
	runDate := time.Now().UTC()
	if v := os.Getenv("CERT_RUN_STARTED"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			runDate = t
		}
	}
	onlySet := map[string]bool{}
	for _, id := range strings.Split(*only, ",") {
		if id = strings.TrimSpace(id); id != "" {
			onlySet[id] = true
		}
	}

	res := Results{Schema: 1, Track: m.Track, Clients: clientVersions()}
	for _, r := range m.Rows {
		row := Row{ID: r.ID, Kind: "behaviour", Title: r.Title, Provenance: r.Provenance, Source: r.Source}
		start := time.Now()
		p := probe.Registry[r.Probe]
		switch {
		case len(onlySet) > 0 && !onlySet[r.ID]:
			row.Verdict, row.Detail = "NOT-RUN", "excluded by -only"
		case optInExcluded(r, *includeOptIn) != "":
			row.Verdict, row.Detail = "NOT-RUN", optInExcluded(r, *includeOptIn)
		case p == nil:
			row.Verdict, row.Detail = "NOT-RUN", fmt.Sprintf("probe %q is not implemented by this suite", r.Probe)
		default:
			runRow(&row, r.Probe, p, *oracleAddr, *kubemqAddr, *rowTimeout, reg, runDate)
		}
		row.DurationMS = time.Since(start).Milliseconds()
		fmt.Fprintf(os.Stderr, "[features] %-46s %-7s %6dms %s\n", row.ID, row.Verdict, row.DurationMS, row.Detail)
		res.Rows = append(res.Rows, row)
	}
	if err := writeJSON(*out, res); err != nil {
		fatal(err.Error())
	}
}

// The broker containers, for probes whose client runs in a container (probe.Env.Container).
var oracleContainer, kubemqContainer string

// lastRow is the id of the previous row, named when a broker turns out to be gone.
var lastRow string

// optInExcluded returns the NOT-RUN detail for an opt-in row run without -include-opt-in, or
// "" when the row runs.
func optInExcluded(r matrixRow, include bool) string {
	if r.OptIn == "" || include {
		return ""
	}
	d := "excluded: " + r.OptIn
	if r.LastResult != "" {
		d += ". Last observed: " + r.LastResult
	}
	return d
}

func runRow(row *Row, name string, p probe.Probe, oracleAddr, kubemqAddr string, timeout time.Duration, reg map[string]gaps.Entry, runDate time.Time) {
	defer func() { lastRow = row.ID }()
	if !answers(oracleAddr) {
		row.Verdict, row.Detail = "VOID", "the Kafka oracle does not answer (it stopped during or after row "+lastRow+")"
		return
	}
	if !answers(kubemqAddr) {
		row.Verdict, row.Detail = "NOT-RUN", "KubeMQ does not answer: it stopped during or after row "+lastRow+"; this row could not run"
		return
	}
	row.Oracle = runLeg(p, probe.Env{Bootstrap: oracleAddr, Target: "real-kafka", Container: oracleContainer}, timeout)
	row.Verdict, row.Detail = decide(name, row.Oracle, func() *Leg {
		return runLeg(p, probe.Env{Bootstrap: kubemqAddr, Target: "kubemq", Container: kubemqContainer}, timeout)
	}, row, reg, runDate)
	if row.KubeMQ != nil && !answers(kubemqAddr) {
		row.Detail += " KubeMQ stopped answering after this row."
	}
}

// decide maps the two legs to a verdict. Pure apart from running the KubeMQ leg, which it only
// does when the oracle leg is usable; unit-tested.
func decide(name string, oracle *Leg, runKubeMQ func() *Leg, row *Row, reg map[string]gaps.Entry, runDate time.Time) (string, string) {
	if !oracle.OK {
		return "VOID", "oracle leg failed: " + oracle.Error
	}
	if err := probe.CheckOracle(name, oracle.Observation); err != nil {
		return "VOID", "oracle precondition: " + err.Error()
	}
	kq := runKubeMQ()
	row.KubeMQ = kq
	if !kq.OK {
		return "FAIL", "KubeMQ leg failed: " + kq.Error
	}
	v, detail := Verdict(row.ID, oracle.Observation, kq.Observation, reg, runDate)
	if v == "GAP" {
		row.Deviation, _ = probe.Diff(oracle.Observation, kq.Observation)
	}
	return v, detail
}

// Verdict compares two observations under the gap register. Pure, for the mutation tests.
func Verdict(id string, oracle, kubemq probe.Observation, reg map[string]gaps.Entry, runDate time.Time) (string, string) {
	diff, same := probe.Diff(oracle, kubemq)
	if same {
		return "PASS", "observations identical: " + compact(oracle)
	}
	if ok, why := gaps.Registered(reg, id, diff, runDate); ok {
		return "GAP", diff + ". " + why + "."
	} else {
		return "FAIL", diff + ". " + why + "."
	}
}

func runLeg(p probe.Probe, env probe.Env, timeout time.Duration) (leg *Leg) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	// A probe that panics (for example on a malformed broker answer) fails its leg, never the
	// whole track.
	defer func() {
		if r := recover(); r != nil {
			leg = &Leg{OK: false, Error: fmt.Sprintf("probe panicked: %v", r), MS: time.Since(start).Milliseconds()}
		}
	}()
	obs, err := p(ctx, env)
	leg = &Leg{OK: err == nil, Observation: obs, MS: time.Since(start).Milliseconds()}
	if err != nil {
		leg.Error = err.Error()
	}
	return leg
}

// answers reports whether a broker answers a Kafka request (ApiVersions) within 10 s. A
// forwarded TCP port can accept connections for a container that has already stopped, so a
// TCP dial alone is not proof.
// A single failed attempt is retried: published ports on a container engine VM can refuse a
// connection for a moment.
func answers(addr string) bool {
	for attempt := 0; attempt < 3; attempt++ {
		cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
		if err != nil {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = cl.Ping(ctx)
		cancel()
		cl.Close()
		if err == nil {
			return true
		}
		time.Sleep(2 * time.Second)
	}
	return false
}

func compact(o probe.Observation) string {
	b, _ := json.Marshal(o)
	return string(b)
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
	sawOptIn := false
	for _, r := range m.Rows {
		if r.ID == "" || seen[r.ID] {
			return m, fmt.Errorf("%s: empty or duplicate row id %q", path, r.ID)
		}
		if r.Provenance != "upstream" && r.Provenance != "kubemq-authored" {
			return m, fmt.Errorf("%s: row %s: provenance must be upstream or kubemq-authored", path, r.ID)
		}
		if r.Source == "" {
			return m, fmt.Errorf("%s: row %s: source is mandatory", path, r.ID)
		}
		if r.OptIn == "" && sawOptIn {
			return m, fmt.Errorf("%s: row %s follows an opt-in row; opt-in rows must come last", path, r.ID)
		}
		sawOptIn = sawOptIn || r.OptIn != ""
		seen[r.ID] = true
	}
	return m, nil
}

func clientVersions() map[string]string {
	out := map[string]string{}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if d.Path == "github.com/twmb/franz-go" {
				out["franz-go"] = d.Version
			}
		}
	}
	return out
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

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "[features] ERROR:", msg)
	os.Exit(2)
}
