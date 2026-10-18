// Command tools-track runs the Kafka platform-tool rows of matrix/tools.yaml (Track D). Each row's
// probe runs Apache Kafka's own tools from the pinned Kafka image against the Kafka oracle, then
// against KubeMQ with only the bootstrap address changed.
//
//	PASS     both observations are identical
//	GAP      they differ exactly as a deviation pre-registered and signed off in docs/gaps.md
//	FAIL     they differ otherwise, or the KubeMQ probe could not complete
//	VOID     the oracle probe could not complete
//	NOT-RUN  the row names a probe this suite does not implement, or was excluded
//
// Verdict rules and the deviation text are the features track's (probe.Diff, gaps.Registered),
// so a registration names the exact observed behaviour and turns FAIL if it changes.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	fprobe "github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/box"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/probe"
)

type matrixRow struct {
	ID         string `yaml:"id"`
	Title      string `yaml:"title"`
	Group      string `yaml:"group"`
	Probe      string `yaml:"probe"`
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
	Infra       string            `json:"infrastructure_error,omitempty"` // the leg is not evidence: see box.InfraCount
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
	Deviation  string `json:"deviation,omitempty"` // the exact observed difference, bound to the register's signed text
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
	matrixPath := flag.String("matrix", "", "matrix/tools.yaml")
	oracleAddr := flag.String("oracle", os.Getenv("ORACLE_BOOTSTRAP"), "Kafka oracle bootstrap address, as containers reach it")
	kubemqAddr := flag.String("kubemq", os.Getenv("KUBEMQ_BOOTSTRAP"), "KubeMQ bootstrap address, as containers reach it")
	out := flag.String("out", os.Getenv("TRACK_RESULTS"), "results JSON to write")
	gapsPath := flag.String("gaps", "", "docs/gaps.md")
	ctrPath := flag.String("ctr", "", "suites/tools/ctr.sh")
	only := flag.String("only", "", "comma-separated row ids to run; the rest are NOT-RUN")
	notRun := flag.String("not-run", "", "record every row NOT-RUN with this reason and start nothing (an opt-in track not opted into)")
	rowTimeout := flag.Duration("row-timeout", 60*time.Minute, "bound on one leg of one row")
	flag.Parse()
	if *matrixPath == "" || *out == "" || *ctrPath == "" {
		fatal("need -matrix, -out and -ctr")
	}
	box.Ctr = *ctrPath
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

	if *notRun != "" {
		res := Results{Schema: 1, Track: m.Track, Clients: map[string]string{}}
		for _, r := range m.Rows {
			res.Rows = append(res.Rows, Row{ID: r.ID, Kind: "behaviour", Title: r.Title, Verdict: "NOT-RUN", Provenance: r.Provenance, Source: r.Source, Detail: *notRun})
		}
		if err := writeJSON(*out, res); err != nil {
			fatal(err.Error())
		}
		return
	}
	ctx := context.Background()
	runLabel := os.Getenv("TOOLS_RUN_LABEL")
	tb, err := box.StartToolbox(ctx, "kcert-tools-"+runLabel)
	if err != nil {
		fatal("toolbox: " + err.Error())
	}
	defer tb.Remove()

	res := Results{Schema: 1, Track: m.Track, Clients: clientVersions(ctx, tb)}
	for _, r := range m.Rows {
		row := Row{ID: r.ID, Kind: "behaviour", Title: r.Title, Provenance: r.Provenance, Source: r.Source}
		start := time.Now()
		p := probe.Registry[r.Probe]
		switch {
		case len(onlySet) > 0 && !onlySet[r.ID]:
			row.Verdict, row.Detail = "NOT-RUN", "excluded by -only"
		case p == nil:
			row.Verdict, row.Detail = "NOT-RUN", fmt.Sprintf("probe %q is not implemented by this suite", r.Probe)
		default:
			if !tb.Running(ctx) && !box.EngineDown() {
				fatal("the toolbox container " + tb.Name + " stopped; the remaining rows are NOT-RUN")
			}
			runRow(&row, r.Probe, p, tb, *oracleAddr, *kubemqAddr, *rowTimeout, reg, runDate)
		}
		row.DurationMS = time.Since(start).Milliseconds()
		if box.EngineDown() {
			// Nothing observed after the engine stopped answering is evidence about a broker.
			row.Verdict, row.Detail, row.Oracle, row.KubeMQ, row.Deviation = "NOT-RUN", "the container engine stopped answering during this row: infrastructure failure, nothing certified", nil, nil, ""
			res.Rows = append(res.Rows, row)
			_ = writeJSON(*out, res)
			fatal("container engine unreachable; the remaining rows are NOT-RUN")
		}
		fmt.Fprintf(os.Stderr, "[tools] %-46s %-7s %6dms %s\n", row.ID, row.Verdict, row.DurationMS, row.Detail)
		res.Rows = append(res.Rows, row)
		if err := writeJSON(*out, res); err != nil { // partial results survive a crash of the track
			fatal(err.Error())
		}
	}
}

// reachable reports whether the toolbox reaches a broker: the tools run in containers, so the
// check runs there too.
func reachable(ctx context.Context, tb *box.Box, addr string) bool {
	host, port, err := net.SplitHostPort(strings.Split(addr, ",")[0])
	if err != nil {
		return false
	}
	return tb.Sh(ctx, "", 10*time.Second, fmt.Sprintf("nc -z -w 3 %s %s", host, port)).Exit == 0
}

// lastRow is the id of the previous row, named when a broker turns out to be gone.
var lastRow string

func runRow(row *Row, name string, p probe.Probe, tb *box.Box, oracleAddr, kubemqAddr string, timeout time.Duration, reg map[string]gaps.Entry, runDate time.Time) {
	defer func() { lastRow = row.ID }()
	ctx := context.Background()
	if !reachable(ctx, tb, oracleAddr) {
		row.Verdict, row.Detail = "VOID", "the Kafka oracle does not accept connections (it stopped during or after row "+lastRow+")"
		return
	}
	if !reachable(ctx, tb, kubemqAddr) {
		row.Verdict, row.Detail = "FAIL", "KubeMQ does not accept connections: it stopped during or after row "+lastRow+"; this row could not run"
		return
	}
	row.Oracle = runLeg(p, probe.Env{Bootstrap: oracleAddr, Target: "real-kafka", Box: tb}, timeout)
	if row.Oracle.Infra != "" {
		row.Verdict, row.Detail = "NOT-RUN", "infrastructure failure during the oracle leg, nothing certified: "+row.Oracle.Infra
		return
	}
	if !row.Oracle.OK {
		row.Verdict, row.Detail = "VOID", "oracle leg failed: "+row.Oracle.Error
		return
	}
	if err := probe.CheckOracle(name, row.Oracle.Observation); err != nil {
		row.Verdict, row.Detail = "VOID", "oracle precondition: "+err.Error()
		return
	}
	row.KubeMQ = runLeg(p, probe.Env{Bootstrap: kubemqAddr, Target: "kubemq", Box: tb}, timeout)
	if row.KubeMQ.Infra != "" {
		row.Verdict, row.Detail = "NOT-RUN", "infrastructure failure during the KubeMQ leg, nothing certified: "+row.KubeMQ.Infra
		return
	}
	if !row.KubeMQ.OK {
		row.Verdict, row.Detail = "FAIL", "KubeMQ leg failed: "+row.KubeMQ.Error
		return
	}
	row.Verdict, row.Detail, row.Deviation = compareLegs(row.ID, name, row.Oracle.Observation, row.KubeMQ.Observation, reg, runDate)
}

// compareLegs is the verdict, detail and deviation of a row whose two legs completed. The legs keep
// their whole observations; the probe's informational fields (probe.Informational) are left out of
// the comparison and named in the detail.
func compareLegs(id, name string, oracle, kubemq probe.Observation, reg map[string]gaps.Entry, runDate time.Time) (verdict, detail, deviation string) {
	o, k := probe.Compared(name, oracle), probe.Compared(name, kubemq)
	verdict, detail = Verdict(id, o, k, reg, runDate)
	if diff, same := fprobe.Diff(o, k); !same {
		deviation = diff
	}
	if note := probe.InformationalNote(name, oracle, kubemq); note != "" {
		detail += " (" + note + ")"
	}
	return verdict, detail, deviation
}

// Verdict compares two observations under the gap register. Pure, for the mutation tests.
func Verdict(id string, oracle, kubemq probe.Observation, reg map[string]gaps.Entry, runDate time.Time) (string, string) {
	diff, same := fprobe.Diff(oracle, kubemq)
	if same {
		return "PASS", "observations identical: " + compact(oracle)
	}
	if ok, why := gaps.Registered(reg, id, diff, runDate); ok {
		return "GAP", diff + ". " + why + "."
	} else {
		return "FAIL", diff + ". " + why + "."
	}
}

func runLeg(p probe.Probe, env probe.Env, timeout time.Duration) *Leg {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	before := box.InfraCount()
	obs, err := p(ctx, env)
	leg := &Leg{OK: err == nil, Observation: obs, MS: time.Since(start).Milliseconds()}
	if err != nil {
		leg.Error = err.Error()
	}
	if box.InfraCount() != before {
		leg.OK, leg.Infra = false, box.InfraLast()
	} else if ctx.Err() != nil && err == nil {
		// A probe that kept going past its deadline recorded calls that failed fast: not evidence.
		leg.OK, leg.Error = false, "the leg did not finish within "+timeout.String()
	}
	return leg
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
		seen[r.ID] = true
	}
	return m, nil
}

// clientVersions records what actually ran: the kafka-clients version the image's tools report
// and the image digest.
func clientVersions(ctx context.Context, tb *box.Box) map[string]string {
	out := map[string]string{"kafka-tools-image": box.Image()}
	r := tb.Tool(ctx, "", 60*time.Second, "kafka-topics.sh", "--version")
	if f := strings.Fields(r.Stdout); r.Exit == 0 && len(f) > 0 {
		out["kafka-tools"] = f[0]
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
	fmt.Fprintln(os.Stderr, "[tools] ERROR:", msg)
	os.Exit(2)
}
