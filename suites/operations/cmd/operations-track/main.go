// Command operations-track runs the metric rows of matrix/operations.yaml (Track G). Each
// workload named by a row runs once per broker, Kafka oracle first, alone on that broker,
// between two metric scrapes; every row naming that workload is judged from the same pair of
// scrapes.
//
//	PASS     both legs meet the row's expectation (series present, counter monotonic, change or
//	         value as the workload requires)
//	GAP      the KubeMQ leg differs exactly as a deviation pre-registered and signed off in
//	         docs/gaps.md (including "KubeMQ exposes no equivalent series")
//	FAIL     it differs otherwise, or the KubeMQ workload or scrape could not complete
//	VOID     the oracle leg does not meet the expectation, or its workload or scrape failed
//	NOT-RUN  excluded by -only, or a workload this suite does not implement
//
// Kafka is scraped through -oracle-scrape: an executable that takes JMX ObjectName patterns as
// arguments and prints kafka-jmx.sh CSV (suites/operations/oracle-jmx.sh). KubeMQ is scraped
// over HTTP at -kubemq-metrics.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/operations/config"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/operations/metric"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/operations/workload"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
)

// Leg is one broker's side of a row.
type Leg struct {
	OK          bool           `json:"ok"`
	Error       string         `json:"error,omitempty"`
	Observation map[string]any `json:"observation,omitempty"`
	Measured    string         `json:"measured,omitempty"`
	MS          int64          `json:"duration_ms"`
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
	Deviation  string `json:"deviation,omitempty"` // observed deviation, bound by the renderer to docs/gaps.md
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

// side is one workload's outcome on one broker.
type side struct {
	before, after metric.Scrape
	run           *workload.Run
	err           error // workload or scrape failure
	ms            int64
}

type scraper func(ctx context.Context) (metric.Scrape, error)

func main() {
	matrixPath := flag.String("matrix", "", "matrix/operations.yaml")
	oracleAddr := flag.String("oracle", os.Getenv("ORACLE_BOOTSTRAP"), "Kafka oracle bootstrap address")
	kubemqAddr := flag.String("kubemq", os.Getenv("KUBEMQ_BOOTSTRAP"), "KubeMQ bootstrap address")
	oracleScrape := flag.String("oracle-scrape", "", "executable printing kafka-jmx.sh CSV for the ObjectName patterns given as arguments")
	kubemqMetrics := flag.String("kubemq-metrics", "", "KubeMQ Prometheus endpoint, http://host:port/metrics")
	out := flag.String("out", os.Getenv("TRACK_RESULTS"), "results JSON to write")
	gapsPath := flag.String("gaps", "", "docs/gaps.md")
	only := flag.String("only", "", "comma-separated row ids to run; the rest are NOT-RUN")
	legTimeout := flag.Duration("leg-timeout", 3*time.Minute, "bound on one workload with its two scrapes on one broker")
	flag.Parse()
	if *matrixPath == "" || *out == "" || *oracleScrape == "" || *kubemqMetrics == "" {
		fatal("need -matrix, -out, -oracle-scrape and -kubemq-metrics")
	}
	metric.ProbeNames = map[string]bool{}
	for n := range config.Registry {
		metric.ProbeNames[n] = true
	}
	m, err := metric.LoadMatrix(*matrixPath)
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
	selected := func(r metric.Row) bool { return len(onlySet) == 0 || onlySet[r.ID] }

	patterns := JMXPatterns(m.Rows)
	oracleScr := func(ctx context.Context) (metric.Scrape, error) { return scrapeJMX(ctx, *oracleScrape, patterns) }
	kubemqScr := func(ctx context.Context) (metric.Scrape, error) { return scrapeHTTP(ctx, *kubemqMetrics) }

	// Workloads in first-use order, each run once per broker.
	var order []string
	needed := map[string]bool{}
	for _, r := range m.Rows {
		if selected(r) && r.Probe == "" && !needed[r.Workload] {
			needed[r.Workload] = true
			order = append(order, r.Workload)
		}
	}
	oracleSides, kubemqSides := map[string]*side{}, map[string]*side{}
	for _, w := range order {
		if !reachable(*oracleAddr) {
			oracleSides[w] = &side{err: fmt.Errorf("the Kafka oracle does not accept connections")}
		} else {
			oracleSides[w] = runSide(w, *oracleAddr, "real-kafka", oracleScr, *legTimeout)
		}
		if !reachable(*kubemqAddr) {
			kubemqSides[w] = &side{err: fmt.Errorf("KubeMQ does not accept connections")}
		} else {
			kubemqSides[w] = runSide(w, *kubemqAddr, "kubemq", kubemqScr, *legTimeout)
		}
		logf("workload %-16s oracle %s; KubeMQ %s", w, sideText(oracleSides[w]), sideText(kubemqSides[w]))
	}

	res := Results{Schema: 1, Track: m.Track, Clients: clientVersions()}
	for _, r := range m.Rows {
		row := Row{ID: r.ID, Kind: "behaviour", Title: r.Title, Provenance: r.Provenance, Source: r.Source}
		if !selected(r) {
			row.Verdict, row.Detail = "NOT-RUN", "excluded by -only"
		} else if r.Probe != "" {
			runProbe(&row, config.Registry[r.Probe], config.Expected[r.Probe], *oracleAddr, *kubemqAddr, *legTimeout, reg, runDate)
		} else {
			JudgeRow(&row, r, oracleSides[r.Workload], kubemqSides[r.Workload], reg, runDate)
		}
		logf("%-44s %-7s %s", row.ID, row.Verdict, row.Detail)
		res.Rows = append(res.Rows, row)
	}
	for _, s := range kubemqSides {
		s.run.Close()
	}
	for _, s := range oracleSides {
		s.run.Close()
	}
	if err := writeJSON(*out, res); err != nil {
		fatal(err.Error())
	}
}

// runSide scrapes, runs the workload, scrapes again. The workload's open resources (the idle
// workload's connection) stay open until the track closes them after judging.
func runSide(name, bootstrap, target string, scrape scraper, timeout time.Duration) *side {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	s := &side{}
	defer func() { s.ms = time.Since(start).Milliseconds() }()
	if s.before, s.err = scrape(ctx); s.err != nil {
		s.err = fmt.Errorf("scrape before the workload: %w", s.err)
		return s
	}
	if s.run, s.err = workload.Execute(ctx, name, bootstrap, target); s.err != nil {
		s.err = fmt.Errorf("workload %s: %w", name, s.err)
		return s
	}
	if s.after, s.err = scrape(ctx); s.err != nil {
		s.err = fmt.Errorf("scrape after the workload: %w", s.err)
		return s
	}
	s.after = append(s.after, s.run.Extra...)
	return s
}

// JudgeRow fills the row's legs and verdict from the two sides of its workload. Pure apart from
// its inputs, for the tests.
func JudgeRow(row *Row, r metric.Row, o, k *side, reg map[string]gaps.Entry, runDate time.Time) {
	if o == nil || k == nil {
		row.Verdict, row.Detail = "NOT-RUN", "workload "+r.Workload+" did not run"
		return
	}
	row.DurationMS = o.ms + k.ms
	if o.err != nil {
		row.Oracle = &Leg{Error: o.err.Error(), MS: o.ms}
		row.Verdict, row.Detail = "VOID", "oracle leg failed: "+o.err.Error()
		return
	}
	or, err := metric.Check(o.before, o.after, r.Kafka.Bind(o.run.Vars), r.Expect, o.run.Quantities)
	if err != nil {
		row.Verdict, row.Detail = "VOID", "oracle expectation could not be evaluated: "+err.Error()
		return
	}
	row.Oracle = &Leg{OK: true, Observation: or.Observation(), Measured: or.String(), MS: o.ms}
	if k.err != nil {
		if !or.Met {
			row.Verdict, row.Detail = "VOID", "oracle precondition failed: Kafka "+or.String()
			return
		}
		row.KubeMQ = &Leg{Error: k.err.Error(), MS: k.ms}
		row.Verdict, row.Detail = "FAIL", "KubeMQ leg failed: "+k.err.Error()
		return
	}
	var kr *metric.Result
	if r.KubeMQ != nil {
		res, err := metric.Check(k.before, k.after, r.KubeMQ.Bind(k.run.Vars), r.Expect, k.run.Quantities)
		if err != nil {
			row.Verdict, row.Detail = "FAIL", "KubeMQ expectation could not be evaluated: "+err.Error()
			return
		}
		kr = &res
		row.KubeMQ = &Leg{OK: true, Observation: res.Observation(), Measured: res.String(), MS: k.ms}
	} else {
		row.KubeMQ = &Leg{OK: true, Observation: map[string]any{"series_present": "absent"}, Measured: "no equivalent series mapped", MS: k.ms}
	}
	verdict, dev, detail := metric.Compare(or, kr)
	switch verdict {
	case "VOID", "PASS":
		row.Verdict, row.Detail = verdict, detail
	default:
		row.Deviation = dev
		if ok, why := gaps.Registered(reg, r.ID, dev, runDate); ok {
			row.Verdict, row.Detail = "GAP", dev+". "+why+". "+detail
		} else {
			row.Verdict, row.Detail = "FAIL", dev+". "+why+". "+detail
		}
	}
}

// runProbe runs a configuration-mapping probe on the oracle, then on KubeMQ, and judges it.
func runProbe(row *Row, p config.Probe, expected func(config.Observation) bool, oracleAddr, kubemqAddr string, timeout time.Duration, reg map[string]gaps.Entry, runDate time.Time) {
	leg := func(addr, target string) (*Leg, config.Observation) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		start := time.Now()
		if !reachable(addr) {
			return &Leg{Error: "broker does not accept connections"}, nil
		}
		obs, err := p(ctx, addr, target)
		l := &Leg{OK: err == nil, Observation: obs, MS: time.Since(start).Milliseconds()}
		if err != nil {
			l.Error = err.Error()
		}
		return l, obs
	}
	var o, k config.Observation
	row.Oracle, o = leg(oracleAddr, "real-kafka")
	if !row.Oracle.OK {
		row.Verdict, row.Detail = "VOID", "oracle leg failed: "+row.Oracle.Error
		return
	}
	if expected != nil && !expected(o) {
		b, _ := json.Marshal(o)
		row.Verdict, row.Detail = "VOID", "oracle precondition failed: Kafka showed "+string(b)
		return
	}
	row.KubeMQ, k = leg(kubemqAddr, "kubemq")
	row.DurationMS = row.Oracle.MS + row.KubeMQ.MS
	if !row.KubeMQ.OK {
		row.Verdict, row.Detail = "FAIL", "KubeMQ leg failed: "+row.KubeMQ.Error
		return
	}
	JudgeProbe(row, o, k, reg, runDate)
}

// JudgeProbe compares two probe observations under the gap register. Pure, for the tests.
func JudgeProbe(row *Row, o, k config.Observation, reg map[string]gaps.Entry, runDate time.Time) {
	dev := metric.Diff(o, k)
	if dev == "" {
		b, _ := json.Marshal(o)
		row.Verdict, row.Detail = "PASS", "observations identical: "+string(b)
		return
	}
	row.Deviation = dev
	if ok, why := gaps.Registered(reg, row.ID, dev, runDate); ok {
		row.Verdict, row.Detail = "GAP", dev+". "+why
	} else {
		row.Verdict, row.Detail = "FAIL", dev+". "+why
	}
}

// JMXPatterns returns one ObjectName pattern per Kafka MBean family the rows read:
// "<domain>:type=<type>[,name=<name>],*". "admin:" selectors are computed by the track.
func JMXPatterns(rows []metric.Row) []string {
	set := map[string]bool{}
	for _, r := range rows {
		n := r.Kafka.Name
		if strings.HasPrefix(n, "admin:") {
			continue
		}
		a := strings.LastIndexByte(n, ':')
		if a <= 0 {
			continue
		}
		set[n[:a]+",*"] = true
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func scrapeJMX(ctx context.Context, cmd string, patterns []string) (metric.Scrape, error) {
	var stdout, stderr bytes.Buffer
	c := exec.CommandContext(ctx, cmd, patterns...)
	c.Stdout, c.Stderr = &stdout, &stderr
	if err := c.Run(); err != nil {
		return nil, fmt.Errorf("%s: %w: %s", filepath.Base(cmd), err, lastLine(stderr.String()))
	}
	return metric.ParseJmxCSV(&stdout)
}

func scrapeHTTP(ctx context.Context, url string) (metric.Scrape, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return metric.ParsePrometheus(resp.Body)
}

func reachable(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func sideText(s *side) string {
	if s.err != nil {
		return "failed: " + s.err.Error()
	}
	return fmt.Sprintf("ok in %dms %v", s.ms, s.run.Quantities)
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
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

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[operations] "+format+"\n", args...)
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "[operations] ERROR:", msg)
	os.Exit(2)
}
