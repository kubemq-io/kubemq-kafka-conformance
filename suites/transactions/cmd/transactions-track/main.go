// Command transactions-track runs the rows of matrix/transactions.yaml (Track B transactions,
// idempotent producer and exactly-once). Each row names a client and a scenario. The client's
// driver runs in a client container, first against the Kafka oracle, then against KubeMQ with
// only the bootstrap address changed; package verify turns each driver's facts into an
// observation and the two observations are compared field by field.
//
//	PASS     both observations are identical
//	GAP      they differ exactly as a deviation pre-registered and signed off in docs/gaps.md
//	FAIL     they differ otherwise, or the KubeMQ leg could not complete
//	VOID     the oracle leg could not complete
//	NOT-RUN  the row names a client or scenario this suite does not implement
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/transactions/verify"
)

type matrixRow struct {
	ID         string `yaml:"id"`
	Title      string `yaml:"title"`
	Group      string `yaml:"group"`
	Client     string `yaml:"client"`
	Scenario   string `yaml:"scenario"`
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
	Client      string            `json:"client,omitempty"`
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
	Deviation  string `json:"deviation,omitempty"` // for a GAP: the observed deviation, exactly as signed off
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

// driver builds the client-container command line (after runner/client-run.sh) for one leg.
type driver func(bootstrap, scenario, target string, deadline time.Duration) []string

// config is what the drivers need from the runner.
type config struct {
	clientRun string // runner/client-run.sh
	image     string // Kafka image by digest: runtime of the franz-go driver and Kafka's own tools
	jdkImage  string // JDK image by digest: runs the Java driver with the source launcher
	pyImage   string // Python image by digest: runs the librdkafka driver (confluent-kafka wheels in wheels/)
	binDir    string // host directory mounted at /kcert: txn-driver, java/, lib/ (kafka-clients), upstream/
}

func (c config) drivers() map[string]driver {
	return map[string]driver{
		"franz-go": func(bootstrap, scenario, target string, deadline time.Duration) []string {
			return []string{"--entrypoint", "/kcert/txn-driver", "-v", c.binDir + ":/kcert:ro", "--", c.image,
				"-bootstrap", bootstrap, "-scenario", scenario, "-target", target, "-deadline", deadline.String()}
		},
		// kafka-clients and its dependencies as shipped in the Kafka image (lib/), compiled in
		// memory by the JDK's source launcher on every leg.
		"java": func(bootstrap, scenario, target string, _ time.Duration) []string {
			return []string{"--entrypoint", "java", "-v", c.binDir + ":/kcert:ro", "--", c.jdkImage,
				"-cp", "/kcert/lib/*", "/kcert/java/TxnDriver.java",
				"-bootstrap", bootstrap, "-scenario", scenario, "-target", target}
		},
		// confluent-kafka-python and the librdkafka it bundles, installed from the hash-pinned
		// wheels in wheels/ (no network) on every leg.
		"librdkafka": func(bootstrap, scenario, target string, _ time.Duration) []string {
			return []string{"--entrypoint", "sh", "-v", c.binDir + ":/kcert:ro", "--", c.pyImage, "-c",
				`pip install -q --no-index --no-deps --disable-pip-version-check /kcert/wheels/*.whl >&2 && exec python /kcert/python/txn_driver.py "$@"`,
				"txn_driver", "-bootstrap", bootstrap, "-scenario", scenario, "-target", target}
		},
		// Kafka's own command-line tools in the Kafka image (upstream system-test path).
		"kafka-tools": func(bootstrap, scenario, target string, _ time.Duration) []string {
			return []string{"--entrypoint", "sh", "-v", c.binDir + ":/kcert:ro", "--", c.image,
				"/kcert/upstream/" + strings.TrimPrefix(scenario, "upstream-") + ".sh", bootstrap, target}
		},
	}
}

func main() {
	matrixPath := flag.String("matrix", "", "matrix/transactions.yaml")
	oracleAddr := flag.String("oracle", os.Getenv("ORACLE_BOOTSTRAP"), "Kafka oracle bootstrap address, as client containers dial it")
	kubemqAddr := flag.String("kubemq", os.Getenv("KUBEMQ_BOOTSTRAP"), "KubeMQ bootstrap address, as client containers dial it")
	out := flag.String("out", os.Getenv("TRACK_RESULTS"), "results JSON to write")
	gapsPath := flag.String("gaps", "", "docs/gaps.md")
	only := flag.String("only", "", "comma-separated row ids to run; the rest are NOT-RUN")
	rowTimeout := flag.Duration("row-timeout", 4*time.Minute, "bound on one leg of one row")
	var cfg config
	flag.StringVar(&cfg.clientRun, "client-run", "", "runner/client-run.sh")
	flag.StringVar(&cfg.image, "image", "", "client container image (the Kafka image by digest)")
	flag.StringVar(&cfg.jdkImage, "jdk-image", "", "JDK image by digest for the Java driver")
	flag.StringVar(&cfg.pyImage, "python-image", "", "Python image by digest for the librdkafka driver")
	flag.StringVar(&cfg.binDir, "bin", "", "directory holding the linux txn-driver binary")
	flag.Parse()
	if *matrixPath == "" || *out == "" || cfg.clientRun == "" || cfg.image == "" || cfg.jdkImage == "" || cfg.pyImage == "" || cfg.binDir == "" {
		fatal("need -matrix, -out, -client-run, -image, -jdk-image, -python-image and -bin")
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
	drivers := cfg.drivers()

	res := Results{Schema: 1, Track: m.Track, Clients: map[string]string{}}
	for _, r := range m.Rows {
		row := Row{ID: r.ID, Kind: "behaviour", Title: r.Title, Provenance: r.Provenance, Source: r.Source}
		start := time.Now()
		d := drivers[r.Client]
		switch {
		case len(onlySet) > 0 && !onlySet[r.ID]:
			row.Verdict, row.Detail = "NOT-RUN", "excluded by -only"
		case d == nil:
			row.Verdict, row.Detail = "NOT-RUN", fmt.Sprintf("client %q has no driver in this suite", r.Client)
		case verify.Analyzers[r.Scenario] == nil:
			row.Verdict, row.Detail = "NOT-RUN", fmt.Sprintf("scenario %q is not implemented by this suite", r.Scenario)
		default:
			runRow(&row, r, d, cfg.clientRun, *oracleAddr, *kubemqAddr, *rowTimeout, reg, runDate)
			for _, l := range []*Leg{row.Oracle, row.KubeMQ} {
				if l != nil && l.Client != "" {
					res.Clients[r.Client] = l.Client
				}
			}
		}
		row.DurationMS = time.Since(start).Milliseconds()
		fmt.Fprintf(os.Stderr, "[transactions] %-46s %-7s %6dms %s\n", row.ID, row.Verdict, row.DurationMS, row.Detail)
		res.Rows = append(res.Rows, row)
	}
	if err := writeJSON(*out, res); err != nil {
		fatal(err.Error())
	}
}

// reachable reports whether a broker accepts TCP connections from this host. Client containers
// dial the host-gateway alias, which the host itself may not resolve, so the check dials the
// same port on localhost.
func reachable(addr string) bool {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	c, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", port), 3*time.Second)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

var lastRow string

func runRow(row *Row, r matrixRow, d driver, clientRun, oracleAddr, kubemqAddr string, timeout time.Duration, reg map[string]gaps.Entry, runDate time.Time) {
	defer func() { lastRow = row.ID }()
	if !reachable(oracleAddr) {
		row.Verdict, row.Detail = "VOID", "the Kafka oracle does not accept connections (it stopped during or after row "+lastRow+")"
		return
	}
	if !reachable(kubemqAddr) {
		row.Verdict, row.Detail = "FAIL", "KubeMQ does not accept connections: it stopped during or after row "+lastRow+"; this row could not run"
		return
	}
	row.Oracle = runLeg(clientRun, d(oracleAddr, r.Scenario, "real-kafka", timeout-30*time.Second), r.Scenario, timeout)
	if !row.Oracle.OK {
		row.Verdict, row.Detail = "VOID", "oracle leg failed: "+row.Oracle.Error
		return
	}
	row.KubeMQ = runLeg(clientRun, d(kubemqAddr, r.Scenario, "kubemq", timeout-30*time.Second), r.Scenario, timeout)
	if !row.KubeMQ.OK {
		row.Verdict, row.Detail = "FAIL", "KubeMQ leg failed: "+row.KubeMQ.Error
		return
	}
	row.Verdict, row.Detail = Verdict(row.ID, row.Oracle.Observation, row.KubeMQ.Observation, reg, runDate)
	if row.Verdict == "GAP" {
		row.Deviation, _ = probe.Diff(row.Oracle.Observation, row.KubeMQ.Observation)
	}
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

func runLeg(clientRun string, args []string, scenario string, timeout time.Duration) *Leg {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, clientRun, args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	leg := &Leg{MS: time.Since(start).Milliseconds()}
	if stderr.Len() > 0 {
		fmt.Fprint(os.Stderr, stderr.String())
	}
	if err != nil {
		leg.Error = fmt.Sprintf("driver %v: %s", err, lastLine(stderr.String()))
		return leg
	}
	rep, perr := ParseReport(stdout.String())
	if perr != nil {
		leg.Error = perr.Error()
		return leg
	}
	obs, aerr := verify.Analyze(scenario, rep)
	if aerr != nil {
		leg.Error = aerr.Error()
		return leg
	}
	leg.OK, leg.Client, leg.Observation = true, rep.Client, obs
	return leg
}

// ParseReport reads the driver's report: the last non-empty line of its standard output.
func ParseReport(stdout string) (verify.Report, error) {
	var rep verify.Report
	line := lastLine(stdout)
	if line == "" {
		return rep, fmt.Errorf("driver printed no report")
	}
	if err := json.Unmarshal([]byte(line), &rep); err != nil {
		return rep, fmt.Errorf("driver report is not JSON: %v", err)
	}
	if rep.Client == "" {
		return rep, fmt.Errorf("driver report does not name the client it loaded")
	}
	return rep, nil
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
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
		if r.Client == "" || r.Scenario == "" {
			return m, fmt.Errorf("%s: row %s: client and scenario are mandatory", path, r.ID)
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
	fmt.Fprintln(os.Stderr, "[transactions] ERROR:", msg)
	os.Exit(2)
}
