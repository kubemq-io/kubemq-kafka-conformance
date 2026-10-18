// Command clients-track runs the client rows of matrix/clients.yaml (Track C). Each row runs one
// client driver (SCENARIO.md) in a container, first against the Kafka oracle, then against KubeMQ
// with only the bootstrap address changed, and judges each leg from the driver's ledger plus a
// fresh reader of the log.
//
//	PASS     both legs clean and their observations identical
//	GAP      both legs clean, observations differ exactly as registered and signed in docs/gaps.md
//	FAIL     the KubeMQ leg has a problem, or the observations differ otherwise
//	VOID     the oracle leg has a problem (the row proves nothing)
//	NOT-RUN  the row names a driver this suite does not have, or was excluded by -only
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
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/clients/verify"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
)

// Scenario sizes (SCENARIO.md).
const (
	scenarioPartitions = 3
	scenarioRecords    = 300
	scenarioRecords2   = 100
	scenarioKeys       = 20
	stockRecords       = 10
	driverWaitSec      = 60
)

type matrixRow struct {
	ID         string   `yaml:"id"`
	Title      string   `yaml:"title"`
	Client     string   `yaml:"client"`
	Version    string   `yaml:"version"`
	Driver     string   `yaml:"driver"`
	Mode       string   `yaml:"mode"`
	Admin      *bool    `yaml:"admin"`
	Limits     []string `yaml:"limits"`
	Provenance string   `yaml:"provenance"`
	Source     string   `yaml:"source"`
}

type matrix struct {
	Track string      `yaml:"track"`
	Rows  []matrixRow `yaml:"rows"`
}

// Leg is one broker's side of a row (the report reads ok, events, error, problems,
// client_version and duration_ms; the rest is evidence).
type Leg struct {
	OK          bool               `json:"ok"`
	Events      int                `json:"events"`
	Error       string             `json:"error,omitempty"`
	Problems    []string           `json:"problems,omitempty"`
	Client      string             `json:"client_version,omitempty"`
	Observation verify.Observation `json:"observation,omitempty"`
	Topic       string             `json:"topic"`
	ExitCode    int                `json:"driver_exit_code"`
	MS          int64              `json:"duration_ms"`
}

type Row struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	Title      string `json:"title"`
	Verdict    string `json:"verdict"`
	Provenance string `json:"provenance"`
	Source     string `json:"source"`
	Detail     string `json:"detail,omitempty"`
	// Deviation is the observed difference between the two clean legs (GAP or FAIL), exactly the
	// text a gap registration must carry.
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

type config struct {
	oracle, kubemq string
	drivers        *drivers
	transcripts    string
	timeout        time.Duration
}

func main() {
	matrixPath := flag.String("matrix", "", "matrix/clients.yaml")
	oracleAddr := flag.String("oracle", os.Getenv("ORACLE_BOOTSTRAP"), "Kafka oracle bootstrap address as containers see it")
	kubemqAddr := flag.String("kubemq", os.Getenv("KUBEMQ_BOOTSTRAP"), "KubeMQ bootstrap address as containers see it")
	out := flag.String("out", os.Getenv("TRACK_RESULTS"), "results JSON to write")
	gapsPath := flag.String("gaps", "", "docs/gaps.md")
	suite := flag.String("suite", "", "suites/clients directory")
	runDir := flag.String("run-dir", os.Getenv("CERT_RUN_DIR"), "run directory (binaries, transcripts)")
	only := flag.String("only", "", "comma-separated row ids to run; the rest are NOT-RUN")
	rowTimeout := flag.Duration("row-timeout", 5*time.Minute, "bound on one leg of one row")
	flag.Parse()
	if *matrixPath == "" || *out == "" || *suite == "" || *runDir == "" {
		fatal("need -matrix, -out, -suite and -run-dir")
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
	cfg := config{oracle: *oracleAddr, kubemq: *kubemqAddr, timeout: *rowTimeout,
		drivers:     newDrivers(*suite, filepath.Join(*runDir, "bin", "clients")),
		transcripts: filepath.Join(*runDir, "transcripts", "clients")}

	res := Results{Schema: 1, Track: m.Track, Clients: map[string]string{}}
	for _, r := range m.Rows {
		row := Row{ID: r.ID, Kind: "behaviour", Title: r.Title, Provenance: r.Provenance, Source: r.Source}
		start := time.Now()
		switch {
		case len(onlySet) > 0 && !onlySet[r.ID]:
			row.Verdict, row.Detail = "NOT-RUN", "excluded by -only"
		case !cfg.drivers.known(r.Driver):
			row.Verdict, row.Detail = "NOT-RUN", fmt.Sprintf("driver %q is not implemented by this suite", r.Driver)
		default:
			runRow(&row, r, cfg, reg, runDate)
			if row.Oracle != nil && row.Oracle.Client != "" {
				res.Clients[r.Client] = row.Oracle.Client
			}
		}
		row.DurationMS = time.Since(start).Milliseconds()
		fmt.Fprintf(os.Stderr, "[clients] %-44s %-7s %6dms %.300s\n", row.ID, row.Verdict, row.DurationMS, row.Detail)
		res.Rows = append(res.Rows, row)
	}
	if err := writeJSON(*out, res); err != nil {
		fatal(err.Error())
	}
}

func specOf(r matrixRow) verify.Spec {
	s := verify.Spec{Mode: r.Mode, Client: r.Client, Version: r.Version, Admin: r.Admin == nil || *r.Admin, Limits: r.Limits}
	switch r.Mode {
	case "scenario":
		s.Partitions, s.Records, s.Records2, s.Keys = scenarioPartitions, scenarioRecords, scenarioRecords2, scenarioKeys
	case "verifiable":
		s.Partitions, s.Records, s.Keys = scenarioPartitions, scenarioRecords, scenarioKeys
	default:
		s.Records = stockRecords
	}
	return s
}

func runRow(row *Row, r matrixRow, cfg config, reg map[string]gaps.Entry, runDate time.Time) {
	if err := cfg.drivers.build(r.Driver); err != nil {
		// The rig could not build the client: nothing ran on either broker.
		row.Verdict, row.Detail = "NOT-RUN", "driver could not be built: "+err.Error()
		return
	}
	if !reachable(cfg.oracle) {
		row.Verdict, row.Detail = "VOID", "the Kafka oracle does not accept connections"
		return
	}
	if !reachable(cfg.kubemq) {
		row.Verdict, row.Detail = "FAIL", "KubeMQ does not accept connections; this row could not run"
		return
	}
	spec := specOf(r)
	row.Oracle = runLeg(r, spec, cfg, cfg.oracle, "real-kafka")
	if v, d, done := legVerdict(row.Oracle, nil); done {
		row.Verdict, row.Detail = v, d
		return
	}
	row.KubeMQ = runLeg(r, spec, cfg, cfg.kubemq, "kubemq")
	if v, d, done := legVerdict(row.Oracle, row.KubeMQ); done {
		row.Verdict, row.Detail = v, d
		return
	}
	row.Verdict, row.Detail = Verdict(row.ID, row.Oracle.Observation, row.KubeMQ.Observation, reg, runDate)
	if row.Verdict != "PASS" {
		row.Deviation, _ = verify.Diff(row.Oracle.Observation, row.KubeMQ.Observation)
	}
}

// legVerdict decides a row from its legs' own problems, before any comparison: a failed oracle
// leg makes the row VOID (it proves nothing), a failed KubeMQ leg makes it FAIL. done is false
// when the legs so far are clean (or KubeMQ has not run yet). Pure, for the tests.
func legVerdict(oracle, kubemq *Leg) (string, string, bool) {
	if oracle != nil && !oracle.OK {
		return "VOID", "oracle leg: " + legSummary(oracle), true
	}
	if kubemq != nil && !kubemq.OK {
		return "FAIL", "KubeMQ leg: " + legSummary(kubemq), true
	}
	return "", "", false
}

// Verdict compares two clean legs' observations under the gap register. Pure, for the tests.
func Verdict(id string, oracle, kubemq verify.Observation, reg map[string]gaps.Entry, runDate time.Time) (string, string) {
	diff, same := verify.Diff(oracle, kubemq)
	if same {
		b, _ := json.Marshal(oracle)
		return "PASS", "observations identical: " + string(b)
	}
	if ok, why := gaps.Registered(reg, id, diff, runDate); ok {
		return "GAP", diff + ". " + why + "."
	} else {
		return "FAIL", diff + ". " + why + "."
	}
}

func legSummary(l *Leg) string {
	if l.Error != "" {
		return l.Error
	}
	return strings.Join(l.Problems, "; ")
}

// hostSide is addr with its host replaced by the loopback address, for the host-side clients.
func hostSide(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return net.JoinHostPort("127.0.0.1", port)
}

// reachable reports whether a broker accepts TCP connections, trying for about 15 seconds so a
// momentary local connection failure is not taken for a stopped broker.
func reachable(addr string) bool {
	for i := 0; i < 5; i++ {
		if c, err := net.DialTimeout("tcp", hostSide(addr), 3*time.Second); err == nil {
			_ = c.Close()
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

func runLeg(r matrixRow, spec verify.Spec, cfg config, bootstrap, target string) *Leg {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	name := fmt.Sprintf("kcert-cl-%s-%s-%s-%d", r.Driver, r.Mode, target, time.Now().UnixNano())
	leg := &Leg{Topic: name}
	if !spec.Admin {
		if err := precreate(ctx, bootstrap, name, spec.Partitions); err != nil {
			leg.Error = "track could not create the topic: " + err.Error()
			leg.MS = time.Since(start).Milliseconds()
			return leg
		}
	}
	env := map[string]string{
		"KCERT_BOOTSTRAP": bootstrap, "KCERT_MODE": spec.Mode, "KCERT_TOPIC": name, "KCERT_GROUP": name + "-group",
		"KCERT_PARTITIONS": fmt.Sprint(spec.Partitions), "KCERT_RECORDS": fmt.Sprint(spec.Records),
		"KCERT_RECORDS2": fmt.Sprint(spec.Records2), "KCERT_KEYS": fmt.Sprint(spec.Keys),
		"KCERT_TIMEOUT_SEC": fmt.Sprint(driverWaitSec), "KCERT_ADMIN": "1",
	}
	if !spec.Admin {
		env["KCERT_ADMIN"] = "0"
	}
	dir := filepath.Join(cfg.transcripts, r.ID)
	_ = os.MkdirAll(dir, 0o755)
	var ledger verify.Ledger
	var stderr []byte
	var exit int
	if r.Driver == "verifiable" {
		prod, perr, pexit := cfg.drivers.runVerifiable(ctx, "producer", env)
		cons, cerr, cexit := []byte(nil), []byte(nil), 0
		if pexit == 0 {
			cons, cerr, cexit = cfg.drivers.runVerifiable(ctx, "consumer", env)
		}
		_ = os.WriteFile(filepath.Join(dir, target+".producer.jsonl"), prod, 0o644)
		_ = os.WriteFile(filepath.Join(dir, target+".consumer.jsonl"), cons, 0o644)
		stderr, exit = append(perr, cerr...), pexit
		if exit == 0 {
			exit = cexit
		}
		ledger = verify.FromVerifiable(prod, cons)
	} else {
		var stdout []byte
		stdout, stderr, exit = cfg.drivers.run(ctx, r.Driver, env)
		_ = os.WriteFile(filepath.Join(dir, target+".ledger.jsonl"), stdout, 0o644)
		ledger = verify.ParseLedger(bytes.NewReader(stdout))
	}
	_ = os.WriteFile(filepath.Join(dir, target+".stderr.log"), stderr, 0o644)
	leg.ExitCode = exit
	leg.Events = len(ledger.Events)
	view := freshRead(ctx, bootstrap, name, name+"-group")
	view.LegStartMS, view.LegEndMS = start.UnixMilli(), time.Now().UnixMilli()
	obs, problems := verify.Check(spec, ledger, view)
	leg.Observation, leg.Problems = obs, problems
	if v, ok := obs["client_version"].(string); ok {
		leg.Client = v
	}
	if ctx.Err() != nil {
		leg.Error = fmt.Sprintf("leg exceeded %s", cfg.timeout)
	}
	if len(problems) > 0 && exit != 0 {
		leg.Problems = append(leg.Problems, fmt.Sprintf("driver exit code %d; last output: %s", exit, tail(stderr, 400)))
	}
	leg.OK = leg.Error == "" && len(leg.Problems) == 0
	leg.MS = time.Since(start).Milliseconds()
	return leg
}

func tail(b []byte, n int) string {
	s := strings.TrimSpace(string(b))
	if len(s) > n {
		s = "..." + s[len(s)-n:]
	}
	return strings.ReplaceAll(s, "\n", " | ")
}

// drivers builds and runs client drivers. Go drivers are cross-compiled for the engine's
// architecture and run in the pinned static runtime image; every other driver is an image built
// from drivers/<name>/Containerfile by build-driver.sh. Nothing is installed on the host.
type drivers struct {
	suite, bin string
	built      map[string]error
	images     map[string]string
}

// goDrivers are built from ./drivers/<name> in this module; imageDrivers from a Containerfile.
var (
	goDrivers    = map[string]bool{"franzgo": true, "sarama": true, "segmentio": true}
	imageDrivers = map[string]bool{"java": true, "python": true, "kcat": true, "confluentgo": true, "dotnet": true, "kafkajs": true, "spring": true,
		"java-legacy": true, "python-legacy": true}
)

func newDrivers(suite, bin string) *drivers {
	return &drivers{suite: suite, bin: bin, built: map[string]error{}, images: map[string]string{}}
}

func (d *drivers) known(name string) bool {
	return goDrivers[name] || imageDrivers[name] || name == "verifiable"
}

func (d *drivers) binary(name string) string {
	return filepath.Join(d.bin, name+"-linux-"+os.Getenv("CERT_ENGINE_ARCH"))
}

func (d *drivers) build(name string) error {
	if err, ok := d.built[name]; ok {
		return err
	}
	arch := os.Getenv("CERT_ENGINE_ARCH")
	var cmd *exec.Cmd
	switch {
	case arch == "":
		d.built[name] = fmt.Errorf("CERT_ENGINE_ARCH is not set (run from runner/certify.sh)")
		return d.built[name]
	case name == "verifiable":
		// Kafka's own tools, from the pinned oracle image (compose/versions.env): nothing to build.
		if kafkaImage() == "" {
			d.built[name] = fmt.Errorf("KAFKA_IMAGE_REPO and KAFKA_IMAGE_DIGEST are not set (run from runner/certify.sh)")
		} else {
			d.built[name] = nil
		}
		return d.built[name]
	case goDrivers[name]:
		// A valid prebuilt image (build-driver.sh) runs as is; otherwise cross-compile the binary.
		pre := exec.Command(filepath.Join(d.suite, "build-driver.sh"), name)
		pre.Dir, pre.Stderr = d.suite, os.Stderr
		if out, err := pre.Output(); err == nil && strings.TrimSpace(string(out)) != "" {
			d.images[name] = strings.TrimSpace(string(out))
			fmt.Fprintf(os.Stderr, "[clients] driver %s: prebuilt image %s\n", name, d.images[name])
			d.built[name] = nil
			return nil
		}
		cmd = exec.Command("go", "build", "-trimpath", "-o", d.binary(name), "./drivers/"+name)
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+arch)
	default:
		cmd = exec.Command(filepath.Join(d.suite, "build-driver.sh"), name)
	}
	cmd.Dir = d.suite
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	start := time.Now()
	err := cmd.Run()
	if err != nil {
		err = fmt.Errorf("%v: %s", err, tail(stderr.Bytes(), 1500))
	} else if imageDrivers[name] {
		d.images[name] = strings.TrimSpace(stdout.String())
		fmt.Fprintf(os.Stderr, "[clients] driver %s: image %s (%s)\n", name, d.images[name], time.Since(start).Round(time.Second))
	}
	d.built[name] = err
	return err
}

// run starts the driver container through runner/client-run.sh and returns its standard output
// (the ledger), standard error and exit code. On timeout the container gets SIGTERM.
func (d *drivers) run(ctx context.Context, name string, env map[string]string) ([]byte, []byte, int) {
	runner := filepath.Join(d.suite, "..", "..", "runner", "client-run.sh")
	var args []string
	mounted := goDrivers[name] && d.images[name] == "" // a locally compiled Go binary
	if mounted {
		args = append(args, "-v", d.binary(name)+":/kcert/driver:ro")
	}
	for k, v := range env {
		args = append(args, "-e", k+"="+v)
	}
	if mounted {
		args = append(args, "--", os.Getenv("GO_DRIVER_IMAGE"), "/kcert/driver")
	} else {
		args = append(args, "--", d.images[name])
	}
	return containerRun(ctx, runner, args)
}

func kafkaImage() string {
	if os.Getenv("KAFKA_IMAGE_REPO") == "" || os.Getenv("KAFKA_IMAGE_DIGEST") == "" {
		return ""
	}
	return os.Getenv("KAFKA_IMAGE_REPO") + "@" + os.Getenv("KAFKA_IMAGE_DIGEST")
}

// verifiableScripts run Kafka's verifiable tools as its ducktape services do. The producer first
// prints the tools' own version, so the row checks which kafka-clients actually ran. The consumer
// is bounded by the scenario wait: it exits by itself after --max-messages.
var verifiableScripts = map[string]string{
	"producer": `printf '{"name":"kcert_version","version":"%s"}\n' "$(/opt/kafka/bin/kafka-topics.sh --version 2>/dev/null | cut -d' ' -f1)"
exec /opt/kafka/bin/kafka-verifiable-producer.sh --bootstrap-server "$KCERT_BOOTSTRAP" --topic "$KCERT_TOPIC" \
  --max-messages "$KCERT_RECORDS" --acks -1 --repeating-keys "$KCERT_KEYS"`,
	"consumer": `exec timeout "$KCERT_TIMEOUT_SEC" /opt/kafka/bin/kafka-verifiable-consumer.sh --bootstrap-server "$KCERT_BOOTSTRAP" \
  --topic "$KCERT_TOPIC" --group-id "$KCERT_GROUP" --max-messages "$KCERT_RECORDS" --verbose \
  --reset-policy earliest --group-protocol classic`,
}

// runVerifiable runs one of Kafka's verifiable tools in the pinned Kafka image.
func (d *drivers) runVerifiable(ctx context.Context, tool string, env map[string]string) ([]byte, []byte, int) {
	runner := filepath.Join(d.suite, "..", "..", "runner", "client-run.sh")
	args := []string{"--entrypoint", "/bin/bash"}
	for k, v := range env {
		args = append(args, "-e", k+"="+v)
	}
	args = append(args, "--", kafkaImage(), "-c", verifiableScripts[tool])
	return containerRun(ctx, runner, args)
}

func containerRun(ctx context.Context, runner string, args []string) ([]byte, []byte, int) {
	cmd := exec.CommandContext(ctx, runner, args...)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 20 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		exit = -1
		if ee, ok := err.(*exec.ExitError); ok {
			exit = ee.ExitCode()
		}
		fmt.Fprintf(&stderr, "\n[clients] driver run: %v\n", err)
	}
	return stdout.Bytes(), stderr.Bytes(), exit
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
		switch {
		case r.ID == "" || seen[r.ID]:
			return m, fmt.Errorf("%s: empty or duplicate row id %q", path, r.ID)
		case r.Provenance != "upstream" && r.Provenance != "kubemq-authored":
			return m, fmt.Errorf("%s: row %s: provenance must be upstream or kubemq-authored", path, r.ID)
		case r.Source == "" || r.Client == "" || r.Version == "" || r.Driver == "":
			return m, fmt.Errorf("%s: row %s: source, client, version and driver are mandatory", path, r.ID)
		case r.Mode != "scenario" && r.Mode != "stock-defaults" && r.Mode != "verifiable":
			return m, fmt.Errorf("%s: row %s: mode must be scenario, stock-defaults or verifiable", path, r.ID)
		case (r.Mode == "verifiable") != (r.Driver == "verifiable"):
			return m, fmt.Errorf("%s: row %s: mode verifiable goes with driver verifiable", path, r.ID)
		case !knownLimits(r.Limits):
			return m, fmt.Errorf("%s: row %s: limits may only be %s", path, r.ID, strings.Join(verify.Limits, ", "))
		case r.Driver == "verifiable" && (r.Admin == nil || *r.Admin):
			return m, fmt.Errorf("%s: row %s: the verifiable tools have no admin step; set admin: false", path, r.ID)
		}
		seen[r.ID] = true
	}
	return m, nil
}

func knownLimits(ls []string) bool {
	for _, l := range ls {
		ok := false
		for _, k := range verify.Limits {
			ok = ok || l == k
		}
		if !ok {
			return false
		}
	}
	return true
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
	fmt.Fprintln(os.Stderr, "[clients] ERROR:", msg)
	os.Exit(2)
}
