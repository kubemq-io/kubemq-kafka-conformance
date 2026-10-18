// Command security-track runs the rows of matrix/security.yaml (Track E). The runner reboots both
// brokers once per posture (runner/certify.sh, TRACK_POSTURES), so the command has three steps:
//
//	prepare -posture P   certificates (once per run) and both brokers' boot config for P
//	run -posture P       every row of posture P: probe on the oracle, then on KubeMQ
//	finish               one results JSON for the whole track, in matrix order
//
// Verdicts, per row:
//
//	PASS     both observations are identical
//	GAP      they differ exactly as a deviation pre-registered and signed off in docs/gaps.md
//	FAIL     they differ otherwise, the KubeMQ probe could not complete, or KubeMQ did not boot
//	         with the posture's documented configuration
//	VOID     the oracle probe could not complete, or the oracle did not boot
//	NOT-RUN  the row's probe is not implemented, or its posture produced nothing
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	fprobe "github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/security/oidc"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/security/posture"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/security/probe"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/security/tlsca"
)

type matrixRow struct {
	ID         string `yaml:"id"`
	Title      string `yaml:"title"`
	Group      string `yaml:"group"`
	Kind       string `yaml:"kind"` // "behaviour" (default) or "advertisement" (no oracle leg run)
	Posture    string `yaml:"posture"`
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
	// Deviation is the observed field-by-field difference (GAP and FAIL rows), which the
	// renderer matches against the signed register entry.
	Deviation  string `json:"deviation,omitempty"`
	Oracle     *Leg   `json:"oracle,omitempty"`
	KubeMQ     *Leg   `json:"kubemq,omitempty"`
	DurationMS int64  `json:"duration_ms"`
}

// Results is the track's results JSON.
type Results struct {
	Schema  int               `json:"schema"`
	Track   string            `json:"track"`
	Rows    []Row             `json:"rows"`
	Clients map[string]string `json:"clients"`
}

func main() {
	if len(os.Args) < 2 {
		fatal("usage: security-track prepare|run|finish [flags]")
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	matrixPath := fs.String("matrix", "", "matrix/security.yaml")
	gapsPath := fs.String("gaps", "", "docs/gaps.md")
	post := fs.String("posture", os.Getenv("POSTURE"), "posture name")
	dir := fs.String("dir", os.Getenv("POSTURE_DIR"), "this posture's directory")
	hosts := fs.String("hosts", "localhost,127.0.0.1,::1", "comma-separated SANs of the broker certificate")
	out := fs.String("out", os.Getenv("TRACK_RESULTS"), "results JSON (finish)")
	rowTimeout := fs.Duration("row-timeout", 3*time.Minute, "bound on one leg of one row")
	_ = fs.Parse(os.Args[2:])
	if *matrixPath == "" {
		fatal("need -matrix")
	}
	m, err := loadMatrix(*matrixPath)
	if err != nil {
		fatal(err.Error())
	}
	switch os.Args[1] {
	case "prepare":
		if *dir == "" || *post == "" {
			fatal("prepare needs -posture and -dir")
		}
		err = prepare(*dir, *post, strings.Split(*hosts, ","))
	case "run":
		if *dir == "" || *post == "" {
			fatal("run needs -posture and -dir")
		}
		err = run(m, *dir, *post, *gapsPath, *rowTimeout)
	case "finish":
		if *out == "" || *dir == "" {
			fatal("finish needs -out and -dir (the postures root)")
		}
		err = writeJSON(*out, finish(m, *dir))
	default:
		fatal("unknown step " + os.Args[1])
	}
	if err != nil {
		fatal(err.Error())
	}
}

// certDir is where the run's certificates live: next to the posture directories.
func certDir(postureDir string) string { return filepath.Join(filepath.Dir(postureDir), "certs") }

func prepare(dir, name string, hosts []string) error {
	cd := certDir(dir)
	if _, err := os.Stat(filepath.Join(cd, "ca.crt")); errors.Is(err, os.ErrNotExist) {
		if err := tlsca.Generate(cd, hosts); err != nil {
			return fmt.Errorf("certificates: %w", err)
		}
		if err := oidc.GenerateKeys(cd); err != nil {
			return fmt.Errorf("token signing keys: %w", err)
		}
	}
	p, err := portsFromEnv()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return posture.Write(dir, name, p, cd)
}

func portsFromEnv() (posture.Ports, error) {
	var p posture.Ports
	for name, dst := range map[string]*int{
		"POSTURE_ORACLE_PORT": &p.OraclePlain, "POSTURE_ORACLE_TLS_PORT": &p.OracleTLS,
		"POSTURE_ORACLE_ADMIN_PORT": &p.OracleAdmin, "POSTURE_KUBEMQ_PORT": &p.KubeMQPlain,
		"POSTURE_KUBEMQ_TLS_PORT": &p.KubeMQTLS, "POSTURE_KUBEMQ_API_PORT": &p.KubeMQAPI,
	} {
		v, err := strconv.Atoi(os.Getenv(name))
		if err != nil || v <= 0 {
			return p, fmt.Errorf("%s is not set to a port (the runner sets it)", name)
		}
		*dst = v
	}
	return p, nil
}

func run(m matrix, dir, name, gapsPath string, timeout time.Duration) error {
	reg, err := gaps.Load(gapsPath)
	if err != nil {
		return fmt.Errorf("gap register: %w", err)
	}
	p, err := portsFromEnv()
	if err != nil {
		return err
	}
	certs, err := tlsca.Load(certDir(dir))
	if err != nil {
		return err
	}
	if spec := posture.Specs[name]; spec.Authorize || spec.QuotaBytesPerSec > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		err := posture.ApplyKafkaACLs(ctx, hostPort(p.OracleAdmin), spec)
		if err == nil {
			err = posture.ApplyKafkaQuotas(ctx, hostPort(p.OracleAdmin), spec)
		}
		cancel()
		if err != nil {
			// Without its ACLs the oracle cannot answer this posture's rows.
			return writeJSON(filepath.Join(dir, "results.json"), voidRows(m, name, "oracle setup failed: "+err.Error()))
		}
	}
	// KubeMQ gets the posture's DENY rules through CreateAcls (its boot policy has only the
	// allows). A broker that refuses them is still measured: the rows then show what it does
	// without them, and each row's detail carries the refusal.
	kubemqSetup := ""
	if spec := posture.Specs[name]; spec.Authorize {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		if err := posture.ApplyKubeMQACLs(ctx, hostPort(p.KubeMQPlain), spec); err != nil {
			kubemqSetup = "KubeMQ setup: " + err.Error()
			fmt.Fprintf(os.Stderr, "[security] %-8s %s\n", name, kubemqSetup)
		}
		cancel()
	}
	oracle := probe.Env{Target: "real-kafka", Plain: hostPort(p.OraclePlain), TLS: hostPort(p.OracleTLS), Certs: certs,
		Container: os.Getenv("POSTURE_ORACLE_CONTAINER"), PlainInContainer: hostPort(p.OraclePlain), PostureDir: dir}
	kubemq := probe.Env{Target: "kubemq", Plain: hostPort(p.KubeMQPlain), TLS: hostPort(p.KubeMQTLS), Certs: certs,
		Container: os.Getenv("POSTURE_KUBEMQ_CONTAINER"), PlainInContainer: hostPort(p.KubeMQPlain), PostureDir: dir}
	runDate := time.Now().UTC()
	if v := os.Getenv("CERT_RUN_STARTED"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			runDate = t
		}
	}
	var rows []Row
	for _, r := range m.Rows {
		if r.Posture != name {
			continue
		}
		row := newRow(r)
		start := time.Now()
		if p := probe.Registry[r.Probe]; p == nil {
			row.Verdict, row.Detail = "NOT-RUN", fmt.Sprintf("probe %q is not implemented by this suite", r.Probe)
		} else {
			runRow(&row, p, oracle, kubemq, timeout, reg, runDate)
		}
		if kubemqSetup != "" {
			row.Detail += " (" + kubemqSetup + ")"
		}
		row.DurationMS = time.Since(start).Milliseconds()
		fmt.Fprintf(os.Stderr, "[security] %-8s %-46s %-7s %6dms %s\n", name, row.ID, row.Verdict, row.DurationMS, row.Detail)
		rows = append(rows, row)
	}
	return writeJSON(filepath.Join(dir, "results.json"), rows)
}

func voidRows(m matrix, name, why string) []Row {
	var rows []Row
	for _, r := range m.Rows {
		if r.Posture == name {
			row := newRow(r)
			row.Verdict, row.Detail = "VOID", why
			rows = append(rows, row)
		}
	}
	return rows
}

// Brokers are reached at 127.0.0.1: ports are published on IPv4 loopback only.
func hostPort(port int) string { return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) }

func newRow(r matrixRow) Row {
	kind := r.Kind
	if kind == "" {
		kind = "behaviour"
	}
	return Row{ID: r.ID, Kind: kind, Title: r.Title, Provenance: r.Provenance, Source: r.Source}
}

func runRow(row *Row, p probe.Probe, oracle, kubemq probe.Env, timeout time.Duration, reg map[string]gaps.Entry, runDate time.Time) {
	if !reachable(anyEndpoint(oracle)) {
		row.Verdict, row.Detail = "VOID", "the Kafka oracle does not accept connections"
		return
	}
	if !reachable(anyEndpoint(kubemq)) {
		row.Verdict, row.Detail = "FAIL", "KubeMQ does not accept connections; this row could not run"
		return
	}
	row.Oracle = runLeg(p, oracle, timeout)
	if !row.Oracle.OK {
		row.Verdict, row.Detail = "VOID", "oracle leg failed: "+row.Oracle.Error
		return
	}
	row.KubeMQ = runLeg(p, kubemq, timeout)
	if !row.KubeMQ.OK {
		row.Verdict, row.Detail = "FAIL", "KubeMQ leg failed: "+row.KubeMQ.Error
		return
	}
	row.Verdict, row.Detail = Verdict(row.ID, row.Oracle.Observation, row.KubeMQ.Observation, reg, runDate)
	if diff, same := fprobe.Diff(row.Oracle.Observation, row.KubeMQ.Observation); !same {
		row.Deviation = diff
	}
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

func reachable(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func runLeg(p probe.Probe, env probe.Env, timeout time.Duration) (leg *Leg) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			leg = &Leg{Error: fmt.Sprintf("probe panicked: %v", r), MS: time.Since(start).Milliseconds()}
		}
	}()
	obs, err := p(ctx, env)
	leg = &Leg{OK: err == nil, Observation: obs, MS: time.Since(start).Milliseconds()}
	if err != nil {
		leg.Error = err.Error()
	}
	return leg
}

// finish assembles the track's results from every posture directory under root, in matrix
// order. A posture whose brokers did not boot (boot-failed, written by the runner) makes its rows
// VOID when the oracle failed and FAIL when KubeMQ failed; a posture whose prepare step failed
// (prepare-failed) or that has no results at all makes its rows NOT-RUN with the reason. Pure apart from reading root, for the mutation tests.
func finish(m matrix, root string) Results {
	res := Results{Schema: 1, Track: m.Track, Clients: clientVersions()}
	ran := map[string]Row{}
	for _, name := range postureNames(m) {
		var rows []Row
		if b, err := os.ReadFile(filepath.Join(root, name, "results.json")); err == nil && json.Unmarshal(b, &rows) == nil {
			for _, r := range rows {
				ran[r.ID] = r
			}
		}
	}
	for _, r := range m.Rows {
		if row, ok := ran[r.ID]; ok {
			res.Rows = append(res.Rows, row)
			continue
		}
		row := newRow(r)
		row.Verdict, row.Detail = postureMissing(root, r.Posture)
		res.Rows = append(res.Rows, row)
	}
	return res
}

func postureMissing(root, name string) (string, string) {
	if _, ok := posture.Specs[name]; !ok {
		return "NOT-RUN", fmt.Sprintf("posture %q is not implemented by this suite", name)
	}
	if b, err := os.ReadFile(filepath.Join(root, name, "prepare-failed")); err == nil {
		return "NOT-RUN", "the track could not prepare posture " + name + ": " + lastLine(string(b))
	}
	b, err := os.ReadFile(filepath.Join(root, name, "boot-failed"))
	if err != nil {
		return "NOT-RUN", fmt.Sprintf("posture %s produced no results", name)
	}
	why := lastLine(string(b))
	if strings.Contains(why, ": oracle ") || strings.Contains(why, "Kafka oracle") {
		return "VOID", "the oracle did not boot with posture " + name + ": " + why
	}
	return "FAIL", "KubeMQ did not boot with posture " + name + ": " + why
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

func postureNames(m matrix) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range m.Rows {
		if !seen[r.Posture] {
			seen[r.Posture] = true
			out = append(out, r.Posture)
		}
	}
	return out
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
		switch {
		case r.ID == "" || seen[r.ID]:
			return m, fmt.Errorf("%s: empty or duplicate row id %q", path, r.ID)
		case r.Provenance != "upstream" && r.Provenance != "kubemq-authored":
			return m, fmt.Errorf("%s: row %s: provenance must be upstream or kubemq-authored", path, r.ID)
		case r.Source == "":
			return m, fmt.Errorf("%s: row %s: source is mandatory", path, r.ID)
		case r.Posture == "":
			return m, fmt.Errorf("%s: row %s: posture is mandatory", path, r.ID)
		}
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
	fmt.Fprintln(os.Stderr, "[security] ERROR:", msg)
	os.Exit(2)
}

func anyEndpoint(e probe.Env) string {
	if a := probe.First(e.Plain); a != "" {
		return a
	}
	return probe.First(e.TLS)
}
