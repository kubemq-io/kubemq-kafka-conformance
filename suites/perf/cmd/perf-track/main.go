// Command perf-track runs the performance baseline of matrix/perf.yaml (Track H): Apache Kafka's
// own kafka-producer-perf-test and kafka-consumer-perf-test, from the pinned Kafka image in a
// client container, against Kafka and then KubeMQ on the same machine, both brokers' data on the
// machine's persistent disk at their default sync behaviour. While one broker is measured the
// other is paused.
//
// This is a reproducible baseline, not a benchmark claim. Rows are kind "measurement": the report
// lists them outside the PASS/FAIL counts and the VOID budget. A row's verdict asserts only that the
// measurement completed on both brokers (every record sent, no errors): PASS, or FAIL when the
// KubeMQ leg could not complete, VOID when the Kafka leg could not. The numbers are reported,
// never judged.
//
//	perf-track run     -matrix M -dir D -ctl ctl.sh -oracle A -kubemq B -oracle-container C -kubemq-container K
//	perf-track finish  -matrix M -dir D -out results.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/perf/parse"
)

type matrixRow struct {
	ID         string `yaml:"id"`
	Title      string `yaml:"title"`
	Kind       string `yaml:"kind"` // produce | consume
	Size       int    `yaml:"size"`
	Acks       string `yaml:"acks,omitempty"`
	Records    int    `yaml:"records"`
	Provenance string `yaml:"provenance"`
	Source     string `yaml:"source"`
}

// Reps per measurement; the median is reported.
const reps = 3

// Measure is one broker's measurement of one row.
type Measure struct {
	OK            bool      `json:"ok"`
	Error         string    `json:"error,omitempty"`
	RecordsPerSec float64   `json:"records_per_sec"`
	MBPerSec      float64   `json:"mb_per_sec"`
	P50Ms         float64   `json:"p50_ms,omitempty"`
	P99Ms         float64   `json:"p99_ms,omitempty"`
	Reps          []float64 `json:"reps_records_per_sec"`
	// OtherRunning is set when the other broker could not be paused during the measurement
	// (single-machine run only); the numbers are then not "one broker at a time".
	OtherRunning bool `json:"other_broker_running,omitempty"`
}

// Legs is what "run" leaves for "finish": row id to broker ("kafka", "kubemq") to measurement.
type Legs map[string]map[string]*Measure

func main() {
	if len(os.Args) < 2 {
		fatal("usage: perf-track run|finish [flags]")
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	matrixPath := fs.String("matrix", "", "matrix/perf.yaml")
	dir := fs.String("dir", "", "posture directory")
	ctl := fs.String("ctl", "", "suites/perf/ctl.sh")
	oracle := fs.String("oracle", "", "Kafka bootstrap as client containers reach it")
	kubemq := fs.String("kubemq", "", "KubeMQ bootstrap as client containers reach it")
	oc := fs.String("oracle-container", "", "Kafka container (paused while KubeMQ is measured)")
	kc := fs.String("kubemq-container", "", "KubeMQ container (paused while Kafka is measured)")
	out := fs.String("out", "", "results JSON")
	_ = fs.Parse(os.Args[2:])
	rows, err := loadMatrix(*matrixPath)
	if err != nil {
		fatal(err.Error())
	}
	legsPath := filepath.Join(*dir, "legs.json")
	switch os.Args[1] {
	case "run":
		r := &runner{ctl: *ctl, image: os.Getenv("KAFKA_IMAGE_REPO") + "@" + os.Getenv("KAFKA_IMAGE_DIGEST"), dir: *dir}
		legs := Legs{}
		for _, b := range []struct{ name, bootstrap, self, other string }{
			{"kafka", *oracle, *oc, *kc}, {"kubemq", *kubemq, *kc, *oc},
		} {
			paused := true
			if err := r.ctlRun("pause", b.other); err != nil {
				logf("could not pause %s: %v (measuring with it running; recorded on every row)", b.other, err)
				paused = false
			}
			r.broker(b.name, b.bootstrap, rows, legs)
			if !paused {
				for _, bySide := range legs {
					if m := bySide[b.name]; m != nil {
						m.OtherRunning = true
					}
				}
			}
			if err := r.ctlRun("unpause", b.other); err != nil {
				logf("could not resume %s: %v", b.other, err)
			}
		}
		if err := writeJSON(legsPath, legs); err != nil {
			fatal(err.Error())
		}
	case "finish":
		var legs Legs
		if b, err := os.ReadFile(legsPath); err == nil {
			_ = json.Unmarshal(b, &legs)
		}
		res, summary := Finish(rows, legs, filepath.Join(*dir, "prepare-failed"), filepath.Join(*dir, "boot-failed"))
		if err := os.WriteFile(filepath.Join(*dir, "summary.md"), []byte(summary), 0o644); err != nil {
			fatal(err.Error())
		}
		fmt.Fprint(os.Stderr, summary)
		if err := writeJSON(*out, res); err != nil {
			fatal(err.Error())
		}
	default:
		fatal("unknown step " + os.Args[1])
	}
}

type runner struct {
	ctl, image, dir string
}

func (r *runner) ctlRun(args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, r.ctl, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// tool runs one Kafka tool in a fresh client container and returns its combined output.
func (r *runner) tool(limit time.Duration, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	argv := []string{"client", "-e", "KAFKA_HEAP_OPTS=-Xms512m -Xmx1g"}
	if os.Getenv("CERT_ENGINE_ARCH") == "arm64" {
		argv = append(argv, "-e", "JAVA_TOOL_OPTIONS=-XX:UseSVE=0")
	}
	argv = append(append(argv, "--", r.image), args...)
	out, err := exec.CommandContext(ctx, r.ctl, argv...).CombinedOutput()
	return string(out), err
}

func (r *runner) broker(name, bootstrap string, rows []matrixRow, legs Legs) {
	set := func(id string, m *Measure) {
		if legs[id] == nil {
			legs[id] = map[string]*Measure{}
		}
		legs[id][name] = m
	}
	topics := map[int]string{}
	for _, row := range rows {
		topic, ok := topics[row.Size]
		if !ok {
			topic = fmt.Sprintf("kcert-perf-%s-%d-%d", name, row.Size, time.Now().Unix())
			topics[row.Size] = topic
			args := []string{"/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", bootstrap, "--create", "--topic", topic, "--partitions", "6"}
			out, err := r.tool(2*time.Minute, args...)
			if err != nil {
				for _, rr := range rows {
					if rr.Size == row.Size {
						set(rr.ID, &Measure{Error: "create topic: " + lastLine(out)})
					}
				}
				continue
			}
			// Warm-up: JIT, page cache, connections. Not measured.
			r.tool(5*time.Minute, producerArgs(bootstrap, topic, row.Size, 20000, "all")...)
		}
		if _, failed := legs[row.ID][name]; failed {
			continue
		}
		m := &Measure{OK: true}
		var p50, p99, mbs []float64
		for i := 0; i < reps; i++ {
			var out string
			var err error
			if row.Kind == "produce" {
				out, err = r.tool(15*time.Minute, producerArgs(bootstrap, topic, row.Size, row.Records, row.Acks)...)
			} else {
				out, err = r.tool(15*time.Minute, "/opt/kafka/bin/kafka-consumer-perf-test.sh", "--bootstrap-server", bootstrap,
					"--topic", topic, "--messages", fmt.Sprint(row.Records), "--timeout", "60000")
			}
			r.keep(name, row.ID, i, out)
			if err != nil {
				m = &Measure{Error: fmt.Sprintf("repetition %d: %v %s", i+1, err, lastLine(out))}
				break
			}
			if row.Kind == "produce" {
				p, perr := parse.ParseProducer(out)
				if perr != nil || p.Records != int64(row.Records) {
					m = &Measure{Error: fmt.Sprintf("repetition %d: %v (records %d of %d)", i+1, perr, p.Records, row.Records)}
					break
				}
				m.Reps = append(m.Reps, p.RecordsPerSec)
				mbs, p50, p99 = append(mbs, p.MBPerSec), append(p50, p.P50Ms), append(p99, p.P99Ms)
			} else {
				c, cerr := parse.ParseConsumer(out)
				if cerr != nil || c.Records < int64(row.Records) {
					m = &Measure{Error: fmt.Sprintf("repetition %d: %v (records %d of %d)", i+1, cerr, c.Records, row.Records)}
					break
				}
				// Fetch rates: the group join (rebalance time) is not consumer throughput.
				m.Reps = append(m.Reps, c.FetchRecordsPerSec)
				mbs = append(mbs, c.FetchMBPerSec)
			}
		}
		if m.OK {
			m.RecordsPerSec, m.MBPerSec, m.P50Ms, m.P99Ms = parse.Median(m.Reps), parse.Median(mbs), parse.Median(p50), parse.Median(p99)
		}
		logf("%s %-28s %s", name, row.ID, describe(m))
		set(row.ID, m)
	}
}

// keep saves each tool's raw output under the run directory (it goes into the raw-log tarball,
// never into git).
func (r *runner) keep(broker, id string, rep int, out string) {
	d := filepath.Join(os.Getenv("CERT_RUN_DIR"), "transcripts", "perf")
	_ = os.MkdirAll(d, 0o755)
	_ = os.WriteFile(filepath.Join(d, fmt.Sprintf("%s-%s-rep%d.txt", broker, id, rep+1)), []byte(out), 0o644)
}

func producerArgs(bootstrap, topic string, size, records int, acks string) []string {
	return []string{"/opt/kafka/bin/kafka-producer-perf-test.sh", "--topic", topic, "--num-records", fmt.Sprint(records),
		"--record-size", fmt.Sprint(size), "--throughput", "-1",
		"--producer-props", "bootstrap.servers=" + bootstrap, "acks=" + acks, "compression.type=none"}
}

func describe(m *Measure) string {
	if !m.OK {
		return "error: " + m.Error
	}
	s := fmt.Sprintf("%.0f records/s, %.1f MB/s", m.RecordsPerSec, m.MBPerSec)
	if m.P99Ms > 0 {
		s += fmt.Sprintf(", 99th-percentile latency %.0f ms", m.P99Ms)
	}
	if m.OtherRunning {
		s += " (the other broker was running: not a one-broker-at-a-time measurement)"
	}
	return s
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
}

type Results struct {
	Schema  int               `json:"schema"`
	Track   string            `json:"track"`
	Rows    []Row             `json:"rows"`
	Clients map[string]string `json:"clients"`
}

// Finish turns the measurements into verdicts and the summary table. Pure apart from reading
// the two failure markers, for the tests.
func Finish(rows []matrixRow, legs Legs, failMarkers ...string) (Results, string) {
	res := Results{Schema: 1, Track: "perf", Clients: map[string]string{"kafka-perf-tools": os.Getenv("KAFKA_VERSION")}}
	why := ""
	for _, f := range failMarkers {
		if b, err := os.ReadFile(f); err == nil {
			why = strings.TrimSpace(string(b))
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "| Row | Kafka records/s | Kafka MB/s | Kafka 99th-pct ms | KubeMQ records/s | KubeMQ MB/s | KubeMQ 99th-pct ms |\n|---|---|---|---|---|---|---|\n")
	for _, r := range rows {
		row := Row{ID: r.ID, Kind: "measurement", Title: r.Title, Provenance: r.Provenance, Source: r.Source}
		k, q := legs[r.ID]["kafka"], legs[r.ID]["kubemq"]
		switch {
		case why != "":
			row.Verdict, row.Detail = "VOID", "the broker pair did not start: "+why
		case k == nil || !k.OK:
			row.Verdict, row.Detail = "VOID", "Kafka measurement did not complete: "+errOf(k)
		case q == nil || !q.OK:
			row.Verdict, row.Detail = "FAIL", "KubeMQ measurement did not complete: "+errOf(q)
		default:
			row.Verdict, row.Detail = "PASS", "measured (not judged): Kafka "+describe(k)+"; KubeMQ "+describe(q)
		}
		res.Rows = append(res.Rows, row)
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s |\n", row.ID, num(k, "rps"), num(k, "mbs"), num(k, "p99"), num(q, "rps"), num(q, "mbs"), num(q, "p99"))
	}
	return res, b.String()
}

func errOf(m *Measure) string {
	if m == nil {
		return "no measurement"
	}
	return m.Error
}

func num(m *Measure, what string) string {
	if m == nil || !m.OK {
		return "n/a"
	}
	switch what {
	case "rps":
		return fmt.Sprintf("%.0f", m.RecordsPerSec)
	case "mbs":
		return fmt.Sprintf("%.1f", m.MBPerSec)
	default:
		if m.P99Ms == 0 {
			return "-"
		}
		return fmt.Sprintf("%.0f", m.P99Ms)
	}
}

func loadMatrix(path string) ([]matrixRow, error) {
	var m struct {
		Track string      `yaml:"track"`
		Rows  []matrixRow `yaml:"rows"`
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m.Track != "perf" || len(m.Rows) == 0 {
		return nil, fmt.Errorf("%s: want track perf with rows", path)
	}
	seen := map[string]bool{}
	for _, r := range m.Rows {
		switch {
		case r.ID == "" || seen[r.ID]:
			return nil, fmt.Errorf("%s: empty or duplicate id %q", path, r.ID)
		case r.Kind != "produce" && r.Kind != "consume":
			return nil, fmt.Errorf("%s: row %s: kind must be produce or consume", path, r.ID)
		case r.Kind == "produce" && r.Acks != "all" && r.Acks != "1":
			return nil, fmt.Errorf("%s: row %s: acks must be all or 1", path, r.ID)
		case r.Size <= 0 || r.Records <= 0:
			return nil, fmt.Errorf("%s: row %s: size and records must be positive", path, r.ID)
		case r.Provenance != "upstream" && r.Provenance != "kubemq-authored", r.Source == "":
			return nil, fmt.Errorf("%s: row %s: provenance and source are mandatory", path, r.ID)
		}
		seen[r.ID] = true
	}
	return m.Rows, nil
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
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

func logf(format string, args ...any) { fmt.Fprintf(os.Stderr, "[perf] "+format+"\n", args...) }

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "[perf] ERROR:", msg)
	os.Exit(2)
}
