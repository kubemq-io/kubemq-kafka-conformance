package metric

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Workloads the track drives between the two scrapes. Each records the quantities named here.
var Workloads = map[string][]string{
	// Produce `records` records (key + value = `payload_bytes` in total) to a fresh topic with
	// `partitions` partitions, acks=all, then consume all of them with a fresh group that
	// commits. Vars: topic, group.
	"produce-consume": {"records", "payload_bytes", "partitions"},
	// Produce `records` records to a fresh topic of 3 partitions, commit 30, 40 and 50 for a group
	// with no members; `lag` = records - consumed, `lag_p<n>` per partition. Vars: topic, group.
	"group-lag": {"records", "consumed", "lag", "lag_p0", "lag_p1", "lag_p2", "partitions"},
	// group-lag with every partition committed at its end: lag 0. Vars: topic, group.
	"group-lag-zero": {"records", "consumed", "lag", "lag_p0", "lag_p1", "lag_p2", "partitions"},
	// group-lag with one live member that received everything, committed the same offsets and
	// stays in the group across the second scrape. Vars: topic, group.
	"group-lag-active": {"records", "consumed", "lag", "lag_p0", "lag_p1", "lag_p2", "partitions"},
	// Produce `rejected` records, each larger than the broker's default 1 MiB record limit, to a
	// fresh topic; every one is refused with MESSAGE_TOO_LARGE (10). Vars: topic.
	"produce-rejected": {"rejected"},
	// Nothing but one fresh topic and one client connection held open across the second scrape:
	// steady-state gauges. Vars: topic.
	"idle": {"partitions"},
}

// ProbeNames lists the configuration-mapping probes the track implements; set by the track (the
// probes live in package config) so the loader can refuse an unknown probe name.
var ProbeNames map[string]bool

// Row is one row of matrix/operations.yaml.
type Row struct {
	ID         string    `yaml:"id"`
	Group      string    `yaml:"group"`
	Title      string    `yaml:"title"`
	Workload   string    `yaml:"workload,omitempty"`
	Probe      string    `yaml:"probe,omitempty"` // a configuration-mapping probe row instead of a metric row
	Kafka      Selector  `yaml:"kafka,omitempty"`
	KubeMQ     *Selector `yaml:"kubemq"` // nil: KubeMQ has no equivalent series; the row FAILs unless a gap is registered
	Expect     Expect    `yaml:"expect,omitempty"`
	Provenance string    `yaml:"provenance"`
	Source     string    `yaml:"source"`
}

// Matrix is matrix/operations.yaml.
type Matrix struct {
	Track string `yaml:"track"`
	Rows  []Row  `yaml:"rows"`
}

// LoadMatrix reads and validates the operations matrix.
func LoadMatrix(path string) (Matrix, error) {
	var m Matrix
	b, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&m); err != nil {
		return m, fmt.Errorf("parse %s: %w", path, err)
	}
	if m.Track != "operations" || len(m.Rows) == 0 {
		return m, fmt.Errorf("%s: want track operations with rows", path)
	}
	seen := map[string]bool{}
	for _, r := range m.Rows {
		if err := r.validate(); err != nil {
			return m, fmt.Errorf("%s: row %q: %w", path, r.ID, err)
		}
		if seen[r.ID] {
			return m, fmt.Errorf("%s: duplicate row id %q", path, r.ID)
		}
		seen[r.ID] = true
	}
	return m, nil
}

func (r Row) validate() error {
	if !strings.HasPrefix(r.ID, "operations.") || r.Group == "" || r.Title == "" {
		return fmt.Errorf("id must start with operations., group and title are mandatory")
	}
	if r.Provenance != "upstream" && r.Provenance != "kubemq-authored" {
		return fmt.Errorf("provenance must be upstream or kubemq-authored")
	}
	if r.Source == "" {
		return fmt.Errorf("source is mandatory")
	}
	if r.Probe != "" {
		if r.Workload != "" || r.Kafka.Name != "" || r.KubeMQ != nil || r.Expect.Kind != "" {
			return fmt.Errorf("a probe row has no workload, selectors or expectation")
		}
		if ProbeNames != nil && !ProbeNames[r.Probe] {
			return fmt.Errorf("unknown probe %q", r.Probe)
		}
		return nil
	}
	qs, ok := Workloads[r.Workload]
	if !ok {
		return fmt.Errorf("unknown workload %q (known: %s)", r.Workload, strings.Join(workloadNames(), ", "))
	}
	if r.Kafka.Name == "" {
		return fmt.Errorf("kafka selector needs a name")
	}
	if r.KubeMQ != nil && r.KubeMQ.Name == "" {
		return fmt.Errorf("kubemq selector needs a name, or omit it when KubeMQ has no equivalent")
	}
	if err := r.Expect.Validate(); err != nil {
		return err
	}
	if r.Expect.Of != "" && !contains(qs, r.Expect.Of) {
		return fmt.Errorf("expectation uses %q, which workload %s does not record (%s)", r.Expect.Of, r.Workload, strings.Join(qs, ", "))
	}
	return nil
}

func workloadNames() []string {
	out := make([]string, 0, len(Workloads))
	for k := range Workloads {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
