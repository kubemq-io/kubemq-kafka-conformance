package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fprobe "github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/probe"
)

func TestVerdictMapping(t *testing.T) {
	day := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	oracle := probe.Observation{"code": 10, "consumed": 5}
	same := probe.Observation{"code": 10, "consumed": 5}
	differ := probe.Observation{"code": 0, "consumed": 5}
	dev, _ := fprobe.Diff(oracle, differ)

	p := filepath.Join(t.TempDir(), "gaps.md")
	reg := func(line string) map[string]gaps.Entry {
		if err := os.WriteFile(p, []byte("| Row | Deviation | Signed off |\n|---|---|---|\n"+line+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		r, err := gaps.Load(p)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if v, _ := Verdict("r", oracle, same, nil, day); v != "PASS" {
		t.Errorf("identical: %s", v)
	}
	if v, _ := Verdict("r", oracle, differ, nil, day); v != "FAIL" {
		t.Errorf("unregistered difference: %s", v)
	}
	if v, _ := Verdict("r", oracle, differ, reg("| `r` | "+dev+" | reviewer 2026-10-06 |"), day); v != "GAP" {
		t.Errorf("registered exact difference: %s", v)
	}
	if v, _ := Verdict("r", oracle, probe.Observation{"code": 0, "consumed": 4}, reg("| `r` | "+dev+" | reviewer 2026-10-06 |"), day); v != "FAIL" {
		t.Errorf("difference beyond the registered one: %s", v)
	}
	if v, _ := Verdict("r", oracle, differ, reg("| `r` | "+dev+" | reviewer 2026-10-07 |"), day); v != "FAIL" {
		t.Errorf("signed after the run: %s", v)
	}
}

func TestLoadMatrixRequiresProvenanceAndSource(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"no source":  "track: f\nrows:\n  - {id: a, probe: x, provenance: upstream}\n",
		"provenance": "track: f\nrows:\n  - {id: a, probe: x, provenance: vendor, source: s}\n",
		"duplicate":  "track: f\nrows:\n  - {id: a, probe: x, provenance: upstream, source: s}\n  - {id: a, probe: x, provenance: upstream, source: s}\n",
	} {
		p := filepath.Join(dir, name+".yaml")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := loadMatrix(p); err == nil {
			t.Errorf("%s: must be rejected", name)
		}
	}
}

func TestRowBindsItsOwnGap(t *testing.T) {
	day := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	oracle := probe.Observation{"code": 10}
	kubemq := probe.Observation{"code": 0}
	dev, _ := fprobe.Diff(oracle, kubemq)
	p := filepath.Join(t.TempDir(), "gaps.md")
	if err := os.WriteFile(p, []byte("| Row | Deviation | Signed off |\n|---|---|---|\n| `tools.x` | "+dev+" | reviewer 2026-10-07 |\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	reg, err := gaps.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := Verdict("tools.x", oracle, kubemq, reg, day); v != "GAP" {
		t.Errorf("single-node row with its own entry: %s", v)
	}
}

// The Burrow row compares group_listed and the cluster fields; lag availability differs between
// Kafka 4.3.1 (Burrow cannot decode its v4 offset commits) and a broker writing v3, and is only
// reported.
func TestBurrowLagAvailabilityIsInformational(t *testing.T) {
	day := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	oracle := probe.Observation{"api_answers": true, "cluster_listed": true, "topic_end_offsets_match": true, "group_listed": true, "group_lag_available": false}
	kubemq := probe.Observation{"api_answers": true, "cluster_listed": true, "topic_end_offsets_match": true, "group_listed": true, "group_lag_available": true}
	if v, _ := Verdict("tools.lag.burrow", oracle, kubemq, nil, day); v != "FAIL" {
		t.Fatalf("control: the raw observations differ in lag availability, so a raw comparison must fail, got %s", v)
	}
	v, detail, dev := compareLegs("tools.lag.burrow", "lag-burrow", oracle, kubemq, nil, day)
	if v != "PASS" || dev != "" {
		t.Errorf("lag availability alone must not decide the row: %s %q", v, dev)
	}
	if !strings.Contains(detail, "group_lag_available: Kafka false, KubeMQ true") {
		t.Errorf("lag availability not reported: %s", detail)
	}
	if _, ok := kubemq["group_lag_available"]; !ok {
		t.Error("the KubeMQ leg lost its lag observation")
	}
	notListed := probe.Observation{"api_answers": true, "cluster_listed": true, "topic_end_offsets_match": true, "group_listed": false, "group_lag_available": false}
	v, _, dev = compareLegs("tools.lag.burrow", "lag-burrow", oracle, notListed, nil, day)
	if v != "FAIL" || dev != "group_listed: Kafka true, KubeMQ false" {
		t.Errorf("group_listed must stay compared, and alone: %s %q", v, dev)
	}
}
