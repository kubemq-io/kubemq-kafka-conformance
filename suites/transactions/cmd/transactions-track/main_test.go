package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
)

func TestVerdictMapping(t *testing.T) {
	day := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	oracle := probe.Observation{"call_outcomes": "abort=NONE", "read_committed_delivered": 9}
	same := probe.Observation{"call_outcomes": "abort=NONE", "read_committed_delivered": 9}
	differ := probe.Observation{"call_outcomes": "abort=INVALID_TXN_STATE", "read_committed_delivered": 9}
	dev, _ := probe.Diff(oracle, differ)

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
	if v, _ := Verdict("r", oracle, differ, reg("| `r` | "+dev+" | reviewer 2026-10-07 |"), day); v != "GAP" {
		t.Errorf("registered exact difference: %s", v)
	}
	if v, _ := Verdict("r", oracle, probe.Observation{"call_outcomes": "abort=INVALID_TXN_STATE", "read_committed_delivered": 8}, reg("| `r` | "+dev+" | reviewer 2026-10-07 |"), day); v != "FAIL" {
		t.Errorf("difference beyond the registered one: %s", v)
	}
	if v, _ := Verdict("r", oracle, differ, reg("| `r` | "+dev+" | reviewer 2026-10-08 |"), day); v != "FAIL" {
		t.Errorf("signed after the run: %s", v)
	}
}

func TestParseReport(t *testing.T) {
	if _, err := ParseReport("log line\n{\"client\":\"franz-go v1\",\"scenario\":\"isolation\",\"facts\":{}}\n"); err != nil {
		t.Fatalf("last line is the report: %v", err)
	}
	for name, out := range map[string]string{
		"empty":     "",
		"not json":  "done\n",
		"no client": "{\"scenario\":\"isolation\",\"facts\":{}}",
	} {
		if _, err := ParseReport(out); err == nil {
			t.Errorf("%s: must be refused", name)
		}
	}
}

func TestLoadMatrixRequiresClientScenarioProvenanceAndSource(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"no source":   "track: t\nrows:\n  - {id: a, client: franz-go, scenario: x, provenance: upstream}\n",
		"provenance":  "track: t\nrows:\n  - {id: a, client: franz-go, scenario: x, provenance: vendor, source: s}\n",
		"no client":   "track: t\nrows:\n  - {id: a, scenario: x, provenance: upstream, source: s}\n",
		"no scenario": "track: t\nrows:\n  - {id: a, client: franz-go, provenance: upstream, source: s}\n",
		"duplicate":   "track: t\nrows:\n  - {id: a, client: c, scenario: x, provenance: upstream, source: s}\n  - {id: a, client: c, scenario: x, provenance: upstream, source: s}\n",
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

func TestShippedMatrixLoads(t *testing.T) {
	m, err := loadMatrix("../../../../matrix/transactions.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if m.Track != "transactions" {
		t.Fatalf("track %q", m.Track)
	}
}
