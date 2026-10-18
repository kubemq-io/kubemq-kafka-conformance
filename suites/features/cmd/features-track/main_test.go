package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
)

func TestVerdictMapping(t *testing.T) {
	day := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	oracle := probe.Observation{"code": 10, "consumed": 5}
	same := probe.Observation{"code": 10, "consumed": 5}
	differ := probe.Observation{"code": 0, "consumed": 5}
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

func TestOptInRowsAreNotRunUnlessIncluded(t *testing.T) {
	r := matrixRow{ID: "x", OptIn: "stops the broker", LastResult: "FAIL on v1"}
	d := optInExcluded(r, false)
	if d == "" || !strings.Contains(d, "stops the broker") || !strings.Contains(d, "FAIL on v1") {
		t.Fatalf("an opt-in row without the flag must be excluded with its reason and last result: %q", d)
	}
	if optInExcluded(r, true) != "" {
		t.Fatal("with the flag the opt-in row runs")
	}
	if optInExcluded(matrixRow{ID: "y"}, false) != "" {
		t.Fatal("an ordinary row always runs")
	}
}

func TestDecideMapping(t *testing.T) {
	day := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	good := probe.Observation{"log_start_advanced_past_first_records_within_120s": true}
	bad := probe.Observation{"log_start_advanced_past_first_records_within_120s": false}
	ran := false
	kq := func(o probe.Observation, ok bool) func() *Leg {
		return func() *Leg {
			ran = true
			return &Leg{OK: ok, Observation: o, Error: map[bool]string{false: "boom"}[ok]}
		}
	}
	cases := []struct {
		name   string
		oracle *Leg
		kubemq func() *Leg
		want   string
		ranKQ  bool
	}{
		{"oracle leg failed", &Leg{OK: false, Error: "x"}, kq(good, true), "VOID", false},
		{"oracle misses the documented behaviour", &Leg{OK: true, Observation: bad}, kq(bad, true), "VOID", false},
		{"KubeMQ leg failed", &Leg{OK: true, Observation: good}, kq(nil, false), "FAIL", true},
		{"identical", &Leg{OK: true, Observation: good}, kq(good, true), "PASS", true},
		{"KubeMQ differs", &Leg{OK: true, Observation: good}, kq(bad, true), "FAIL", true},
	}
	for _, c := range cases {
		ran = false
		row := &Row{ID: "r"}
		v, d := decide("log-retention-ms", c.oracle, c.kubemq, row, nil, day)
		if v != c.want || ran != c.ranKQ {
			t.Errorf("%s: verdict %s (ran KubeMQ %v), want %s (%v); %s", c.name, v, ran, c.want, c.ranKQ, d)
		}
	}
}

func TestEveryProbeHasAnOracleExpectation(t *testing.T) {
	for name := range probe.Registry {
		if _, ok := probe.Expectations[name]; !ok {
			t.Errorf("probe %s declares no oracle expectation", name)
		}
	}
	for name := range probe.Expectations {
		if _, ok := probe.Registry[name]; !ok {
			t.Errorf("expectation for unknown probe %s", name)
		}
	}
}

func TestMatrixNamesRegisteredProbes(t *testing.T) {
	m, err := loadMatrix("../../../../matrix/features.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range m.Rows {
		if probe.Registry[r.Probe] == nil {
			t.Errorf("row %s names unknown probe %q", r.ID, r.Probe)
		}
	}
}

func TestOptInRowsMustComeLast(t *testing.T) {
	p := filepath.Join(t.TempDir(), "m.yaml")
	body := "track: f\nrows:\n  - {id: a, probe: x, provenance: upstream, source: s, opt_in: reason}\n  - {id: b, probe: x, provenance: upstream, source: s}\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadMatrix(p); err == nil {
		t.Fatal("an ordinary row after an opt-in row must be rejected")
	}
}

// A GAP row carries exactly the deviation text the register signed off, which is what the
// renderer checks again; other verdicts carry none.
func TestDecideGapCarriesSignedDeviation(t *testing.T) {
	day := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	good := probe.Observation{"log_start_advanced_past_first_records_within_120s": true}
	bad := probe.Observation{"log_start_advanced_past_first_records_within_120s": false}
	dev, _ := probe.Diff(good, bad)
	p := filepath.Join(t.TempDir(), "gaps.md")
	if err := os.WriteFile(p, []byte("| Row | Deviation | Signed off |\n|---|---|---|\n| `r` | "+dev+" | reviewer 2026-10-06 |\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	reg, err := gaps.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	kq := func(o probe.Observation) func() *Leg { return func() *Leg { return &Leg{OK: true, Observation: o} } }

	row := &Row{ID: "r"}
	if v, d := decide("log-retention-ms", &Leg{OK: true, Observation: good}, kq(bad), row, reg, day); v != "GAP" {
		t.Fatalf("registered difference: %s (%s)", v, d)
	}
	if row.Deviation != reg["r"].Deviation {
		t.Errorf("deviation %q, register signed %q", row.Deviation, reg["r"].Deviation)
	}
	row = &Row{ID: "r"}
	if v, _ := decide("log-retention-ms", &Leg{OK: true, Observation: good}, kq(good), row, reg, day); v != "PASS" || row.Deviation != "" {
		t.Errorf("PASS row: verdict %s, deviation %q", v, row.Deviation)
	}
}
