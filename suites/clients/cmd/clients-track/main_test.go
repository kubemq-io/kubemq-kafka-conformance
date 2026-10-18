package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/clients/verify"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
)

func TestVerdictMapping(t *testing.T) {
	run := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	o := verify.Observation{"group_protocol": "classic", "records_acked": 10}
	k := verify.Observation{"group_protocol": "consumer", "records_acked": 10}
	dev := `group_protocol: Kafka "classic", KubeMQ "consumer"`
	signed := map[string]gaps.Entry{"r": {Row: "r", Deviation: dev, SignedBy: "s", Signed: run.AddDate(0, 0, -1), Valid: true}}
	late := map[string]gaps.Entry{"r": {Row: "r", Deviation: dev, SignedBy: "s", Signed: run.AddDate(0, 0, 1), Valid: true}}
	other := map[string]gaps.Entry{"r": {Row: "r", Deviation: "something else", SignedBy: "s", Signed: run, Valid: true}}
	for _, c := range []struct {
		name string
		k    verify.Observation
		reg  map[string]gaps.Entry
		want string
	}{
		{"identical", o, nil, "PASS"},
		{"differs, unregistered", k, nil, "FAIL"},
		{"differs, registered and signed", k, signed, "GAP"},
		{"differs, signed after the run", k, late, "FAIL"},
		{"differs otherwise than signed", k, other, "FAIL"},
	} {
		if got, detail := Verdict("r", o, c.k, c.reg, run); got != c.want {
			t.Errorf("%s: got %s (%s), want %s", c.name, got, detail, c.want)
		}
	}
}

func TestMatrixLoadsAndRejectsBadRows(t *testing.T) {
	if _, err := loadMatrix(filepath.Join("..", "..", "..", "..", "matrix", "clients.yaml")); err != nil {
		t.Fatalf("matrix/clients.yaml: %v", err)
	}
	dir := t.TempDir()
	for name, body := range map[string]string{
		"no driver":  "track: clients\nrows:\n- {id: a, title: t, client: c, version: v, mode: scenario, provenance: upstream, source: s}\n",
		"bad mode":   "track: clients\nrows:\n- {id: a, title: t, client: c, version: v, driver: d, mode: x, provenance: upstream, source: s}\n",
		"duplicate":  "track: clients\nrows:\n- {id: a, title: t, client: c, version: v, driver: d, mode: scenario, provenance: upstream, source: s}\n- {id: a, title: t, client: c, version: v, driver: d, mode: scenario, provenance: upstream, source: s}\n",
		"provenance": "track: clients\nrows:\n- {id: a, title: t, client: c, version: v, driver: d, mode: scenario, provenance: mine, source: s}\n",
	} {
		p := filepath.Join(dir, strings.ReplaceAll(name, " ", "-")+".yaml")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := loadMatrix(p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestEveryMatrixDriverIsKnown(t *testing.T) {
	m, err := loadMatrix(filepath.Join("..", "..", "..", "..", "matrix", "clients.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	d := newDrivers("", "")
	for _, r := range m.Rows {
		if !d.known(r.Driver) {
			t.Errorf("row %s names driver %q, which this suite does not build", r.ID, r.Driver)
		}
	}
}

func TestLegVerdictMapping(t *testing.T) {
	clean := &Leg{OK: true}
	broken := &Leg{OK: false, Problems: []string{"records never delivered: 1 (7)"}}
	for _, c := range []struct {
		name           string
		oracle, kubemq *Leg
		want           string
		done           bool
	}{
		{"oracle problem makes the row VOID", broken, nil, "VOID", true},
		{"oracle problem wins over a KubeMQ problem", broken, broken, "VOID", true},
		{"KubeMQ problem makes the row FAIL", clean, broken, "FAIL", true},
		{"clean oracle, KubeMQ not run yet", clean, nil, "", false},
		{"both clean: compare observations", clean, clean, "", false},
	} {
		v, _, done := legVerdict(c.oracle, c.kubemq)
		if v != c.want || done != c.done {
			t.Errorf("%s: got %q %v, want %q %v", c.name, v, done, c.want, c.done)
		}
	}
}
