package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/security/probe"
)

var day = time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)

func TestVerdictBindsTheSignedDeviation(t *testing.T) {
	kafka := probe.Observation{"error_code": 58}
	kubemq := probe.Observation{"error_code": 0}
	if v, _ := Verdict("security.x", kafka, kafka, nil, day); v != "PASS" {
		t.Fatalf("identical observations: %s", v)
	}
	if v, _ := Verdict("security.x", kafka, kubemq, nil, day); v != "FAIL" {
		t.Fatalf("wrong error code, no registration: %s", v)
	}
	reg := map[string]gaps.Entry{"security.x": {Row: "security.x", Deviation: "error_code: Kafka 58, KubeMQ 0", SignedBy: "reviewer", Signed: day, Valid: true}}
	if v, _ := Verdict("security.x", kafka, kubemq, reg, day); v != "GAP" {
		t.Fatalf("registered deviation: %s", v)
	}
	if v, _ := Verdict("security.x", kafka, probe.Observation{"error_code": 1}, reg, day); v != "FAIL" {
		t.Fatalf("a different deviation than the signed one must FAIL: %s", v)
	}
}

func TestFinishAccountsForEveryRow(t *testing.T) {
	m := matrix{Track: "security", Rows: []matrixRow{
		{ID: "a", Posture: "tls"}, {ID: "b", Posture: "tls"}, {ID: "c", Posture: "nope"},
	}}
	root := t.TempDir()
	if err := writeJSON(filepath.Join(root, "tls", "results.json"), []Row{{ID: "b", Verdict: "PASS"}}); err != nil {
		t.Fatal(err)
	}
	res := finish(m, root)
	got := map[string]string{}
	for _, r := range res.Rows {
		got[r.ID] = r.Verdict
	}
	if len(res.Rows) != 3 || res.Rows[0].ID != "a" || got["a"] != "NOT-RUN" || got["b"] != "PASS" || got["c"] != "NOT-RUN" {
		t.Fatalf("matrix order and NOT-RUN for missing rows: %+v", res.Rows)
	}
}

func TestFinishBootFailures(t *testing.T) {
	m := matrix{Track: "security", Rows: []matrixRow{{ID: "a", Posture: "tls"}}}
	for _, c := range []struct{ log, want string }{
		{"[brokers] ERROR: posture tls: oracle exited during startup; see logs/x", "VOID"},
		{"[brokers] ERROR: KubeMQ container kcert-security-kubemq exited during startup; see logs/y", "FAIL"},
	} {
		root := t.TempDir()
		_ = os.MkdirAll(filepath.Join(root, "tls"), 0o755)
		_ = os.WriteFile(filepath.Join(root, "tls", "boot-failed"), []byte("noise\n"+c.log+"\n"), 0o644)
		r := finish(m, root).Rows[0]
		if r.Verdict != c.want || !strings.Contains(r.Detail, "did not boot") {
			t.Errorf("%q: got %s %q, want %s", c.log, r.Verdict, r.Detail, c.want)
		}
	}
}

func TestMatrixLoads(t *testing.T) {
	m, err := loadMatrix("../../../../matrix/security.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range m.Rows {
		if probe.Registry[r.Probe] == nil {
			t.Errorf("row %s names probe %q, which is not implemented", r.ID, r.Probe)
		}
	}
}

func TestFinishPrepareFailure(t *testing.T) {
	m := matrix{Track: "security", Rows: []matrixRow{{ID: "a", Posture: "tls"}}}
	root := t.TempDir()
	_ = os.MkdirAll(filepath.Join(root, "tls"), 0o755)
	_ = os.WriteFile(filepath.Join(root, "tls", "prepare-failed"), []byte("track.sh prepare tls failed; see logs/track-security.log\n"), 0o644)
	if r := finish(m, root).Rows[0]; r.Verdict != "NOT-RUN" || !strings.Contains(r.Detail, "could not prepare posture tls") {
		t.Fatalf("got %s %q", r.Verdict, r.Detail)
	}
}
