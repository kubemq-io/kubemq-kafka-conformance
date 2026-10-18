package metric

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The committed matrix must load: every row valid, every expectation evaluable.
func TestCommittedMatrixLoads(t *testing.T) {
	m, err := LoadMatrix(filepath.Join("..", "..", "..", "matrix", "operations.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Rows) == 0 {
		t.Fatal("no rows")
	}
}

func TestLoadMatrixRejectsBadRows(t *testing.T) {
	good := `track: operations
rows:
  - id: operations.x
    group: g
    title: t
    workload: produce-consume
    kafka: {name: k}
    kubemq: {name: q}
    expect: {kind: delta-exact, of: records}
    provenance: kubemq-authored
    source: s
`
	dir := t.TempDir()
	write := func(s string) string {
		p := filepath.Join(dir, "m.yaml")
		if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if _, err := LoadMatrix(write(good)); err != nil {
		t.Fatalf("control: %v", err)
	}
	for name, mut := range map[string][2]string{
		"wrong track":           {"track: operations", "track: features"},
		"bad id prefix":         {"id: operations.x", "id: features.x"},
		"unknown workload":      {"workload: produce-consume", "workload: chaos"},
		"quantity not recorded": {"of: records", "of: lag"},
		"bad provenance":        {"provenance: kubemq-authored", "provenance: vendor"},
		"no source":             {"source: s", "source: \"\""},
		"empty kubemq selector": {"kubemq: {name: q}", "kubemq: {labels: {a: b}}"},
		"unknown field":         {"group: g", "group: g\n    grop: g"},
		"unknown expectation":   {"kind: delta-exact", "kind: exact"},
	} {
		if _, err := LoadMatrix(write(strings.Replace(good, mut[0], mut[1], 1))); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := LoadMatrix(write(good + strings.TrimPrefix(good, "track: operations\nrows:\n"))); err == nil {
		t.Error("duplicate id accepted")
	}
}
