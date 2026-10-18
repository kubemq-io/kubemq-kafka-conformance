package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFinish(t *testing.T) {
	rows := []matrixRow{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	legs := Legs{
		"a": {"kafka": {OK: true, RecordsPerSec: 10}, "kubemq": {OK: true, RecordsPerSec: 5, P99Ms: 7}},
		"b": {"kafka": {OK: true}, "kubemq": {Error: "timeout"}},
		"c": {"kafka": {Error: "boom"}, "kubemq": {OK: true}},
	}
	none := filepath.Join(t.TempDir(), "absent")
	res, summary := Finish(rows, legs, none, none)
	got := []string{res.Rows[0].Verdict, res.Rows[1].Verdict, res.Rows[2].Verdict}
	if strings.Join(got, " ") != "PASS FAIL VOID" {
		t.Fatalf("%v", got)
	}
	for _, r := range res.Rows {
		if r.Kind != "measurement" {
			t.Fatalf("perf rows are measurements, outside the pass/fail counts: %+v", r)
		}
	}
	if !strings.Contains(summary, "| a | 10 |") || !strings.Contains(summary, "| 5 | 0.0 | 7 |") {
		t.Fatalf("summary:\n%s", summary)
	}
	bf := filepath.Join(t.TempDir(), "boot-failed")
	_ = os.WriteFile(bf, []byte("oracle failed"), 0o644)
	res, _ = Finish(rows, legs, none, bf)
	for _, r := range res.Rows {
		if r.Verdict != "VOID" {
			t.Fatalf("a pair that never started makes every row VOID: %+v", r)
		}
	}
}

func TestCommittedMatrixLoads(t *testing.T) {
	rows, err := loadMatrix("../../../../matrix/perf.yaml")
	if err != nil || len(rows) != 9 {
		t.Fatalf("%d rows, %v", len(rows), err)
	}
}
