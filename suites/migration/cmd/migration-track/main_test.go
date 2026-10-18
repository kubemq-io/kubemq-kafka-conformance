package main

import (
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
)

func okResume() map[string]any {
	return map[string]any{"some_translated": true, "never_ahead": true, "started_at_commit": true,
		"nothing_skipped": true, "no_duplicates": true, "in_order": true, "offsets_identical": true}
}

func TestJudge(t *testing.T) {
	bad := okResume()
	bad["nothing_skipped"] = false
	const dev = "nothing_skipped: Kafka true, KubeMQ false"
	signed := map[string]gaps.Entry{rowResumeFranz: {Row: rowResumeFranz, Deviation: dev, SignedBy: "reviewer",
		Signed: time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC), Valid: true}}
	runDate := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		o, k    *Leg
		reg     map[string]gaps.Entry
		verdict string
	}{
		{"same", &Leg{OK: true, Observation: okResume()}, &Leg{OK: true, Observation: okResume()}, nil, "PASS"},
		{"KubeMQ skipped", &Leg{OK: true, Observation: okResume()}, &Leg{OK: true, Observation: bad}, nil, "FAIL"},
		{"KubeMQ skipped, registered", &Leg{OK: true, Observation: okResume()}, &Leg{OK: true, Observation: bad}, signed, "GAP"},
		{"oracle skipped", &Leg{OK: true, Observation: bad}, &Leg{OK: true, Observation: okResume()}, nil, "VOID"},
		{"oracle leg failed", &Leg{Error: "boom"}, &Leg{OK: true, Observation: okResume()}, nil, "VOID"},
		{"KubeMQ leg failed", &Leg{OK: true, Observation: okResume()}, &Leg{Error: "boom"}, nil, "FAIL"},
	}
	for _, c := range cases {
		row := Row{ID: rowResumeFranz}
		judge(&row, rowResumeFranz, c.o, c.k, c.reg, runDate)
		if row.Verdict != c.verdict {
			t.Errorf("%s: %s (%s), want %s", c.name, row.Verdict, row.Detail, c.verdict)
		}
		if (row.Verdict == "GAP" || row.Verdict == "FAIL") && c.k.OK && row.Deviation == "" {
			t.Errorf("%s: difference without deviation", c.name)
		}
		if row.Verdict == "GAP" && row.Deviation != dev {
			t.Errorf("%s: deviation %q", c.name, row.Deviation)
		}
	}
}

func TestCommittedMatrixLoads(t *testing.T) {
	rows, err := loadMatrix("../../../../matrix/migration.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(good) {
		t.Fatalf("matrix has %d rows, suite judges %d", len(rows), len(good))
	}
}

// The oracle precondition is looked up by the matrix row id.
func TestJudgeOraclePrecondition(t *testing.T) {
	bad := okResume()
	bad["nothing_skipped"] = false
	row := Row{ID: rowResumeFranz}
	judge(&row, rowResumeFranz, &Leg{OK: true, Observation: bad}, &Leg{OK: true, Observation: okResume()}, nil, time.Now())
	if row.Verdict != "VOID" {
		t.Fatalf("oracle precondition by base id: %s", row.Verdict)
	}
}
