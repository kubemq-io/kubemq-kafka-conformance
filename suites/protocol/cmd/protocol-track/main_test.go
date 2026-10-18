package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	c "github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/conformance"
)

// The row-level verdict mapping, end to end against two in-memory kfake brokers standing in
// for the oracle and KubeMQ: a clean pair is PASS, a broken KubeMQ leg is FAIL, a broken
// oracle leg is VOID.
func TestRunRowVerdicts(t *testing.T) {
	cases := []struct {
		name        string
		breakOracle bool
		breakKubeMQ bool
		want        string
		driver      func() c.Driver
	}{
		{"clean full transcript", false, false, "PASS", func() c.Driver { return c.NewFranzGoDriverWithStepBound(5 * time.Second) }},
		{"KubeMQ answers a wrong error code", false, true, "FAIL", func() c.Driver { return c.NewFranzGoDriverWithStepBound(5 * time.Second) }},
		{"oracle answers a wrong error code", true, false, "VOID", func() c.Driver { return c.NewFranzGoDriverWithStepBound(5 * time.Second) }},
		{"clean smoke", false, false, "PASS", func() c.Driver { return c.NewSaramaDriver() }},
		{"KubeMQ breaks a smoke round-trip", false, true, "FAIL", func() c.Driver { return c.NewSaramaDriver() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oracle, err := c.NewKfakeTarget()
			if err != nil {
				t.Fatal(err)
			}
			defer oracle.Close()
			kubemq, err := c.NewKfakeTarget()
			if err != nil {
				t.Fatal(err)
			}
			defer kubemq.Close()
			if tc.breakOracle {
				oracle.InjectProduceError(10)
			}
			if tc.breakKubeMQ {
				kubemq.InjectProduceError(10)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			row := Row{ID: "test"}
			runRow(ctx, &row, c.ScenarioCore(), tc.driver(), oracle.BootstrapAddr(), kubemq.BootstrapAddr(), "")
			if row.Verdict != tc.want {
				t.Fatalf("verdict %s, want %s; detail: %s", row.Verdict, tc.want, firstLine(row.Detail))
			}
			if tc.want == "VOID" && !strings.Contains(row.Detail, "oracle") {
				t.Fatalf("a VOID must name the oracle leg: %s", row.Detail)
			}
		})
	}
}

func TestLoadMatrixRejectsBadRows(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"dup":        "track: p\nrows:\n  - {id: a, provenance: upstream}\n  - {id: a, provenance: upstream}\n",
		"provenance": "track: p\nrows:\n  - {id: a, provenance: vendor}\n",
		"empty":      "track: p\nrows: []\n",
	} {
		p := dir + "/" + name + ".yaml"
		if err := writeFile(p, body); err != nil {
			t.Fatal(err)
		}
		if _, err := loadMatrix(p); err == nil {
			t.Errorf("%s: loadMatrix must reject it", name)
		}
	}
}

func writeFile(p, s string) error { return os.WriteFile(p, []byte(s), 0o644) }
