package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/apikeys"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
)

func i16(v int16) *int16 { return &v }

// fakeDoc: key 0 full, key 1 narrower, key 2 missing, key 99 KubeMQ-only; one feature missing,
// one the same.
func fakeDoc() apikeys.Doc {
	return apikeys.Doc{
		Summary: map[string]int{"full": 1, "narrower": 1, "not-advertised": 1, "kubemq-only": 1},
		Keys: []apikeys.KeyRow{
			{ID: "apikey.0", Key: 0, Name: "Produce", OracleMin: 3, OracleMax: 9, KubeMQMin: i16(3), KubeMQMax: i16(9), Status: "full", Provenance: "kubemq-authored"},
			{ID: "apikey.1", Key: 1, Name: "Fetch", OracleMin: 4, OracleMax: 18, KubeMQMin: i16(4), KubeMQMax: i16(12), Status: "narrower", Breaks: "b", Impact: "i", Provenance: "kubemq-authored"},
			{ID: "apikey.2", Key: 2, Name: "ListOffsets", OracleMin: 1, OracleMax: 11, Status: "not-advertised", Breaks: "b", Impact: "i", Provenance: "kubemq-authored"},
			{ID: "apikey.99", Key: 99, Name: "Key99", KubeMQMin: i16(0), KubeMQMax: i16(0), Status: "kubemq-only", Provenance: "kubemq-authored"},
		},
		Features: []apikeys.FeatureRow{
			{ID: "feature.a", Feature: "a", OracleSupported: "0-1", OracleFinalized: "1", KubeMQSupported: "-", KubeMQFinalized: "-", Status: "not-advertised"},
			{ID: "feature.b", Feature: "b", OracleSupported: "0-1", OracleFinalized: "1", KubeMQSupported: "0-1", KubeMQFinalized: "1", Status: "same"},
		},
	}
}

func verdicts(rows []Row) map[string]string {
	m := map[string]string{}
	for _, r := range rows {
		m[r.ID] = r.Verdict
		if r.Kind != "advertisement" && !(r.Kind == "broker-internal" && r.Verdict == "NOT-APPLICABLE") {
			m[r.ID] += "/wrong-kind"
		}
	}
	return m
}

func register(t *testing.T, lines ...string) map[string]gaps.Entry {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gaps.md")
	body := "| Row | Deviation | Signed off |\n|---|---|---|\n" + strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	reg, err := gaps.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

var runDay = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func TestClassifyUnregistered(t *testing.T) {
	doc := fakeDoc()
	b, _ := apikeys.Render(doc)
	got := verdicts(classifyAPIKeys(doc, nil, b, b, nil, nil, runDay))
	want := map[string]string{"apikeys.matrix-current": "PASS", "apikey.0": "PASS", "apikey.1": "FAIL", "apikey.2": "FAIL", "apikey.99": "PASS", "feature.a": "FAIL", "feature.b": "PASS"}
	for id, v := range want {
		if got[id] != v {
			t.Errorf("%s: %s, want %s", id, got[id], v)
		}
	}
}

func TestClassifyRegisteredExactDeviationIsGap(t *testing.T) {
	doc := fakeDoc()
	b, _ := apikeys.Render(doc)
	reg := register(t,
		"| apikey.1 | "+apikeys.KeyDeviation(doc.Keys[1])+" | reviewer 2026-10-05 |",
		"| apikey.2 | "+apikeys.KeyDeviation(doc.Keys[2])+" | reviewer 2026-10-06 |", // signed after the run
		"| feature.a | "+apikeys.FeatureDeviation(doc.Features[0])+" | - |",          // unsigned
	)
	got := verdicts(classifyAPIKeys(doc, nil, b, b, nil, reg, runDay))
	if got["apikey.1"] != "GAP" || got["apikey.2"] != "FAIL" || got["feature.a"] != "FAIL" {
		t.Fatalf("got %v", got)
	}
}

func TestClassifyChangedDeviationNeedsNewSignOff(t *testing.T) {
	doc := fakeDoc()
	signed := apikeys.KeyDeviation(doc.Keys[1]) // v4-v12 signed
	reg := register(t, "| apikey.1 | "+signed+" | reviewer 2026-10-01 |")
	doc.Keys[1].KubeMQMax = i16(7) // the next release advertises less
	b, _ := apikeys.Render(doc)
	rows := classifyAPIKeys(doc, nil, b, b, nil, reg, runDay)
	for _, r := range rows {
		if r.ID == "apikey.1" && (r.Verdict != "FAIL" || !strings.Contains(r.Detail, "differs")) {
			t.Fatalf("a deviation that changed since sign-off must FAIL: %s %s", r.Verdict, r.Detail)
		}
	}
}

func TestClassifyMatrixCurrent(t *testing.T) {
	doc := fakeDoc()
	b, _ := apikeys.Render(doc)
	stale := append([]byte("# edited\n"), b...)
	cases := map[string]struct {
		missing   []int16
		committed []byte
		err       error
	}{
		"stale":         {nil, stale, nil},
		"unreadable":    {nil, nil, errors.New("no such file")},
		"uncurated key": {[]int16{1}, b, nil},
	}
	for name, c := range cases {
		rows := classifyAPIKeys(doc, c.missing, b, c.committed, c.err, nil, runDay)
		if rows[0].ID != "apikeys.matrix-current" || rows[0].Verdict != "FAIL" {
			t.Errorf("%s: matrix-current %s, want FAIL", name, rows[0].Verdict)
		}
	}
}
