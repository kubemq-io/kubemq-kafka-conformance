package report

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBrokerInternalScopeIsVisibleOutsideApplicationCounts(t *testing.T) {
	f := newFixture(t, []string{"client", "apikey.27"}, "protocol")
	writeJSON(t, filepath.Join(f.run, "run.json"), Run{Tracks: []string{"protocol"}, StartedUTC: "2026-10-13T08:00:00Z", KubeMQ: Image{Version: "dev"}})
	f.file("docs/gaps.md", "| Row | Deviation | Signed off |\n|---|---|---|\n| apikey.27 | excluded broker request | owner 2026-10-13T07:00Z |\n")
	f.results("protocol", Row{ID: "client", Verdict: Pass}, Row{ID: "apikey.27", Kind: BrokerInternal, Verdict: NotApplicable, Deviation: "excluded broker request", Detail: "Kafka broker interoperability remains unsupported"})
	r, err := f.build()
	if err != nil {
		t.Fatal(err)
	}
	ts := r.Tracks[0]
	if !r.Passed || ts.Rows != 1 || ts.Counts[Pass] != 1 || ts.Counts[Gap] != 0 || ts.NotApplicable != 1 {
		t.Fatalf("excluded row counted as a pass or gap: %+v", ts)
	}
	for _, output := range []string{ReportMarkdown(r), CompatibilityMarkdown(r)} {
		if !strings.Contains(output, "outside application compatibility: 1") || !strings.Contains(output, "`apikey.27` | NOT-APPLICABLE") || !strings.Contains(output, "Kafka broker interoperability remains unsupported") {
			t.Fatalf("exclusion disappeared from report: %s", output)
		}
	}
	f.results("protocol", Row{ID: "client", Verdict: Pass}, Row{ID: "apikey.27", Kind: BrokerInternal, Verdict: NotApplicable, Deviation: "excluded broker request", Provenance: "-"})
	r, err = f.build()
	if err != nil {
		t.Fatal(err)
	}
	if r.Passed || len(r.Tracks[0].NoProvenance) != 1 {
		t.Fatal("scope exclusion hid missing provenance")
	}
}

func TestBrokerInternalScopeCannotHideFailures(t *testing.T) {
	start := time.Date(2026, 10, 13, 8, 0, 0, 0, time.UTC)
	row := Row{ID: "apikey.27", Kind: BrokerInternal, Verdict: NotApplicable, Deviation: "excluded"}
	for _, tc := range []struct {
		name, id, kind, approved, signed string
	}{
		{"client", "apikey.1", BrokerInternal, "excluded", "owner 2026-10-13T07:00Z"},
		{"controller tool", "apikey.64", BrokerInternal, "excluded", "owner 2026-10-13T07:00Z"},
		{"wrong kind", "apikey.27", "advertisement", "excluded", "owner 2026-10-13T07:00Z"},
		{"late", "apikey.27", BrokerInternal, "excluded", "owner 2026-10-13T09:00Z"},
		{"changed", "apikey.27", BrokerInternal, "different", "owner 2026-10-13T07:00Z"},
		{"unsigned", "apikey.27", BrokerInternal, "excluded", "pending"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := row
			r.ID, r.Kind = tc.id, tc.kind
			r = applyRules(r, map[string]gapEntry{r.ID: {Signed: tc.signed, Deviation: tc.approved}}, nil, nil, "dev", start)
			if r.Verdict != Fail || r.Kind != "advertisement" {
				t.Fatalf("invalid exclusion survived: %+v", r)
			}
		})
	}
}
