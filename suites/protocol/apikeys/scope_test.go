package apikeys

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildBrokerInternalScopeIsExplicitAndRestricted(t *testing.T) {
	ov := Versions{Keys: map[int16][2]int16{27: {1, 2}, 83: {0, 0}, 84: {0, 0}, 85: {0, 1}, 86: {0, 0}, 87: {0, 1}, 64: {0, 0}, 1: {4, 18}}}
	impacts := map[int16]Impact{}
	for key := range ov.Keys {
		impacts[key] = Impact{Key: key, Breaks: "visible", Impact: "unsupported", Scope: "broker-internal"}
	}
	doc, missing := Build(ov, Versions{Keys: map[int16][2]int16{}}, impacts)
	if len(missing) != 0 || doc.Summary["not-applicable"] != 6 || doc.Summary["not-advertised"] != 2 {
		t.Fatalf("wrong scope counts: %v missing %v", doc.Summary, missing)
	}
	for _, row := range doc.Keys {
		if IsBrokerInternalKey(row.Key) != (row.Status == "not-applicable") {
			t.Fatalf("key %d was misclassified: %+v", row.Key, row)
		}
	}
	// An advertisement is measured normally; an absent-only exclusion must not
	// conceal an implementation that advertises an unsupported range.
	doc, _ = Build(ov, Versions{Keys: map[int16][2]int16{27: {1, 1}}}, impacts)
	if doc.Keys[1].Key != 27 || doc.Keys[1].Status != "narrower" {
		t.Fatalf("advertised broker request hidden: %+v", doc.Keys)
	}
}

func TestLoadImpactsRejectsClientScopeExclusion(t *testing.T) {
	p := filepath.Join(t.TempDir(), "impacts.yaml")
	if err := os.WriteFile(p, []byte("impacts:\n  - key: 1\n    impact: hidden\n    breaks: consumers\n    scope: broker-internal\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadImpacts(p); err == nil {
		t.Fatal("a client-facing Fetch exclusion was accepted")
	}
}
