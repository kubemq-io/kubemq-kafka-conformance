package verify

// Verifier mutation tests (docs/methodology.md): each injects one defect into a known-good
// driver report and proves the observation changes, so the row cannot stay PASS.

import (
	"encoding/json"
	"testing"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
)

func recs(keys []string, offsets []int64) []Rec {
	out := make([]Rec, len(keys))
	for i, k := range keys {
		out[i] = Rec{Partition: 0, Offset: offsets[i], Key: k, Value: ValueFor(k)}
	}
	return out
}

func span(from, to int64) []int64 {
	var o []int64
	for i := from; i <= to; i++ {
		o = append(o, i)
	}
	return o
}

func goodIsolation() IsolationFacts {
	c := append(Keys("c", 0, 6), Keys("c", 6, 3)...)
	all := append(append(Keys("c", 0, 6), Keys("a", 0, 4)...), Keys("c", 6, 3)...)
	return IsolationFacts{
		OpenLSO: 0, OpenHWM: 6, FinalLSO: 16, FinalHWM: 16,
		Outcomes:        map[string]string{"commit1": "NONE", "abort": "NONE", "commit2": "NONE"},
		ReadCommitted:   recs(c, append(span(0, 5), span(12, 14)...)),
		ReadUncommitted: recs(all, append(append(span(0, 5), span(7, 10)...), span(12, 14)...)),
	}
}

func clone(f IsolationFacts) IsolationFacts {
	b, _ := json.Marshal(f)
	var g IsolationFacts
	_ = json.Unmarshal(b, &g)
	return g
}

func TestIsolationControlIsClean(t *testing.T) {
	o := IsolationObservation(goodIsolation())
	want := map[string]any{
		"read_committed_offsets": "0-5,12-14", "read_uncommitted_offsets": "0-5,7-10,12-14",
		"read_committed_missing": 0, "read_committed_unexpected": 0, "read_uncommitted_missing": 0,
		"open_lso_below_hwm": true, "call_outcomes": "abort=NONE,commit1=NONE,commit2=NONE",
	}
	for k, v := range want {
		if o[k] != v {
			t.Errorf("%s = %v, want %v", k, o[k], v)
		}
	}
}

func TestMutation_IsolationDetectsEveryDefect(t *testing.T) {
	clean := IsolationObservation(goodIsolation())
	muts := map[string]func(f *IsolationFacts){
		"loss":      func(f *IsolationFacts) { f.ReadCommitted = append(f.ReadCommitted[:2:2], f.ReadCommitted[3:]...) },
		"duplicate": func(f *IsolationFacts) { f.ReadCommitted = append(f.ReadCommitted, f.ReadCommitted[4]) },
		"reorder": func(f *IsolationFacts) {
			f.ReadCommitted[1], f.ReadCommitted[2] = f.ReadCommitted[2], f.ReadCommitted[1]
		},
		"wrong offset":    func(f *IsolationFacts) { f.ReadCommitted[7].Offset = 9 },
		"wrong content":   func(f *IsolationFacts) { f.ReadUncommitted[3].Value = "x" },
		"aborted visible": func(f *IsolationFacts) { f.ReadCommitted = append(f.ReadCommitted, f.ReadUncommitted[6]) },
		"aborted filtered by broker": func(f *IsolationFacts) {
			f.ReadUncommitted = append(f.ReadUncommitted[:6:6], f.ReadUncommitted[10:]...)
		},
		"marker without offset":  func(f *IsolationFacts) { f.FinalLSO, f.FinalHWM = 13, 13 },
		"lso not pinned by open": func(f *IsolationFacts) { f.OpenLSO = 6 },
		"lso stuck after commit": func(f *IsolationFacts) { f.FinalLSO = 12 },
		"wrong error code":       func(f *IsolationFacts) { f.Outcomes["abort"] = "INVALID_TXN_STATE" },
	}
	for name, m := range muts {
		f := clone(goodIsolation())
		m(&f)
		if _, same := probe.Diff(clean, IsolationObservation(f)); same {
			t.Errorf("isolation verifier stayed green under %q", name)
		}
	}
}

func TestRanges(t *testing.T) {
	for in, want := range map[string]struct {
		offs []int64
		s    string
	}{
		"empty":     {nil, ""},
		"run":       {[]int64{0, 1, 2}, "0-2"},
		"gaps":      {[]int64{0, 1, 2, 5, 7, 8}, "0-2,5,7-8"},
		"duplicate": {[]int64{0, 1, 1, 2}, "0-1,1-2"},
		"backwards": {[]int64{0, 2, 1}, "0,2,1"},
	} {
		if got := Ranges(want.offs); got != want.s {
			t.Errorf("%s: %q, want %q", in, got, want.s)
		}
	}
}

func TestAnalyzeRejectsWrongScenario(t *testing.T) {
	b, _ := json.Marshal(goodIsolation())
	if _, err := Analyze("isolation", Report{Scenario: "other", Facts: b}); err == nil {
		t.Fatal("a report for another scenario must be refused")
	}
	if _, err := Analyze("isolation", Report{Scenario: "isolation", Facts: []byte("{")}); err == nil {
		t.Fatal("malformed facts must be refused")
	}
	if _, err := Analyze("isolation", Report{Scenario: "isolation", Facts: b}); err != nil {
		t.Fatal(err)
	}
}
