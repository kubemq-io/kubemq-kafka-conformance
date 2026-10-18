package verify

import "testing"

func sp(s string) *string { return &s }

func compactedSource() []Rec {
	return []Rec{
		{Partition: 0, Offset: 0, Key: sp("a"), Value: sp("1")},
		{Partition: 0, Offset: 1, Key: sp("b"), Value: sp("1")},
		{Partition: 0, Offset: 2, Key: sp("a"), Value: sp("2")},
		{Partition: 1, Offset: 0, Key: sp("c"), Value: sp("1")},
		{Partition: 0, Offset: 3, Key: sp("b"), Value: nil},
	}
}

func TestMutation_CompactedDetectsEveryDefect(t *testing.T) {
	src := compactedSource()
	// The target compacted already: a's old value and b entirely gone.
	tgt := []Rec{{Partition: 0, Offset: 2, Key: sp("a"), Value: sp("2")}, {Partition: 1, Offset: 0, Key: sp("c"), Value: sp("1")}}
	if c := CompareCompacted(src, tgt, []string{"b"}); !c.SameLatest || !c.DeletedDeleted {
		t.Fatalf("control: compaction on the target is not a difference: %+v", c)
	}
	muts := map[string][]Rec{
		"old value kept as latest (reorder)": {{Partition: 0, Offset: 2, Key: sp("a"), Value: sp("1")}, {Partition: 1, Offset: 0, Key: sp("c"), Value: sp("1")}},
		"tombstone lost (deleted key back)":  {{Partition: 0, Offset: 1, Key: sp("b"), Value: sp("1")}, {Partition: 0, Offset: 2, Key: sp("a"), Value: sp("2")}, {Partition: 1, Offset: 0, Key: sp("c"), Value: sp("1")}},
		"key lost":                           {{Partition: 0, Offset: 2, Key: sp("a"), Value: sp("2")}},
		"value changed":                      {{Partition: 0, Offset: 2, Key: sp("a"), Value: sp("2")}, {Partition: 1, Offset: 0, Key: sp("c"), Value: sp("x")}},
	}
	for name, m := range muts {
		if c := CompareCompacted(src, m, []string{"b"}); c.SameLatest && c.DeletedDeleted {
			t.Errorf("compacted verifier stayed green under %s", name)
		}
	}
}

func TestMutation_IdenticalDetectsEveryDefect(t *testing.T) {
	a := []Rec{{Partition: 0, Offset: 0, Key: sp("k"), Value: sp("v"), Headers: map[string]string{"seq": "0"}},
		{Partition: 0, Offset: 1, Key: sp("k"), Value: sp("w"), Headers: map[string]string{"seq": "1"}}}
	if ok, p := Identical(a, a); !ok {
		t.Fatalf("control: %v", p)
	}
	clone := func() []Rec {
		out := make([]Rec, len(a))
		copy(out, a)
		return out
	}
	muts := map[string]func() []Rec{
		"loss":                func() []Rec { return clone()[:1] },
		"duplicate":           func() []Rec { b := clone(); r := b[1]; r.Offset = 2; return append(b, r) },
		"wrong offset":        func() []Rec { b := clone(); b[1].Offset = 5; return b },
		"wrong value":         func() []Rec { b := clone(); b[0].Value = sp("x"); return b },
		"lost header":         func() []Rec { b := clone(); b[0].Headers = nil; return b },
		"null as empty":       func() []Rec { b := clone(); b[0].Key = sp(""); return b },
		"timestamp rewritten": func() []Rec { b := clone(); b[1].Timestamp = 99; return b },
	}
	for name, m := range muts {
		if ok, _ := Identical(a, m()); ok {
			t.Errorf("identity verifier stayed green under %s", name)
		}
	}
}
