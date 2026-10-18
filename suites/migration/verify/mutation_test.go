package verify

// Verifier mutation tests for the migration track (docs/methodology.md): each injects one defect
// into a correct cutover and proves the verifier reports it. Loss = a skipped record, duplicate =
// an offset delivered twice, reorder = offsets out of order, wrong offset = a consumer that
// started somewhere other than its translated commit, and translation ahead of the source.

import "testing"

func seq(from, to int64) []int64 {
	var out []int64
	for o := from; o < to; o++ {
		out = append(out, o)
	}
	return out
}

// good: three partitions of 100 records; the source group committed 60, 100 and 40; MirrorMaker 2
// translated 60 exactly, 90 (behind: 10 re-reads) and 40; the consumer resumed from there.
func good() map[int32]Partition {
	return map[int32]Partition{
		0: {SourceCommitted: 60, Translated: 60, End: 100, Delivered: seq(60, 100)},
		1: {SourceCommitted: 100, Translated: 90, End: 100, Delivered: seq(90, 100)},
		2: {SourceCommitted: 40, Translated: 40, End: 100, Delivered: seq(40, 100)},
	}
}

func TestMutation_ResumeDetectsEveryDefect(t *testing.T) {
	clean := CheckResume(good())
	if !clean.OK() || clean.Rereads != 10 {
		t.Fatalf("control: correct cutover must pass with 10 re-reads: %+v", clean)
	}
	muts := map[string]func(m map[int32]Partition){
		"loss (record skipped)": func(m map[int32]Partition) {
			p := m[0]
			p.Delivered = append(seq(60, 70), seq(71, 100)...)
			m[0] = p
		},
		"duplicate": func(m map[int32]Partition) { p := m[2]; p.Delivered = append(p.Delivered, 55); m[2] = p },
		"reorder": func(m map[int32]Partition) {
			p := m[0]
			d := seq(60, 100)
			d[3], d[4] = d[4], d[3]
			p.Delivered = d
			m[0] = p
		},
		"wrong offset (started from the beginning)": func(m map[int32]Partition) { p := m[2]; p.Delivered = seq(0, 100); m[2] = p },
		"wrong offset (started at the log end)":     func(m map[int32]Partition) { p := m[0]; p.Delivered = nil; m[0] = p },
		"translated ahead of the source": func(m map[int32]Partition) {
			m[0] = Partition{SourceCommitted: 60, Translated: 65, End: 100, Delivered: seq(65, 100)}
		},
		"untranslated partition not resumed from the start": func(m map[int32]Partition) { p := m[1]; p.Translated = -1; p.Delivered = nil; m[1] = p },
		"untranslated partition resumed at the log end":     func(m map[int32]Partition) { p := m[1]; p.Translated = -1; p.Delivered = seq(90, 100); m[1] = p },
		"tail not delivered":                                func(m map[int32]Partition) { p := m[1]; p.Delivered = seq(90, 99); m[1] = p },
		"no partition translated, everything re-read": func(m map[int32]Partition) {
			for p, e := range m {
				e.Translated, e.Delivered = -1, seq(0, e.End)
				m[p] = e
			}
		},
	}
	for name, mut := range muts {
		m := good()
		mut(m)
		got := CheckResume(m)
		if got.OK() {
			t.Errorf("verifier stayed green under %s: %+v", name, got)
		}
		if _, same := Diff(clean.Observation(), got.Observation()); same {
			t.Errorf("%s: observation identical to a correct cutover", name)
		}
		if len(got.Problems) == 0 {
			t.Errorf("%s: no problem explained", name)
		}
	}
}

// A partition MirrorMaker 2 did not translate, resumed from the log start, is a correct cutover
// (it re-reads, never skips).
func TestUntranslatedPartitionFromStartIsCorrect(t *testing.T) {
	m := good()
	m[1] = Partition{SourceCommitted: 100, Translated: -1, End: 100, Delivered: seq(0, 100)}
	r := CheckResume(m)
	if !r.OK() || r.AllTranslated || r.Untranslated != 1 || r.Rereads != 100 {
		t.Fatalf("%+v", r)
	}
}

func TestDiffFormat(t *testing.T) {
	d, same := Diff(map[string]any{"a": true, "b": 1}, map[string]any{"a": false, "c": "x"})
	want := `a: Kafka true, KubeMQ false; b: Kafka 1, KubeMQ absent; c: Kafka absent, KubeMQ "x"`
	if same || d != want {
		t.Fatalf("got %q", d)
	}
}
