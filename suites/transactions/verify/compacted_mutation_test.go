package verify

import "testing"

func goodCompacted() CompactedFacts {
	rc := []Rec{
		{Offset: 3, Key: "k-03", Value: "1:k-03"}, {Offset: 4, Key: "k-04", Value: "1:k-04"},
		{Offset: 12, Key: "k-00", Value: "3:k-00"}, {Offset: 13, Key: "k-01", Value: "3:k-01"},
		{Offset: 14, Key: "k-02", Value: "3:k-02"}, {Offset: 40, Key: "roll", Value: "9"},
	}
	return CompactedFacts{Settled: true, ReadCommitted: rc, ReadUncommitted: rc}
}

func TestMutation_Compacted(t *testing.T) {
	if o := CompactedObservation(goodCompacted()); o["read_committed_latest"] != "k-00=3,k-01=3,k-02=3,k-03=1,k-04=1" ||
		o["read_committed_one_record_per_key"] != true || o["aborted_records_left_in_log"] != 0 {
		t.Fatalf("control must be clean: %v", o)
	}
	mutate(t, "compacted-read", goodCompacted(), CompactedObservation, map[string]func(*CompactedFacts){
		"cleaner never ran":     func(f *CompactedFacts) { f.Settled = false },
		"aborted value visible": func(f *CompactedFacts) { f.ReadCommitted[0].Value = "aborted:k-03" },
		"aborted value kept as latest": func(f *CompactedFacts) {
			f.ReadCommitted = append(f.ReadCommitted, Rec{Offset: 41, Key: "k-00", Value: "aborted:k-00"})
		},
		"superseded value kept": func(f *CompactedFacts) {
			f.ReadCommitted = append([]Rec{{Offset: 0, Key: "k-00", Value: "1:k-00"}}, f.ReadCommitted...)
		},
		"latest value lost":   func(f *CompactedFacts) { f.ReadCommitted = f.ReadCommitted[1:] },
		"older value instead": func(f *CompactedFacts) { f.ReadCommitted[2].Value = "1:k-00" },
		"aborted data left in log": func(f *CompactedFacts) {
			f.ReadUncommitted = append(f.ReadUncommitted, Rec{Offset: 7, Key: "k-00", Value: "aborted:k-00"})
		},
		"offsets rewritten backwards": func(f *CompactedFacts) {
			f.ReadCommitted = append(f.ReadCommitted, Rec{Offset: 2, Key: "k-04", Value: "1:k-04"})
		},
	})
}
