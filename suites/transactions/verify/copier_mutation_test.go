package verify

import (
	"strconv"
	"testing"
)

func TestMutation_Copier(t *testing.T) {
	good := CopierFacts{Seeded: 20, ProducedAcked: 20, CopierExits: []int{0, 0}}
	for i := 19; i >= 0; i-- { // committed output order across partitions is not specified
		good.Values = append(good.Values, strconv.Itoa(i))
	}
	if o := CopierObservation(good); o["output_missing"] != 0 || o["output_duplicates"] != 0 || o["output_unexpected"] != 0 {
		t.Fatalf("control must be clean: %v", o)
	}
	mutate(t, "upstream-copier", good, CopierObservation, map[string]func(*CopierFacts){
		"loss":             func(f *CopierFacts) { f.Values = f.Values[1:] },
		"duplicate":        func(f *CopierFacts) { f.Values = append(f.Values, "3") },
		"aborted value":    func(f *CopierFacts) { f.Values = append(f.Values, "20") },
		"garbage":          func(f *CopierFacts) { f.Values[0] = "07" },
		"seed not acked":   func(f *CopierFacts) { f.ProducedAcked = 19 },
		"copier failed":    func(f *CopierFacts) { f.CopierExits[1] = 1 },
		"copier never ran": func(f *CopierFacts) { f.CopierExits = nil },
	})
}
