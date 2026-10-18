package verify

// Mutation tests for the send-offsets, fencing, timeout, consume-transform-produce and
// transaction-version verifiers: each injects one defect into a known-good report and proves
// the observation changes.

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
)

func cloneJSON[T any](v T) T {
	b, _ := json.Marshal(v)
	var out T
	_ = json.Unmarshal(b, &out)
	return out
}

func mutate[T any](t *testing.T, label string, good T, judge func(T) Observation, muts map[string]func(*T)) {
	t.Helper()
	clean := judge(good)
	for name, m := range muts {
		f := cloneJSON(good)
		m(&f)
		if _, same := probe.Diff(clean, judge(f)); same {
			t.Errorf("%s verifier stayed green under %q", label, name)
		}
	}
}

func goodSendOffsets() SendOffsetsFacts {
	return SendOffsetsFacts{
		Outcomes: map[string]string{"txn1_send_offsets": "NONE", "txn1_end": "NONE", "txn2_send_offsets": "NONE",
			"txn2_end": "NONE", "txn3_send_offsets": "NONE", "txn3_end": "NONE"},
		AfterCommit1: "5", StableFetchWhileOpen: "UNSTABLE_OFFSET_COMMIT", AfterAbort: "5", AfterCommit2: "10",
		ReadCommitted: recs(Keys("o", 0, 10), append(span(0, 4), span(12, 16)...)),
	}
}

func TestMutation_SendOffsets(t *testing.T) {
	mutate(t, "send-offsets", goodSendOffsets(), SendOffsetsObservation, map[string]func(*SendOffsetsFacts){
		"abort kept the offset":  func(f *SendOffsetsFacts) { f.AfterAbort = "10" },
		"commit lost the offset": func(f *SendOffsetsFacts) { f.AfterCommit2 = "5" },
		"pending offset visible": func(f *SendOffsetsFacts) { f.StableFetchWhileOpen = "10" },
		"wrong error code":       func(f *SendOffsetsFacts) { f.Outcomes["txn2_send_offsets"] = "INVALID_GROUP_ID" },
		"output loss":            func(f *SendOffsetsFacts) { f.ReadCommitted = f.ReadCommitted[1:] },
		"output duplicate":       func(f *SendOffsetsFacts) { f.ReadCommitted = append(f.ReadCommitted, f.ReadCommitted[0]) },
	})
}

func goodFencing() FencingFacts {
	return FencingFacts{
		Outcomes: map[string]string{"b_init": "NONE", "a_send_after_fence": "PRODUCER_FENCED",
			"a_commit_after_fence": "PRODUCER_FENCED", "b_commit": "NONE"},
		FinalLSO: 7, FinalHWM: 7,
		ReadCommitted:   recs(Keys("b", 0, 2), span(4, 5)),
		ReadUncommitted: recs(append(Keys("z", 0, 3), Keys("b", 0, 2)...), append(span(0, 2), span(4, 5)...)),
	}
}

func TestMutation_Fencing(t *testing.T) {
	mutate(t, "fencing", goodFencing(), FencingObservation, map[string]func(*FencingFacts){
		"zombie commit accepted":   func(f *FencingFacts) { f.Outcomes["a_commit_after_fence"] = "NONE" },
		"zombie data committed":    func(f *FencingFacts) { f.ReadCommitted = append(recs(Keys("z", 0, 3), span(0, 2)), f.ReadCommitted...) },
		"other fencing error code": func(f *FencingFacts) { f.Outcomes["a_send_after_fence"] = "INVALID_PRODUCER_EPOCH" },
		"no abort marker":          func(f *FencingFacts) { f.FinalLSO, f.FinalHWM = 6, 6 },
		"lso stuck":                func(f *FencingFacts) { f.FinalLSO = 0 },
		"new producer data lost":   func(f *FencingFacts) { f.ReadCommitted = f.ReadCommitted[:1] },
	})
}

func goodTimeout() TimeoutFacts {
	return TimeoutFacts{
		Outcomes:        map[string]string{"late_commit": "INVALID_PRODUCER_EPOCH", "new_producer_commit": "NONE"},
		AbortedByBroker: true, FinalLSO: 6, FinalHWM: 6,
		ReadCommitted:   recs(Keys("u", 0, 1), []int64{4}),
		ReadUncommitted: recs(append(Keys("t", 0, 3), "u-00"), []int64{0, 1, 2, 4}),
	}
}

func TestMutation_Timeout(t *testing.T) {
	mutate(t, "timeout-abort", goodTimeout(), TimeoutObservation, map[string]func(*TimeoutFacts){
		"never aborted":         func(f *TimeoutFacts) { f.AbortedByBroker = false },
		"late commit succeeded": func(f *TimeoutFacts) { f.Outcomes["late_commit"] = "NONE" },
		"timed-out data visible": func(f *TimeoutFacts) {
			f.ReadCommitted = append(recs(Keys("t", 0, 3), span(0, 2)), f.ReadCommitted...)
		},
		"new producer refused": func(f *TimeoutFacts) { f.Outcomes["new_producer_commit"] = "CONCURRENT_TRANSACTIONS" },
		"wrong offset":         func(f *TimeoutFacts) { f.ReadCommitted[0].Offset = 3 },
	})
	if o, _ := analyzeTimeoutLimit([]byte(`{"outcomes":{"at_max_900000":"NONE","above_max_900001":"INVALID_TRANSACTION_TIMEOUT"}}`)); o["call_outcomes"] != "above_max_900001=INVALID_TRANSACTION_TIMEOUT,at_max_900000=NONE" {
		t.Errorf("timeout-limit: %v", o)
	}
}

func goodCPP() CPPFacts {
	f := CPPFacts{Commits: 8, Aborts: 4, FirstError: "NONE", GroupOffsets: map[string]int64{"0": 20, "1": 20, "2": 20}}
	off := map[int32]int64{}
	for n := 0; n < CPPInputs; n++ {
		p := int32(n % CPPPartitions)
		k := fmt.Sprintf("o-%02d", n)
		f.ReadCommitted = append(f.ReadCommitted, Rec{Partition: p, Offset: off[p], Key: k, Value: ValueFor(k)})
		f.ReadUncommitted = append(f.ReadUncommitted, Rec{Partition: p, Offset: off[p], Key: k, Value: ValueFor(k)})
		off[p] += 2
	}
	f.ReadUncommitted = append(f.ReadUncommitted, Rec{Partition: 0, Offset: 1, Key: "o-00", Value: ValueFor("o-00")})
	return f
}

func TestMutation_CPP(t *testing.T) {
	if o := CPPObservation(goodCPP()); o["output_missing"] != 0 || o["output_duplicates"] != 0 || o["output_out_of_order"] != 0 ||
		o["group_offsets"] != "0=20,1=20,2=20" || o["aborted_outputs_seen_uncommitted"] != true {
		t.Fatalf("control must be clean: %v", o)
	}
	mutate(t, "consume-transform-produce", goodCPP(), CPPObservation, map[string]func(*CPPFacts){
		"loss":             func(f *CPPFacts) { f.ReadCommitted = f.ReadCommitted[1:] },
		"duplicate":        func(f *CPPFacts) { f.ReadCommitted = append(f.ReadCommitted, f.ReadCommitted[7]) },
		"reorder":          func(f *CPPFacts) { f.ReadCommitted[0], f.ReadCommitted[3] = f.ReadCommitted[3], f.ReadCommitted[0] },
		"wrong partition":  func(f *CPPFacts) { f.ReadCommitted[1].Partition = 0 },
		"wrong offset":     func(f *CPPFacts) { f.GroupOffsets["1"] = 19 },
		"no abort":         func(f *CPPFacts) { f.Aborts = 0 },
		"aborts invisible": func(f *CPPFacts) { f.ReadUncommitted = f.ReadCommitted },
		"wrong error code": func(f *CPPFacts) { f.FirstError = "end:CONCURRENT_TRANSACTIONS" },
		"wrong content":    func(f *CPPFacts) { f.ReadCommitted[5].Value = "x" },
	})
}

func TestMutation_Version(t *testing.T) {
	good := VersionFacts{Finalized: 2, SupportedMax: 2, Epochs: []int{0, 1, 2},
		Outcomes:      map[string]string{"commit1": "NONE", "commit2": "NONE"},
		ReadCommitted: recs(Keys("v", 0, 4), []int64{0, 1, 3, 4})}
	if o := VersionObservation(good); o["epoch_bumps"] != "every transaction" {
		t.Fatalf("control: %v", o)
	}
	mutate(t, "transaction-version", good, VersionObservation, map[string]func(*VersionFacts){
		"feature not advertised": func(f *VersionFacts) { f.Finalized, f.SupportedMax = -1, -1 },
		"no epoch bump":          func(f *VersionFacts) { f.Epochs = []int{0, 0, 0} },
		"one bump":               func(f *VersionFacts) { f.Epochs = []int{0, 1, 1} },
		"wrong error code":       func(f *VersionFacts) { f.Outcomes["commit2"] = "INVALID_PRODUCER_EPOCH" },
		"loss":                   func(f *VersionFacts) { f.ReadCommitted = f.ReadCommitted[:3] },
	})
}
