package verify

import "testing"

func TestMutation_Idempotent(t *testing.T) {
	good := IdempotentFacts{
		Outcomes: map[string]string{"01_init": "NONE", "02_seq0": "NONE@0", "03_seq0_resent": "NONE@0",
			"04_seq5_gap": "OUT_OF_ORDER_SEQUENCE_NUMBER", "05_seq3": "NONE@3", "06_next_epoch_seq0": "NONE@5",
			"07_old_epoch_seq5": "INVALID_PRODUCER_EPOCH", "08_next_epoch_seq1": "NONE@6"},
		Read: recs(Keys("k", 0, 7), span(0, 6)),
	}
	mutate(t, "idempotent-sequences", good, IdempotentObservation, map[string]func(*IdempotentFacts){
		"retry appended twice":     func(f *IdempotentFacts) { f.Read = append(f.Read[:3:3], recs(Keys("k", 0, 7), span(3, 9))...) },
		"retry answered elsewhere": func(f *IdempotentFacts) { f.Outcomes["03_seq0_resent"] = "NONE@3" },
		"gap accepted":             func(f *IdempotentFacts) { f.Outcomes["04_seq5_gap"] = "NONE@3" },
		"wrong error code":         func(f *IdempotentFacts) { f.Outcomes["07_old_epoch_seq5"] = "PRODUCER_FENCED" },
		"loss":                     func(f *IdempotentFacts) { f.Read = f.Read[:6] },
		"refused record in log": func(f *IdempotentFacts) {
			f.Read = append(f.Read, Rec{Offset: 7, Key: "x-00", Value: ValueFor("x-00")})
		},
		"reorder": func(f *IdempotentFacts) { f.Read[1], f.Read[2] = f.Read[2], f.Read[1] },
	})
}

func TestMutation_Epoch(t *testing.T) {
	good := EpochFacts{
		Outcomes: map[string]string{"01_init": "NONE", "02_second_init": "NONE", "03_add_partitions_old_epoch": "PRODUCER_FENCED",
			"04_add_partitions_current_epoch": "NONE", "05_produce_old_epoch": "INVALID_PRODUCER_EPOCH", "06_produce_current_epoch": "NONE@0",
			"07_end_txn_old_epoch": "PRODUCER_FENCED", "08_end_txn_current_epoch": "NONE", "09_init_with_old_epoch": "PRODUCER_FENCED"},
		SameProducerID: true, EpochDelta: 1,
		ReadCommitted: recs(Keys("e", 0, 2), span(0, 1)),
	}
	mutate(t, "epoch-errors", good, EpochObservation, map[string]func(*EpochFacts){
		"fenced as epoch error": func(f *EpochFacts) { f.Outcomes["07_end_txn_old_epoch"] = "INVALID_PRODUCER_EPOCH" },
		"stale epoch accepted":  func(f *EpochFacts) { f.Outcomes["05_produce_old_epoch"] = "NONE@0" },
		"new producer id":       func(f *EpochFacts) { f.SameProducerID = false },
		"epoch not bumped":      func(f *EpochFacts) { f.EpochDelta = 0 },
		"commit lost":           func(f *EpochFacts) { f.ReadCommitted = nil },
	})
}
