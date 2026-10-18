package verify

import (
	"encoding/json"
	"fmt"
)

// Scenario "idempotent-sequences" (SCENARIOS.md), raw wire: an idempotent producer id (no
// transactional id) sends hand-built batches to partition 0 of a fresh topic, acks=all:
//
//	01 InitProducerId
//	02 epoch e, sequence 0: k-00..k-02
//	03 the same batch again (a retry the broker must deduplicate, answering the first offset)
//	04 epoch e, sequence 5 (3 and 4 missing): x-00, must be refused as out of order
//	05 epoch e, sequence 3: k-03..k-04
//	06 epoch e+1, sequence 0: k-05 (a client-side epoch bump restarts the sequence)
//	07 epoch e, sequence 5: x-01, a stale epoch, must be refused
//	08 epoch e+1, sequence 1: k-06
//
// Then the partition is read: k-00..k-06 exactly once at offsets 0-6, no x-*. Each outcome is
// the error name, and for success "NONE@<base offset>".
type IdempotentFacts struct {
	Outcomes map[string]string `json:"outcomes"`
	Read     []Rec             `json:"read"`
}

func init() { Analyzers["idempotent-sequences"] = analyzeIdempotent }

func analyzeIdempotent(raw json.RawMessage) (Observation, error) {
	var f IdempotentFacts
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("idempotent-sequences facts: %w", err)
	}
	return IdempotentObservation(f), nil
}

// IdempotentObservation judges the facts. Pure, for the mutation tests.
func IdempotentObservation(f IdempotentFacts) Observation {
	o := Observation{"call_outcomes": Outcomes(f.Outcomes)}
	AnalyzeRead(Keys("k", 0, 7), f.Read).Fields("log", o)
	return o
}

// Scenario "epoch-errors" (SCENARIOS.md), raw wire: which error a fenced (stale-epoch)
// transactional producer gets on each call. One transactional id T:
//
//	01 InitProducerId(T) -> id P, epoch e1
//	02 InitProducerId(T) again -> same id P, epoch e2 = e1+1 (the first instance is fenced)
//	03 AddPartitionsToTxn with e1, 04 with e2
//	05 transactional Produce with e1, 06 with e2 (e-00, e-01)
//	07 EndTxn(commit) with e1, 08 with e2
//	09 InitProducerId(T, P, e1): a recovering client presenting its stale epoch (KIP-360)
//
// On Kafka 4.x the stale epoch is answered PRODUCER_FENCED by the transaction coordinator and
// INVALID_PRODUCER_EPOCH by the partition leader. read_committed then delivers e-00, e-01.
type EpochFacts struct {
	Outcomes       map[string]string `json:"outcomes"`
	SameProducerID bool              `json:"same_producer_id"`
	EpochDelta     int               `json:"epoch_delta"`
	ReadCommitted  []Rec             `json:"read_committed"`
}

func init() { Analyzers["epoch-errors"] = analyzeEpoch }

func analyzeEpoch(raw json.RawMessage) (Observation, error) {
	var f EpochFacts
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("epoch-errors facts: %w", err)
	}
	return EpochObservation(f), nil
}

// EpochObservation judges the facts. Pure, for the mutation tests.
func EpochObservation(f EpochFacts) Observation {
	o := Observation{
		"call_outcomes":                Outcomes(f.Outcomes),
		"second_init_same_producer_id": f.SameProducerID,
		"second_init_epoch_delta":      f.EpochDelta,
	}
	AnalyzeRead(Keys("e", 0, 2), f.ReadCommitted).Fields("read_committed", o)
	return o
}
