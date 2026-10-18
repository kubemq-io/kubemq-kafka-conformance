package verify

import (
	"encoding/json"
	"fmt"
)

// Scenario "fencing" (SCENARIOS.md): zombie fencing. Producer A (transactional id T) begins and
// sends z-00..z-02 (acknowledged). Producer B starts with the same T: its InitProducerId bumps
// the epoch and aborts A's open transaction. A then tries to send z-03 and to commit; both must
// fail with a fencing error. B sends b-00..b-01 and commits. read_committed delivers only b-*;
// read_uncommitted delivers z-00..z-02 and b-*; the final offsets are equal.
type FencingFacts struct {
	Outcomes        map[string]string `json:"outcomes"` // b_init, a_send_after_fence, a_commit_after_fence, b_commit
	FinalLSO        int64             `json:"final_lso"`
	FinalHWM        int64             `json:"final_hwm"`
	ReadCommitted   []Rec             `json:"read_committed"`
	ReadUncommitted []Rec             `json:"read_uncommitted"`
}

func init() { Analyzers["fencing"] = analyzeFencing }

func analyzeFencing(raw json.RawMessage) (Observation, error) {
	var f FencingFacts
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("fencing facts: %w", err)
	}
	return FencingObservation(f), nil
}

// FencingObservation judges the facts. Pure, for the mutation tests.
func FencingObservation(f FencingFacts) Observation {
	o := Observation{
		"call_outcomes":            Outcomes(f.Outcomes),
		"final_last_stable_offset": f.FinalLSO,
		"final_high_watermark":     f.FinalHWM,
	}
	AnalyzeRead(Keys("b", 0, 2), f.ReadCommitted).Fields("read_committed", o)
	AnalyzeRead(append(Keys("z", 0, 3), Keys("b", 0, 2)...), f.ReadUncommitted).Fields("read_uncommitted", o)
	return o
}

// Scenario "timeout-abort" (SCENARIOS.md): a transaction left open past its
// transaction.timeout.ms (3000) is aborted by the broker. The producer begins, sends t-00..t-02
// (acknowledged) and goes silent. The driver lists the last stable offset and high watermark
// every second for up to 45 s and reports whether they became equal (the broker wrote the abort
// marker). The producer then tries to commit (it must fail: its epoch was bumped), and a new
// producer with the same transactional id commits u-00. read_committed delivers only u-00.
type TimeoutFacts struct {
	Outcomes        map[string]string `json:"outcomes"` // late_commit, new_producer_commit
	AbortedByBroker bool              `json:"aborted_by_broker"`
	FinalLSO        int64             `json:"final_lso"`
	FinalHWM        int64             `json:"final_hwm"`
	ReadCommitted   []Rec             `json:"read_committed"`
	ReadUncommitted []Rec             `json:"read_uncommitted"`
}

func init() { Analyzers["timeout-abort"] = analyzeTimeout }

func analyzeTimeout(raw json.RawMessage) (Observation, error) {
	var f TimeoutFacts
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("timeout-abort facts: %w", err)
	}
	return TimeoutObservation(f), nil
}

// TimeoutObservation judges the facts. Pure, for the mutation tests.
func TimeoutObservation(f TimeoutFacts) Observation {
	o := Observation{
		"call_outcomes":            Outcomes(f.Outcomes),
		"aborted_by_broker_in_45s": f.AbortedByBroker,
		"final_last_stable_offset": f.FinalLSO,
		"final_high_watermark":     f.FinalHWM,
	}
	AnalyzeRead(Keys("u", 0, 1), f.ReadCommitted).Fields("read_committed", o)
	AnalyzeRead(append(Keys("t", 0, 3), Keys("u", 0, 1)...), f.ReadUncommitted).Fields("read_uncommitted", o)
	return o
}

// Scenario "timeout-limit" (SCENARIOS.md): InitProducerId with a transaction timeout above the
// broker's transaction.max.timeout.ms (Kafka default 900000) is refused with
// INVALID_TRANSACTION_TIMEOUT; exactly the maximum is accepted.
type TimeoutLimitFacts struct {
	Outcomes map[string]string `json:"outcomes"` // at_max_900000, above_max_900001
}

func init() { Analyzers["timeout-limit"] = analyzeTimeoutLimit }

func analyzeTimeoutLimit(raw json.RawMessage) (Observation, error) {
	var f TimeoutLimitFacts
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("timeout-limit facts: %w", err)
	}
	return Observation{"call_outcomes": Outcomes(f.Outcomes)}, nil
}
