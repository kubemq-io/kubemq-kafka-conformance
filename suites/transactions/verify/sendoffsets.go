package verify

import (
	"encoding/json"
	"fmt"
)

// Scenario "send-offsets" (SCENARIOS.md): consumer-group offsets committed inside a transaction,
// with no group membership. Input topic holds i-00..i-09 (1 partition).
//
//	txn1: o-00..o-04 to the output topic, offset 5 for the input partition, commit
//	txn2: o-05..o-09, offset 10, and while it is open an OffsetFetch with requireStable; abort
//	txn3: o-05..o-09 again, offset 10, commit
//
// The group's committed offset (OffsetFetch without requireStable) is read after each
// transaction. On Apache Kafka: 5, 5 (the abort discards the pending offset), 10; the stable
// fetch while txn2 is open answers UNSTABLE_OFFSET_COMMIT; read_committed delivers o-00..o-09 once.
const SendOffsetsInputs = 10

// SendOffsetsFacts is what a driver reports for scenario "send-offsets".
type SendOffsetsFacts struct {
	Outcomes             map[string]string `json:"outcomes"` // txnN_send_offsets, txnN_end
	AfterCommit1         string            `json:"after_commit1"`
	StableFetchWhileOpen string            `json:"stable_fetch_while_open"`
	AfterAbort           string            `json:"after_abort"`
	AfterCommit2         string            `json:"after_commit2"`
	ReadCommitted        []Rec             `json:"read_committed"`
}

func init() { Analyzers["send-offsets"] = analyzeSendOffsets }

func analyzeSendOffsets(raw json.RawMessage) (Observation, error) {
	var f SendOffsetsFacts
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("send-offsets facts: %w", err)
	}
	return SendOffsetsObservation(f), nil
}

// SendOffsetsObservation judges the facts. Pure, for the mutation tests.
func SendOffsetsObservation(f SendOffsetsFacts) Observation {
	o := Observation{
		"call_outcomes":                  Outcomes(f.Outcomes),
		"group_offset_after_commit1":     f.AfterCommit1,
		"group_offset_stable_while_open": f.StableFetchWhileOpen,
		"group_offset_after_abort":       f.AfterAbort,
		"group_offset_after_commit2":     f.AfterCommit2,
	}
	AnalyzeRead(Keys("o", 0, SendOffsetsInputs), f.ReadCommitted).Fields("output_read_committed", o)
	return o
}
