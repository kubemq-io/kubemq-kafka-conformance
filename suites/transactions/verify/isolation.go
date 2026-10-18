package verify

import (
	"encoding/json"
	"fmt"
)

// Scenario "isolation" (SCENARIOS.md): one transactional producer on a fresh 1-partition topic.
//
//	txn 1: c-00..c-05, then, while it is still open, list the last stable offset (read_committed
//	       latest) and the high watermark (read_uncommitted latest); commit
//	txn 2: a-00..a-03, abort
//	txn 3: c-06..c-08, commit
//	then list both offsets again, read the partition from the start with read_committed and with
//	read_uncommitted, each by a fresh consumer with no group.
//
// On Apache Kafka every commit or abort marker takes one offset, so the data lands at 0-5, 7-10,
// 12-14 and both final offsets are 16. read_committed must deliver only the c-* records and
// read_uncommitted every data record; markers are never delivered.
const (
	IsolationCommitted1 = 6
	IsolationAborted    = 4
	IsolationCommitted2 = 3
)

// IsolationFacts is what a driver reports for scenario "isolation".
type IsolationFacts struct {
	OpenLSO         int64             `json:"open_lso"`
	OpenHWM         int64             `json:"open_hwm"`
	FinalLSO        int64             `json:"final_lso"`
	FinalHWM        int64             `json:"final_hwm"`
	Outcomes        map[string]string `json:"outcomes"` // commit1, abort, commit2
	ReadCommitted   []Rec             `json:"read_committed"`
	ReadUncommitted []Rec             `json:"read_uncommitted"`
}

func init() { Analyzers["isolation"] = analyzeIsolation }

func analyzeIsolation(raw json.RawMessage) (Observation, error) {
	var f IsolationFacts
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("isolation facts: %w", err)
	}
	return IsolationObservation(f), nil
}

// IsolationObservation judges the facts. Pure, for the mutation tests.
func IsolationObservation(f IsolationFacts) Observation {
	committed := append(Keys("c", 0, IsolationCommitted1), Keys("c", IsolationCommitted1, IsolationCommitted2)...)
	all := append(append(Keys("c", 0, IsolationCommitted1), Keys("a", 0, IsolationAborted)...), Keys("c", IsolationCommitted1, IsolationCommitted2)...)
	o := Observation{
		"open_last_stable_offset":  f.OpenLSO,
		"open_high_watermark":      f.OpenHWM,
		"open_lso_below_hwm":       f.OpenLSO < f.OpenHWM,
		"final_last_stable_offset": f.FinalLSO,
		"final_high_watermark":     f.FinalHWM,
		"call_outcomes":            Outcomes(f.Outcomes),
	}
	AnalyzeRead(committed, f.ReadCommitted).Fields("read_committed", o)
	AnalyzeRead(all, f.ReadUncommitted).Fields("read_uncommitted", o)
	return o
}
