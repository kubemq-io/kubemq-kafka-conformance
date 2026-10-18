package verify

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Scenario "consume-transform-produce" (SCENARIOS.md): exactly-once pipeline through a consumer
// group. Input topic (3 partitions) holds i-00..i-59, record n on partition n%3. One
// transactional consumer-producer in group G reads at most 5 records per transaction
// (read_committed), writes o-NN for each i-NN to the same partition of the output topic (3
// partitions), commits the input offsets in the same transaction, and aborts every third
// transaction on purpose, after its records were acknowledged, then resumes from the group's
// committed offsets. It stops when 60 outputs are committed.
//
// Exactly once: read_committed of the output delivers every o-NN exactly once, in input order
// per partition; the group's committed offsets equal the input end offsets (20 per partition);
// aborts really happened and their outputs are visible only to read_uncommitted. How many
// transactions it took depends on poll timing, so counts of transactions and output offsets are
// not compared.
const (
	CPPInputs     = 60
	CPPPartitions = 3
)

// CPPFacts is what a driver reports for scenario "consume-transform-produce".
type CPPFacts struct {
	Commits         int              `json:"commits"`
	Aborts          int              `json:"aborts"`
	FirstError      string           `json:"first_error"`   // first failed transactional call, "NONE" if none
	GroupOffsets    map[string]int64 `json:"group_offsets"` // partition -> committed offset
	ReadCommitted   []Rec            `json:"read_committed"`
	ReadUncommitted []Rec            `json:"read_uncommitted"`
}

func init() { Analyzers["consume-transform-produce"] = analyzeCPP }

func analyzeCPP(raw json.RawMessage) (Observation, error) {
	var f CPPFacts
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("consume-transform-produce facts: %w", err)
	}
	return CPPObservation(f), nil
}

// CPPWant is the output keys each partition must deliver, in order.
func CPPWant() map[int32][]string {
	w := map[int32][]string{}
	for n := 0; n < CPPInputs; n++ {
		p := int32(n % CPPPartitions)
		w[p] = append(w[p], fmt.Sprintf("o-%02d", n))
	}
	return w
}

// CPPObservation judges the facts. Pure, for the mutation tests.
func CPPObservation(f CPPFacts) Observation {
	want := CPPWant()
	byPart := map[int32][]Rec{}
	misplaced := 0
	for _, r := range f.ReadCommitted {
		byPart[r.Partition] = append(byPart[r.Partition], r)
	}
	var sum ReadSummary
	sum.ContentMatches = true
	for p, keys := range want {
		s := AnalyzeRead(keys, byPart[p])
		sum.Delivered += s.Delivered
		sum.Missing += s.Missing
		sum.Duplicates += s.Duplicates
		sum.OutOfOrder += s.OutOfOrder
		sum.Unexpected += s.Unexpected
		sum.ContentMatches = sum.ContentMatches && s.ContentMatches
	}
	for p := range byPart {
		if _, ok := want[p]; !ok {
			misplaced += len(byPart[p])
		}
	}
	committed := map[string]bool{}
	for _, r := range f.ReadCommitted {
		committed[fmt.Sprintf("%d/%d", r.Partition, r.Offset)] = true
	}
	abortedVisible := 0
	for _, r := range f.ReadUncommitted {
		if !committed[fmt.Sprintf("%d/%d", r.Partition, r.Offset)] {
			abortedVisible++
		}
	}
	return Observation{
		"output_delivered":                 sum.Delivered,
		"output_missing":                   sum.Missing,
		"output_duplicates":                sum.Duplicates,
		"output_out_of_order":              sum.OutOfOrder,
		"output_unexpected":                sum.Unexpected + misplaced,
		"output_content_matches":           sum.ContentMatches,
		"group_offsets":                    offsetsText(f.GroupOffsets),
		"aborts_happened":                  f.Aborts > 0,
		"aborted_outputs_seen_uncommitted": abortedVisible > 0,
		"first_error":                      f.FirstError,
	}
}

func offsetsText(m map[string]int64) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, ",")
}

// Scenario "transaction-version" (SCENARIOS.md): KIP-890 transaction version 2 status. The driver
// reports the finalized transaction.version level the broker advertises in ApiVersions (-1 when
// absent) and its supported range, and the producer epoch before and after each of two committed
// transactions. Under transaction version 2 the broker bumps the epoch on every commit or abort;
// under version 0 or 1 the epoch stays.
type VersionFacts struct {
	Finalized     int               `json:"finalized"` // finalized transaction.version max level, -1 if absent
	SupportedMax  int               `json:"supported_max"`
	Epochs        []int             `json:"epochs"` // before txn 1, after txn 1, after txn 2
	Outcomes      map[string]string `json:"outcomes"`
	ReadCommitted []Rec             `json:"read_committed"`
}

func init() { Analyzers["transaction-version"] = analyzeVersion }

func analyzeVersion(raw json.RawMessage) (Observation, error) {
	var f VersionFacts
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("transaction-version facts: %w", err)
	}
	return VersionObservation(f), nil
}

// VersionObservation judges the facts. Pure, for the mutation tests.
func VersionObservation(f VersionFacts) Observation {
	bumps := "none"
	if len(f.Epochs) == 3 {
		switch {
		case f.Epochs[1] > f.Epochs[0] && f.Epochs[2] > f.Epochs[1]:
			bumps = "every transaction"
		case f.Epochs[1] == f.Epochs[0] && f.Epochs[2] == f.Epochs[1]:
			bumps = "none"
		default:
			bumps = fmt.Sprintf("irregular %v", f.Epochs)
		}
	} else {
		bumps = fmt.Sprintf("not observed (%d epochs)", len(f.Epochs))
	}
	o := Observation{
		"transaction_version_finalized":     f.Finalized,
		"transaction_version_supported_max": f.SupportedMax,
		"epoch_bumps":                       bumps,
		"call_outcomes":                     Outcomes(f.Outcomes),
	}
	AnalyzeRead(Keys("v", 0, 4), f.ReadCommitted).Fields("read_committed", o)
	return o
}
