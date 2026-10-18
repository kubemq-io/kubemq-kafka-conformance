package verify

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Scenario "upstream-copier" (SCENARIOS.md): Apache Kafka's transactions system test path with
// Kafka's own tools. kafka-verifiable-producer seeds `seeded` values "0".."seeded-1"; one
// TransactionalMessageCopier per input partition copies them with random aborts; a read_committed
// console consumer reads the output. Exactly once, as the upstream test asserts: every seeded
// value appears exactly once in the committed output, and nothing else does. The copiers' random
// aborts make transaction counts run-dependent, so only the outcome is compared.
type CopierFacts struct {
	Seeded        int      `json:"seeded"`
	ProducedAcked int      `json:"produced_acked"`
	CopierExits   []int    `json:"copier_exits"`
	Values        []string `json:"read_committed_values"`
}

func init() { Analyzers["upstream-copier"] = analyzeCopier }

func analyzeCopier(raw json.RawMessage) (Observation, error) {
	var f CopierFacts
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("upstream-copier facts: %w", err)
	}
	return CopierObservation(f), nil
}

// CopierObservation judges the facts. Pure, for the mutation tests.
func CopierObservation(f CopierFacts) Observation {
	seen := map[string]int{}
	unexpected, dups := 0, 0
	for _, v := range f.Values {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n >= f.Seeded || strconv.Itoa(n) != v {
			unexpected++
			continue
		}
		seen[v]++
		if seen[v] > 1 {
			dups++
		}
	}
	missing := 0
	for i := 0; i < f.Seeded; i++ {
		if seen[strconv.Itoa(i)] == 0 {
			missing++
		}
	}
	exits := "none"
	if len(f.CopierExits) > 0 {
		b, _ := json.Marshal(f.CopierExits)
		exits = string(b)
	}
	return Observation{
		"seeded":                  f.Seeded,
		"seed_acknowledged":       f.ProducedAcked,
		"copier_exit_codes":       exits,
		"output_committed_values": len(f.Values),
		"output_missing":          missing,
		"output_duplicates":       dups,
		"output_unexpected":       unexpected,
	}
}
