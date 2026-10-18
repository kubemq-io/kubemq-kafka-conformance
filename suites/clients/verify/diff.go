package verify

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Diff compares the oracle's and KubeMQ's observations field by field. The text (sorted keys,
// JSON values) is also the exact deviation a gap registration must carry, as in the features
// track.
func Diff(oracle, kubemq Observation) (string, bool) {
	keys := map[string]bool{}
	for k := range oracle {
		keys[k] = true
	}
	for k := range kubemq {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	var parts []string
	for _, k := range sorted {
		o, ok1 := oracle[k]
		q, ok2 := kubemq[k]
		if oj, qj := enc(o, ok1), enc(q, ok2); oj != qj {
			parts = append(parts, fmt.Sprintf("%s: Kafka %s, KubeMQ %s", k, oj, qj))
		}
	}
	return strings.Join(parts, "; "), len(parts) == 0
}

func enc(v any, present bool) string {
	if !present {
		return "absent"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}
