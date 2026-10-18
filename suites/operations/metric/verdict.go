package metric

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Compare turns the two legs of a metric row into a verdict before gap binding:
//
//	VOID  the oracle leg does not meet the expectation (wrong mapping or workload, not KubeMQ)
//	PASS  the KubeMQ leg's observation equals the oracle's
//	DIFF  they differ; deviation is the field-by-field text a gap registration must carry,
//	      in the same format as the features track ("field: Kafka x, KubeMQ y; ...")
//
// kubemq is nil when the row maps no KubeMQ series; that is a difference, never a pass.
func Compare(oracle Result, kubemq *Result) (verdict, deviation, detail string) {
	if !oracle.Met {
		return "VOID", "", "oracle precondition failed: Kafka " + oracle.String()
	}
	if kubemq == nil {
		return "DIFF", "series_present: Kafka true, KubeMQ absent", "KubeMQ exposes no equivalent series"
	}
	dev := diff(oracle.Observation(), kubemq.Observation())
	if dev == "" {
		return "PASS", "", "Kafka " + oracle.String() + "; KubeMQ " + kubemq.String()
	}
	return "DIFF", dev, "Kafka " + oracle.String() + "; KubeMQ " + kubemq.String()
}

// Diff is the field-by-field deviation text between two observations ("" when identical), for
// the configuration-mapping probe rows.
func Diff(o, k map[string]any) string { return diff(o, k) }

func diff(o, k map[string]any) string {
	keys := map[string]bool{}
	for x := range o {
		keys[x] = true
	}
	for x := range k {
		keys[x] = true
	}
	sorted := make([]string, 0, len(keys))
	for x := range keys {
		sorted = append(sorted, x)
	}
	sort.Strings(sorted)
	var parts []string
	for _, x := range sorted {
		ov, ok1 := o[x]
		kv, ok2 := k[x]
		if a, b := enc(ov, ok1), enc(kv, ok2); a != b {
			parts = append(parts, fmt.Sprintf("%s: Kafka %s, KubeMQ %s", x, a, b))
		}
	}
	return strings.Join(parts, "; ")
}

func enc(v any, present bool) string {
	if !present {
		return "absent"
	}
	b, _ := json.Marshal(v)
	return string(b)
}
