// Package verify turns what a transaction driver reported (its facts) into an observation the
// track compares between the Kafka oracle and KubeMQ.
//
// Every client driver (franz-go, Java kafka-clients, librdkafka) runs the same scenario and
// reports raw facts only: the records each reader received with their offsets, the offsets the
// broker listed, and the outcome of each transactional call as a Kafka error name. All judging
// happens here, once, in pure functions with mutation tests, so a driver cannot grade itself and
// every client is held to the same rules.
package verify

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
)

// Observation is the features track's observation type, so the same field-by-field diff and gap
// binding apply.
type Observation = probe.Observation

// Report is what a driver prints as the last line of its standard output.
type Report struct {
	Client   string          `json:"client"`   // library and version actually loaded
	Scenario string          `json:"scenario"` // scenario name it ran
	Facts    json.RawMessage `json:"facts"`
}

// Rec is one record a reader received.
type Rec struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// Analyzer turns one scenario's facts into an observation. An error means the facts are
// malformed (a driver defect), which the track treats like a leg that could not complete.
type Analyzer func(facts json.RawMessage) (Observation, error)

// Analyzers maps scenario names (matrix/transactions.yaml) to their analysis.
var Analyzers = map[string]Analyzer{}

// Analyze runs the analyzer of the scenario the report names, after checking it is the scenario
// that was asked for.
func Analyze(want string, r Report) (Observation, error) {
	if r.Scenario != want {
		return nil, fmt.Errorf("driver ran scenario %q, the row asked for %q", r.Scenario, want)
	}
	a := Analyzers[want]
	if a == nil {
		return nil, fmt.Errorf("no analyzer for scenario %q", want)
	}
	return a(r.Facts)
}

// ValueFor is the value every driver writes for a key, so content is checkable.
func ValueFor(key string) string { return "v-" + key }

// ReadSummary judges one reader's records against the keys it must deliver, in order, on one
// partition.
type ReadSummary struct {
	Delivered      int    // records received
	Missing        int    // expected keys never received
	Duplicates     int    // expected keys received more than once
	OutOfOrder     int    // records received before a key that precedes them
	Unexpected     int    // records whose key is not expected (for read_committed: aborted data)
	ContentMatches bool   // every expected record carries ValueFor(key)
	Offsets        string // the offsets received, as ranges ("0-5,12-14")
}

// AnalyzeRead compares got with the expected keys in send order.
func AnalyzeRead(want []string, got []Rec) ReadSummary {
	s := ReadSummary{Delivered: len(got), ContentMatches: true}
	pos := map[string]int{}
	for i, k := range want {
		pos[k] = i
	}
	seen := map[string]int{}
	last := -1
	var offs []int64
	for _, r := range got {
		offs = append(offs, r.Offset)
		i, ok := pos[r.Key]
		if !ok {
			s.Unexpected++
			continue
		}
		seen[r.Key]++
		if seen[r.Key] > 1 {
			s.Duplicates++
			continue
		}
		if r.Value != ValueFor(r.Key) {
			s.ContentMatches = false
		}
		if i < last {
			s.OutOfOrder++
		}
		if i > last {
			last = i
		}
	}
	for _, k := range want {
		if seen[k] == 0 {
			s.Missing++
		}
	}
	s.Offsets = Ranges(offs)
	return s
}

// Fields renders a summary as observation fields under a prefix.
func (s ReadSummary) Fields(prefix string, o Observation) {
	o[prefix+"_delivered"] = s.Delivered
	o[prefix+"_missing"] = s.Missing
	o[prefix+"_duplicates"] = s.Duplicates
	o[prefix+"_out_of_order"] = s.OutOfOrder
	o[prefix+"_unexpected"] = s.Unexpected
	o[prefix+"_content_matches"] = s.ContentMatches
	o[prefix+"_offsets"] = s.Offsets
}

// Ranges renders offsets in received order as compact ranges: 0,1,2,5 -> "0-2,5". A repeated or
// backwards offset starts a new range, so duplicates and reordering stay visible.
func Ranges(offs []int64) string {
	if len(offs) == 0 {
		return ""
	}
	var parts []string
	start, prev := offs[0], offs[0]
	flush := func() {
		if start == prev {
			parts = append(parts, strconv.FormatInt(start, 10))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", start, prev))
		}
	}
	for _, o := range offs[1:] {
		if o == prev+1 {
			prev = o
			continue
		}
		flush()
		start, prev = o, o
	}
	flush()
	return strings.Join(parts, ",")
}

// Outcomes renders named call outcomes (Kafka error names, "NONE" for success) in name order.
func Outcomes(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ",")
}

// Keys returns prefix-00 .. prefix-(n-1), starting at from.
func Keys(prefix string, from, n int) []string {
	out := make([]string, 0, n)
	for i := from; i < from+n; i++ {
		out = append(out, fmt.Sprintf("%s-%02d", prefix, i))
	}
	return out
}
