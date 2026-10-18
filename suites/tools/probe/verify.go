package probe

// Verifiers: pure functions over what a tool printed, so their mutation tests (loss, duplicate,
// reorder, wrong offset, wrong content) need no broker (mutation_test.go).

import (
	"sort"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
)

// ConsoleSent is one line fed to kafka-console-producer.sh.
type ConsoleSent struct {
	Key     string
	Value   string // unique per record: it identifies the record
	Headers string // as the console consumer prints them: "k1:v1,k2:v2"
}

// ConsoleCheck summarizes a console-producer then console-consumer round trip.
type ConsoleCheck struct {
	Produced           int  `json:"produced"`
	Consumed           int  `json:"consumed"`
	Missing            int  `json:"missing"`
	Duplicates         int  `json:"duplicates"`
	Unexpected         int  `json:"unexpected"`
	OutOfOrder         int  `json:"out_of_order"`          // a key's records not in send order
	OffsetsFromZero    bool `json:"offsets_from_zero"`     // each partition's offsets 0,1,2,... in delivery order
	KeysSinglePartiton bool `json:"keys_single_partition"` // every key landed on one partition
	ContentMatches     bool `json:"content_matches"`       // key and headers of each record as sent
}

// AnalyzeConsole compares the records the console consumer printed with the lines sent.
func AnalyzeConsole(sent []ConsoleSent, got []kout.ConsoleRecord) ConsoleCheck {
	c := ConsoleCheck{Produced: len(sent), Consumed: len(got), OffsetsFromZero: true, KeysSinglePartiton: true, ContentMatches: true}
	index := map[string]int{}
	for i, s := range sent {
		index[s.Value] = i
	}
	seen := map[string]bool{}
	next := map[int]int64{}
	keyPart := map[string]int{}
	lastPos := map[string]int{}
	for _, g := range got {
		if g.Offset != next[g.Partition] {
			c.OffsetsFromZero = false
		}
		next[g.Partition] = g.Offset + 1
		i, ok := index[g.Value]
		if !ok {
			c.Unexpected++
			continue
		}
		if seen[g.Value] {
			c.Duplicates++
			continue
		}
		seen[g.Value] = true
		s := sent[i]
		if g.Key != s.Key || g.Headers != s.Headers {
			c.ContentMatches = false
		}
		if p, ok := keyPart[s.Key]; ok && p != g.Partition {
			c.KeysSinglePartiton = false
		}
		keyPart[s.Key] = g.Partition
		if last, ok := lastPos[s.Key]; ok && i < last {
			c.OutOfOrder++
		}
		lastPos[s.Key] = i
	}
	c.Missing = len(sent) - len(seen)
	return c
}

// VerifiableCheck summarizes kafka-verifiable-producer.sh then kafka-verifiable-consumer.sh.
type VerifiableCheck struct {
	ProducerAcked      int  `json:"producer_acked"`       // tool_data.acked
	SuccessEvents      int  `json:"success_events"`       // producer_send_success lines
	DistinctAckOffsets bool `json:"distinct_ack_offsets"` // no two acks share a partition and offset
	AckOffsetsDense    bool `json:"ack_offsets_dense"`    // per partition, acked offsets are 0..n-1
	Consumed           int  `json:"consumed"`             // sum of records_consumed counts
	ConsumedCoversAcks bool `json:"consumed_covers_acks"` // per partition, consumed offset range is exactly the acked one
	CommitsSucceeded   bool `json:"commits_succeeded"`    // every offsets_committed event has success=true
	FinalCommitAtEnd   bool `json:"final_commit_at_end"`  // last committed offset per partition = acked count
	// RecordsExactlyAcked: with the consumer's --verbose record events, every acked (partition,
	// offset) was consumed exactly once and nothing else was (a duplicate plus a loss in one
	// partition keeps count, min and max, so the range check alone would miss it).
	RecordsExactlyAcked bool `json:"records_exactly_acked"`
}

// AnalyzeVerifiable checks the two tools' JSON events against each other.
func AnalyzeVerifiable(prod, cons []kout.VerifiableEvent) VerifiableCheck {
	c := VerifiableCheck{DistinctAckOffsets: true, AckOffsetsDense: true, ConsumedCoversAcks: true, CommitsSucceeded: true, FinalCommitAtEnd: true}
	acked := map[int]map[int64]bool{}
	for _, e := range prod {
		switch e.Name {
		case "producer_send_success":
			c.SuccessEvents++
			if acked[e.Partition] == nil {
				acked[e.Partition] = map[int64]bool{}
			}
			if acked[e.Partition][e.Offset] {
				c.DistinctAckOffsets = false
			}
			acked[e.Partition][e.Offset] = true
		case "tool_data":
			c.ProducerAcked = e.Acked
		}
	}
	for _, offs := range acked {
		for o := int64(0); o < int64(len(offs)); o++ {
			if !offs[o] {
				c.AckOffsetsDense = false
			}
		}
	}
	seen := map[[2]int64]int{}
	records := 0
	for _, e := range cons {
		if e.Name == "record_data" {
			records++
			seen[[2]int64{int64(e.Partition), e.Offset}]++
		}
	}
	c.RecordsExactlyAcked = records > 0
	for k, n := range seen {
		if n != 1 || !acked[int(k[0])][k[1]] {
			c.RecordsExactlyAcked = false
		}
	}
	for p, offs := range acked {
		for o := range offs {
			if seen[[2]int64{int64(p), o}] == 0 {
				c.RecordsExactlyAcked = false
			}
		}
	}
	type rng struct {
		min, max int64
		count    int
	}
	got := map[int]*rng{}
	lastCommit := map[int]int64{}
	for _, e := range cons {
		switch e.Name {
		case "records_consumed":
			c.Consumed += e.Count
			for _, p := range e.Partitions {
				r := got[p.Partition]
				if r == nil {
					r = &rng{min: p.MinOffset, max: p.MaxOffset}
					got[p.Partition] = r
				}
				if p.MinOffset < r.min {
					r.min = p.MinOffset
				}
				if p.MaxOffset > r.max {
					r.max = p.MaxOffset
				}
				r.count += p.Count
			}
		case "offsets_committed":
			if e.Success == nil || !*e.Success {
				c.CommitsSucceeded = false
			}
			for _, o := range e.Offsets {
				lastCommit[o.Partition] = o.Offset
			}
		}
	}
	for p, offs := range acked {
		r := got[p]
		n := int64(len(offs))
		if r == nil || r.min != 0 || r.max != n-1 || int64(r.count) != n {
			c.ConsumedCoversAcks = false
		}
		if lastCommit[p] != n {
			c.FinalCommitAtEnd = false
		}
	}
	for p := range got {
		if acked[p] == nil {
			c.ConsumedCoversAcks = false
		}
	}
	return c
}

// sortedKeys returns the keys of a string set, sorted.
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sumOffsets sums a partition-to-offset map.
func sumOffsets(m map[int]int64) int64 {
	var s int64
	for _, v := range m {
		s += v
	}
	return s
}

// LineCheck compares the lines a pipeline delivered with the lines fed into it (Connect's file
// sink against its file source; MirrorMaker 2's target against its source). Ported from the
// kubemq-server ecosystem legs' eco_verify_exact and eco_verify_no_loss.
type LineCheck struct {
	Expected        int  `json:"expected"`
	Delivered       int  `json:"delivered"`
	Missing         int  `json:"missing"`
	Unexpected      int  `json:"unexpected"`
	Duplicates      int  `json:"duplicates"`
	OrderViolations int  `json:"order_violations"` // first occurrences not in the fed order
	Identical       bool `json:"identical"`        // byte for byte: same lines, same order, nothing extra
}

// AnalyzeLines checks delivered against expected.
func AnalyzeLines(expected, delivered []string) LineCheck {
	c := LineCheck{Expected: len(expected), Delivered: len(delivered)}
	want := map[string]int{}
	for i, l := range expected {
		if _, ok := want[l]; !ok {
			want[l] = i
		}
	}
	seen := map[string]bool{}
	last := -1
	for _, l := range delivered {
		i, ok := want[l]
		if !ok {
			c.Unexpected++
			continue
		}
		if seen[l] {
			c.Duplicates++
			continue
		}
		seen[l] = true
		if i < last {
			c.OrderViolations++
		}
		last = i
	}
	for l := range want {
		if !seen[l] {
			c.Missing++
		}
	}
	c.Identical = len(expected) == len(delivered)
	for i := range expected {
		if !c.Identical || expected[i] != delivered[i] {
			c.Identical = false
			break
		}
	}
	return c
}

// SortedEqual reports whether two line lists hold the same multiset (any order).
func SortedEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	ca := map[string]int{}
	for _, l := range a {
		ca[l]++
	}
	for _, l := range b {
		ca[l]--
		if ca[l] < 0 {
			return false
		}
	}
	return true
}
