package probe

import (
	"bytes"
	"fmt"
)

// Sent is one produced record as the probe intended it.
type Sent struct {
	Partition int32
	Key       []byte
	Value     []byte
	Headers   [][2][]byte
}

// Got is one consumed record as the broker returned it.
type Got struct {
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   [][2][]byte
}

// RoundTrip summarizes a produce-then-consume of a fresh topic. It is pure so its mutation
// tests (loss, duplicate, reorder, wrong offset, wrong content) need no broker.
type RoundTrip struct {
	Produced        int  `json:"produced"`
	Consumed        int  `json:"consumed"`
	Missing         int  `json:"missing"`
	Duplicates      int  `json:"duplicates"`
	OutOfOrder      int  `json:"out_of_order"`      // records later than a successor in the same partition
	OffsetsFromZero bool `json:"offsets_from_zero"` // each partition's offsets are 0,1,2,... with no gap
	ContentMatches  bool `json:"content_matches"`   // key, value and headers of each record as sent
}

// AnalyzeRoundTrip matches consumed records to sent ones by (partition, position within the
// partition in send order).
func AnalyzeRoundTrip(sent []Sent, got []Got) RoundTrip {
	rt := RoundTrip{Produced: len(sent), Consumed: len(got), OffsetsFromZero: true, ContentMatches: true}
	perPart := map[int32][]Sent{}
	for _, s := range sent {
		perPart[s.Partition] = append(perPart[s.Partition], s)
	}
	seen := map[string]int{}
	next := map[int32]int64{}
	lastIdx := map[int32]int{}
	for _, g := range got {
		if g.Offset != next[g.Partition] {
			rt.OffsetsFromZero = false
		}
		next[g.Partition] = g.Offset + 1
		want := perPart[g.Partition]
		idx := indexOf(want, g)
		if idx < 0 {
			rt.ContentMatches = false
			continue
		}
		k := fmt.Sprintf("%d/%d", g.Partition, idx)
		seen[k]++
		if seen[k] > 1 {
			rt.Duplicates++
			continue
		}
		if last, ok := lastIdx[g.Partition]; ok && idx < last {
			rt.OutOfOrder++
		}
		lastIdx[g.Partition] = idx
		if int64(idx) != g.Offset {
			rt.OffsetsFromZero = false
		}
	}
	for p, list := range perPart {
		for i := range list {
			if seen[fmt.Sprintf("%d/%d", p, i)] == 0 {
				rt.Missing++
			}
		}
	}
	return rt
}

// indexOf finds the sent record with the same key, value and headers. Sent records within one
// partition are unique by value in every probe, so the first match is the match.
func indexOf(list []Sent, g Got) int {
	for i, s := range list {
		if bytes.Equal(s.Key, g.Key) && bytes.Equal(s.Value, g.Value) && sameHeaders(s.Headers, g.Headers) {
			return i
		}
	}
	return -1
}

func sameHeaders(a, b [][2][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i][0], b[i][0]) || !bytes.Equal(a[i][1], b[i][1]) || (a[i][1] == nil) != (b[i][1] == nil) {
			return false
		}
	}
	return true
}

// Observation turns the summary into observation fields.
func (rt RoundTrip) Observation() Observation {
	return Observation{
		"produced": rt.Produced, "consumed": rt.Consumed, "missing": rt.Missing,
		"duplicates": rt.Duplicates, "out_of_order": rt.OutOfOrder,
		"offsets_from_zero": rt.OffsetsFromZero, "content_matches": rt.ContentMatches,
	}
}
