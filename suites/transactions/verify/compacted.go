package verify

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Scenario "compacted-read" (SCENARIOS.md): a compacted topic written only by transactions, read
// after the log cleaner rewrote it. One partition, cleanup.policy=compact, segment.ms=1000,
// min.cleanable.dirty.ratio=0.01, delete.retention.ms=5000, min.compaction.lag.ms=0.
//
//	txn1 commit: k-00..k-04 = "1:<key>"
//	txn2 abort:  k-00..k-04 = "aborted:<key>"
//	txn3 commit: k-00..k-02 = "3:<key>"
//
// Then, every 4 s for up to 120 s, a committed transaction appends key "roll" (so segments roll and
// become cleanable) and the partition is read with read_committed and read_uncommitted. The driver
// stops when the cleaner has visibly run: every k-* key has exactly one record under
// read_committed and no aborted record is left in the log (Kafka's cleaner discards aborted
// transactional data). It reports whether that happened (`settled`) and the last two reads.
//
// What must hold after the rewrite: read_committed shows k-00..k-02 = "3:", k-03, k-04 = "1:", and
// never an aborted value; the rewritten batches still carry correct transaction markers.
type CompactedFacts struct {
	Settled         bool  `json:"settled"`
	ReadCommitted   []Rec `json:"read_committed"`
	ReadUncommitted []Rec `json:"read_uncommitted"`
}

func init() { Analyzers["compacted-read"] = analyzeCompacted }

func analyzeCompacted(raw json.RawMessage) (Observation, error) {
	var f CompactedFacts
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("compacted-read facts: %w", err)
	}
	return CompactedObservation(f), nil
}

// CompactedObservation judges the facts. Pure, for the mutation tests. The "roll" key is the
// driver's own cadence and is ignored.
func CompactedObservation(f CompactedFacts) Observation {
	latest := map[string]string{}
	count := map[string]int{}
	abortedCommitted, backwards := 0, 0
	lastOff := map[string]int64{}
	for _, r := range f.ReadCommitted {
		if r.Key == "roll" {
			continue
		}
		if strings.HasPrefix(r.Value, "aborted:") {
			abortedCommitted++
		}
		if o, ok := lastOff[r.Key]; ok && r.Offset <= o {
			backwards++
		}
		lastOff[r.Key] = r.Offset
		count[r.Key]++
		latest[r.Key] = strings.SplitN(r.Value, ":", 2)[0]
	}
	keys := make([]string, 0, len(latest))
	for k := range latest {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	onePerKey := true
	for _, k := range keys {
		parts = append(parts, k+"="+latest[k])
		if count[k] != 1 {
			onePerKey = false
		}
	}
	abortedInLog := 0
	for _, r := range f.ReadUncommitted {
		if strings.HasPrefix(r.Value, "aborted:") {
			abortedInLog++
		}
	}
	return Observation{
		"cleaner_ran_within_120s":           f.Settled,
		"read_committed_latest":             strings.Join(parts, ","),
		"read_committed_one_record_per_key": onePerKey,
		"read_committed_aborted_visible":    abortedCommitted,
		"read_committed_offsets_backwards":  backwards,
		"aborted_records_left_in_log":       abortedInLog,
	}
}
