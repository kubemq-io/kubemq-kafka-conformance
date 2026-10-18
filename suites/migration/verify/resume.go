// Package verify judges the migration track's evidence (Track G, docs/migration.md).
//
// The central question of a cutover is what a consumer group does when it moves from the old
// cluster to the new one: MirrorMaker 2 translates the group's committed offsets onto the target,
// and the consumer resumes there. A correct cutover never skips a record; it may re-read a few
// records below the translation point, never more than the gap between the source commit and the
// translated commit.
//
// Every judgement here is a pure function of offsets, so the mutation self-tests can prove each
// defect turns the verifier red without a broker.
package verify

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Partition is one partition's evidence after a cutover. Offsets are on the mirrored (target)
// topic; MirrorMaker 2 preserves offsets on a freshly created remote topic, which the row checks
// separately (OffsetsIdentical).
type Partition struct {
	SourceCommitted int64 // the group's committed offset on the source cluster at cutover
	Translated      int64 // the group's committed offset on the target, as MirrorMaker 2 wrote it; -1 when none
	// A partition MirrorMaker 2 did not translate (since Kafka 3.5 it keeps sparse offset syncs, so
	// an old commit may have no usable sync) must resume from the log start: the migrated consumer
	// runs with auto.offset.reset=earliest (docs/migration.md), so it re-reads, never skips.
	End       int64   // the target topic's log end offset when the consumer started
	Delivered []int64 // offsets the resumed consumer received, in delivery order
}

// Resume is the verdict-relevant facts of one consumer's resume on the target.
type Resume struct {
	AllTranslated   bool // every partition got a translated offset (reported, not judged: it depends on sync timing)
	Untranslated    int
	Partitions      int
	NeverAhead      bool // no translated offset is above the source commit (ahead = records skipped)
	StartedAtCommit bool // in every partition the first delivered offset is the translated offset
	NothingSkipped  bool // every offset in [translated, end) was delivered
	NoDuplicates    bool // no offset delivered twice
	InOrder         bool // offsets delivered in increasing order within each partition
	Rereads         int64
	Problems        []string
}

// Observation is what the row compares between the oracle leg and the KubeMQ leg. Rereads
// depend on when MirrorMaker 2 last synced offsets, so they are reported but not compared.
func (r Resume) Observation() map[string]any {
	return map[string]any{
		"some_translated": r.Untranslated < r.Partitions,
		"never_ahead":     r.NeverAhead, "started_at_commit": r.StartedAtCommit,
		"nothing_skipped": r.NothingSkipped, "no_duplicates": r.NoDuplicates, "in_order": r.InOrder,
	}
}

// OK is true when the resume is correct. A cutover where MirrorMaker 2 translated no partition at
// all is not correct even though nothing is skipped: every consumer would re-read everything.
func (r Resume) OK() bool {
	return r.Untranslated < r.Partitions && r.NeverAhead && r.StartedAtCommit && r.NothingSkipped && r.NoDuplicates && r.InOrder
}

// CheckResume judges a resumed consumer. parts is keyed by partition number.
func CheckResume(parts map[int32]Partition) Resume {
	r := Resume{AllTranslated: true, NeverAhead: true, StartedAtCommit: true, NothingSkipped: true, NoDuplicates: true, InOrder: true}
	ids := make([]int32, 0, len(parts))
	for p := range parts {
		ids = append(ids, p)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	bad := func(flag *bool, format string, args ...any) {
		*flag = false
		r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
	}
	r.Partitions = len(ids)
	for _, p := range ids {
		e := parts[p]
		start := e.Translated
		if start < 0 {
			r.AllTranslated = false
			r.Untranslated++
			r.Problems = append(r.Problems, fmt.Sprintf("partition %d: not translated (source committed %d); resumes from the log start", p, e.SourceCommitted))
			start = 0
		}
		if e.Translated > e.SourceCommitted {
			bad(&r.NeverAhead, "partition %d: translated %d is ahead of the source commit %d (%d records skipped)", p, e.Translated, e.SourceCommitted, e.Translated-e.SourceCommitted)
		} else {
			r.Rereads += e.SourceCommitted - start
		}
		seen := map[int64]bool{}
		last := int64(-1)
		for i, off := range e.Delivered {
			if i == 0 && off != start {
				bad(&r.StartedAtCommit, "partition %d: first delivered offset %d, expected start %d", p, off, start)
			}
			if seen[off] {
				bad(&r.NoDuplicates, "partition %d: offset %d delivered twice", p, off)
			}
			if off <= last {
				bad(&r.InOrder, "partition %d: offset %d delivered after %d", p, off, last)
			}
			seen[off] = true
			if off > last {
				last = off
			}
		}
		if len(e.Delivered) == 0 && start < e.End {
			bad(&r.StartedAtCommit, "partition %d: nothing delivered, %d records after the expected start %d", p, e.End-start, start)
		}
		missing := 0
		for off := start; off < e.End; off++ {
			if !seen[off] {
				missing++
			}
		}
		if missing > 0 {
			bad(&r.NothingSkipped, "partition %d: %d of the offsets %d..%d never delivered", p, missing, start, e.End-1)
		}
	}
	return r
}

// Diff compares two observations field by field, in the format every track uses
// ("field: Kafka x, KubeMQ y; ..."), and reports whether they are identical.
func Diff(oracle, kubemq map[string]any) (string, bool) {
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
	out := ""
	for _, k := range sorted {
		o, ok1 := oracle[k]
		q, ok2 := kubemq[k]
		os, qs := enc(o, ok1), enc(q, ok2)
		if os != qs {
			if out != "" {
				out += "; "
			}
			out += fmt.Sprintf("%s: Kafka %s, KubeMQ %s", k, os, qs)
		}
	}
	return out, out == ""
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
