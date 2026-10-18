package conformance

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
)

// This file is THE load-bearing contract for the whole harness. Two independent rules exist
// here on purpose and must never be merged:
//
//  1. Normalize() rewrites offsets to be BASE-RELATIVE (relative to the first observed offset
//     for a topic-partition within a transcript) so two transcripts recorded against different
//     targets — which may start a fresh topic at different absolute offsets after retention,
//     compaction-adjacent effects, or simply because the two brokers assigned different
//     per-run physical starting points — can still be compared for equality.
//
//  2. CheckFreshTopicBaseOffsetZero() is a SEPARATE, absolute invariant: a FRESH topic's first
//     fetched baseOffset MUST be exactly 0 on every target, full stop. This guards against the
//     wire risk of a 1-based STAN-sequence leak onto the Kafka wire. If rule 1 (base-relative
//     normalization) were the only check, a target that leaked sequence 1 as its "first"
//     offset would still normalize to a base-relative 0 and the bug would be silently hidden.
//     Rule 2 catches it BEFORE normalization discards the absolute value.
//
// Never fold rule 2 into rule 1's logic.

// ErrNoEvents is returned by CheckFreshTopicBaseOffsetZero when a transcript has no FetchFrom
// events to check — the caller should treat this as an infra/setup problem, not silently pass.
var ErrNoEvents = errors.New("kafkaconformance: no FetchFrom events to check for baseOffset invariant")

// eventKey identifies the (topic, partition) stream an Event's offset belongs to, for the
// purposes of base-relative rewriting.
type eventKey struct {
	topic     string
	partition int32
}

// Normalize returns a copy of the transcript with every Event's Topic rewritten to the
// scenario's LogicalTopic (physical → logical mapping), offsets rewritten to be
// topic-partition-base-relative, and volatile fields scrubbed (see scrubEvent). The input
// transcript is not mutated.
//
// Base-relative rewrite: for each distinct (topic, partition) pair seen in the transcript, the
// MINIMUM RelOffset observed among that pair's OFFSET-BEARING events becomes that pair's base;
// every event of that pair gets RelOffset -= base. This makes two transcripts recorded against
// targets that assign different absolute starting offsets comparable, while preserving the
// internal ordering/spacing between offsets (which IS meaningful — e.g. gaps from control
// records or skipped test data would still show up as a diff).
//
// "Offset-bearing" is deliberately narrow: only Produce and FetchFrom events with ErrCode == 0
// and RelOffset >= 0 carry a genuine log offset (see driver_franzgo.go's runCreateTopic/
// runProduce/runFetchFrom/runListOffsets/runMetadata). CreateTopic and Metadata events always
// record RelOffset's zero-value (0) — never a real offset — and any event with a non-zero
// ErrCode or a negative RelOffset (the -1 "no offset" sentinel used on produce/fetch/list-offsets
// failure) carries no real offset either. Letting those events into the min() would corrupt the
// base: e.g. a CreateTopic event's spurious RelOffset=0 would floor the base at 0 even when every
// real Produce/FetchFrom offset starts at 1000+, silently defeating the base-relative rewrite for
// every other event in the pair. ListOffsets events are also excluded from the base even on
// success — they are boundary queries about the log, not positions within the produced/fetched
// stream being compared, and including them would let a stale/racy boundary reading (e.g. an
// earliest/latest call issued before all produces land) skew the base away from the actual
// first produced/fetched record.
//
// Events excluded from the base computation are NOT excluded from the output — every event still
// appears in the normalized transcript (so CreateTopic/Metadata/ListOffsets/error events remain
// diffable), but only offset-bearing events have their RelOffset rewritten relative to the base;
// all other events pass their RelOffset through unchanged (their RelOffset already carries no
// comparable log-offset semantics, so there is nothing meaningful to rebase).
func Normalize(t Transcript) Transcript {
	// Compute the per-(topic,partition) base offset (minimum observed RelOffset) using the
	// PHYSICAL topic name recorded on each event — the mapping to LogicalTopic happens after
	// the base is computed, since the base must be computed within one physical stream. Only
	// Produce/FetchFrom events that succeeded and carry a real (non-negative) offset
	// contribute to the base — see the doc above for why CreateTopic/Metadata/ListOffsets/
	// error events must NOT participate.
	bases := make(map[eventKey]int64)
	haveBase := make(map[eventKey]bool)
	for _, e := range t.Events {
		if !isOffsetBearing(e) {
			continue
		}
		k := eventKey{topic: e.Topic, partition: e.Partition}
		if !haveBase[k] || e.RelOffset < bases[k] {
			bases[k] = e.RelOffset
			haveBase[k] = true
		}
	}

	out := Transcript{
		Driver:       t.Driver,
		DriverKind:   t.DriverKind,
		Target:       t.Target,
		LogicalTopic: t.LogicalTopic,
		Events:       make([]Event, 0, len(t.Events)),
	}

	for _, e := range t.Events {
		k := eventKey{topic: e.Topic, partition: e.Partition}
		ne := scrubEvent(e)
		// Only rebase events that actually contributed (or could have contributed) a genuine
		// log offset; non-offset-bearing events (CreateTopic/Metadata/ListOffsets/errors) pass
		// their RelOffset through unchanged — there is no per-pair base to subtract from a
		// value that was never a comparable log offset in the first place.
		if isOffsetBearing(e) && haveBase[k] {
			ne.RelOffset = e.RelOffset - bases[k]
		}
		// Physical topic -> logical topic. This scenario covers exactly one logical
		// topic per transcript (Transcript.LogicalTopic), so every event's physical topic name
		// maps to that same logical name. A future multi-topic scenario would need a real
		// per-event physical->logical table; not needed here (see harness.go's
		// Transcript.LogicalTopic doc).
		ne.Topic = t.LogicalTopic
		out.Events = append(out.Events, ne)
	}

	return out
}

// isOffsetBearing reports whether e carries a genuine log offset that should participate in (and
// be rewritten by) Normalize's base-relative rewrite. Only successful Produce and FetchFrom
// events with a non-negative RelOffset qualify — see the Normalize doc above for the full
// rationale (CreateTopic/Metadata always leave RelOffset at its zero-value; ListOffsets queries a
// boundary rather than a position in the stream being compared; any event with a non-zero
// ErrCode or the -1 "no offset" failure sentinel carries no real offset).
func isOffsetBearing(e Event) bool {
	if e.ErrCode != 0 || e.RelOffset < 0 {
		return false
	}
	return e.Action == string(ActionProduce) || e.Action == string(ActionFetchFrom)
}

// scrubEvent returns a copy of e with volatile, non-semantic fields scrubbed so that
// comparisons across targets/oracles/runs are not defeated by incidental differences.
//
// Scrub rules:
//   - ValueSHA / HeadersSHA are already content digests (see HashValue/HashHeaders below) — no
//     rewrite needed here, but computing them via those helpers (rather than embedding
//     timestamps, broker IDs, or host:port strings) IS the scrub for record content.
//   - Topic is rewritten by the caller (Normalize) to the LogicalTopic — physical, per-run
//     unique topic names never survive into a normalized Event.
//   - RelOffset is rewritten by the caller (Normalize) to be base-relative.
//   - Partition, Key, Action, ErrCode pass through unchanged — they are already
//     target-independent, semantic fields.
//
// This function exists as an explicit seam: if a future target leaks a volatile field into
// Event (e.g. a raw timestamp or broker-id), scrub it here rather than special-casing diff.go.
func scrubEvent(e Event) Event {
	return Event{
		Action:     e.Action,
		Topic:      e.Topic, // overwritten by caller with LogicalTopic
		Partition:  e.Partition,
		Key:        e.Key,
		ValueSHA:   e.ValueSHA,
		HeadersSHA: e.HeadersSHA,
		RelOffset:  e.RelOffset, // overwritten by caller with base-relative value
		ErrCode:    e.ErrCode,
	}
}

// HashValue returns the hex-encoded sha256 of a record value, for use as Event.ValueSHA. A nil
// and an empty (non-nil) []byte both hash to the sha256 of zero bytes — Kafka has no wire
// distinction between "no value" (a null value, tombstone-style) and "" at the digest level
// used here; callers that must distinguish null-vs-empty values need a side channel (out of
// scope for this scenario, which does not produce null values).
func HashValue(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

// HashHeaders returns the hex-encoded sha256 of the canonical encoding of a record's headers,
// for use as Event.HeadersSHA. Canonical encoding: headers are taken in WIRE ORDER (Kafka
// headers are an ordered list, not a map — order is significant and must not be sorted), each
// encoded as len(key) ':' key ':' len(value) ':' value using decimal ASCII lengths as
// unambiguous separators (a key or value containing a literal ':' cannot be confused with the
// separator because the lengths are explicit), concatenated, then hashed.
//
// A nil or empty headers slice returns "" (the empty string), not a hash of zero bytes — this
// makes "no headers" trivially distinguishable from "headers hashed to this coincidental value"
// in reports/diffs.
func HashHeaders(headers []RecordHeader) string {
	if len(headers) == 0 {
		return ""
	}
	h := sha256.New()
	for _, hd := range headers {
		// hash.Hash.Write never returns an error (per the hash.Hash contract); the write is
		// unconditional and its return value is intentionally ignored.
		_, _ = fmt.Fprintf(h, "%d:%s:%d:%s", len(hd.Key), hd.Key, len(hd.Value), hd.Value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// CheckFreshTopicBaseOffsetZero enforces the ABSOLUTE invariant (see file doc rule 2): among a
// transcript's FetchFrom events for a fresh (this-run-created) topic-partition, the minimum
// observed RAW (pre-normalization) RelOffset must be exactly 0. Call this BEFORE Normalize() —
// it inspects the transcript's raw, absolute offsets, not the base-relative ones.
//
// Returns an error describing the violation (offset, topic, partition, target) if the invariant
// is broken, ErrNoEvents if there are no SUCCESSFUL FetchFrom events to check, or nil if the
// invariant holds.
//
// FetchFrom ERROR events (ErrCode != 0) are SKIPPED when computing the per-(topic,partition)
// minimum: a failed fetch leaves RelOffset at its zero-value (0), never a real served offset, so
// letting it into the min() would floor the minimum at 0 and falsely certify "first
// baseOffset==0" for a partition the broker never actually served. If, after this filtering, no
// (topic,partition) has any SUCCESSFUL FetchFrom event — including the case where a partition's
// only FetchFrom events were errors — this is treated exactly like the no-FetchFrom case
// (ErrNoEvents), so a partition served only errors is never silently passed as SUCCESS.
func CheckFreshTopicBaseOffsetZero(t Transcript) error {
	type minKey struct {
		topic     string
		partition int32
	}
	mins := make(map[minKey]int64)
	seen := make(map[minKey]bool)

	for _, e := range t.Events {
		if e.Action != string(ActionFetchFrom) {
			continue
		}
		// Skip FetchFrom ERROR events: their RelOffset is not a real served offset (it is left at
		// the zero-value on failure), so including it would corrupt the minimum and could falsely
		// report the invariant holds. A (topic,partition) whose only FetchFrom events are errors
		// therefore never enters mins and is handled by the len(mins)==0 => ErrNoEvents case below.
		if e.ErrCode != 0 {
			continue
		}
		k := minKey{topic: e.Topic, partition: e.Partition}
		if !seen[k] || e.RelOffset < mins[k] {
			mins[k] = e.RelOffset
			seen[k] = true
		}
	}

	if len(mins) == 0 {
		return ErrNoEvents
	}

	// Deterministic iteration order for a stable, reproducible error message across runs.
	keys := make([]minKey, 0, len(mins))
	for k := range mins {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].topic != keys[j].topic {
			return keys[i].topic < keys[j].topic
		}
		return keys[i].partition < keys[j].partition
	})

	var violations []string
	for _, k := range keys {
		if got := mins[k]; got != 0 {
			violations = append(violations, fmt.Sprintf(
				"target=%s topic=%s partition=%d: first fetched baseOffset=%d, want 0",
				t.Target, k.topic, k.partition, got))
		}
	}

	if len(violations) > 0 {
		return fmt.Errorf("kafkaconformance: absolute baseOffset==0 invariant violated: %v", violations)
	}
	return nil
}
