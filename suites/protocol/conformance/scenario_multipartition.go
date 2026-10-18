package conformance

// scenario_multipartition.go — the multi-partition conformance scenario.
//
// ScenarioMultiPartition mirrors scenario_core.go's shape but is partition-explicit: it pins ONE
// client partitioner (kgo.ManualPartitioner, wired driver-wide in driver_franzgo.go's Run — the
// broker does NOT hash) and targets every record's partition directly via Step.Partition,
// so there is deliberately no cross-client key-placement question at all (murmur2 vs crc32 never
// enters the picture — every partition assignment here is explicit, not hash-derived).
//
// Steps, per partition p in {0,1,2}:
//  1. CreateTopic{NumPartitions: 3} — once, for the whole scenario.
//  2. Metadata — confirms the topic is visible.
//  3. Per-partition keyed produce: a batch of keyed records tagged with p in their key, so
//     per-key order within a partition is independently verifiable.
//  4. Per-partition fetch-from-earliest, count-terminated.
//  5. Per-partition ListOffsets (earliest/latest).
//  6. A second produce round per partition (fewer records), then a final per-partition fetch
//     verifying the new end — mirrors ScenarioCore's own two-generation shape so a mid-fetch/
//     contiguity check spans two produce generations.
//  7. A final per-partition ListOffsets (earliest/latest again — latest should have advanced).
//
// Assertions layer: per-partition contiguity is already asserted for free by the existing
// CompareTranscripts/Normalize/CheckFreshTopicBaseOffsetZero machinery (normalize.go), which was
// already keyed by (topic,partition) — never exercised with partition>0 data until this scenario.
// Per-key order within a partition is asserted by scenarioMultiPartitionCheckKeyOrder below, a
// small pure helper mirroring diff.go's CheckFreshTopicBaseOffsetZero invariant-check pattern.

const (
	// scenarioMultiPartitionName is the LogicalTopic / Scenario.Name for ScenarioMultiPartition.
	scenarioMultiPartitionName = "multipartition"

	// scenarioMultiPartitionCount is the number of partitions the scenario's topic is created
	// with — deliberately small (well under the maxKafkaPartitions=256 cap) but > 1, the
	// minimum needed to prove per-partition independence.
	scenarioMultiPartitionCount = int32(3)

	// scenarioMultiPartitionInitialPerPartition is the number of keyed records produced to EACH
	// partition in the first produce round.
	scenarioMultiPartitionInitialPerPartition = 10

	// scenarioMultiPartitionFollowupPerPartition is the number of additional keyed records
	// produced to EACH partition in the second produce round (mirrors ScenarioCore's
	// two-generation shape).
	scenarioMultiPartitionFollowupPerPartition = 4

	// scenarioMultiPartitionTotalPerPartition is the total record count per partition after both
	// produce rounds — also the mid-topic fetch's count (a full re-read from earliest through the
	// new end, since each partition is independently small enough that a from-scratch re-read is
	// simpler and just as meaningful as ScenarioCore's offset-15 mid-fetch).
	scenarioMultiPartitionTotalPerPartition = scenarioMultiPartitionInitialPerPartition + scenarioMultiPartitionFollowupPerPartition
)

// scenarioMultiPartitionRecord builds one deterministic keyed Record for partition p, index idx
// within that partition's produce round. The key embeds BOTH p and idx (e.g. "p0-k03") so
// scenarioMultiPartitionCheckKeyOrder can verify per-key order independently per partition, and
// so a cross-partition contamination bug (a record landing on the wrong partition) is trivially
// visible in a failed diff (the key itself names its intended partition).
func scenarioMultiPartitionRecord(p int32, idx int) Record {
	key := []byte(itoaPad(int(p)) + "-k" + itoaPad(idx))
	var value []byte
	if idx%5 == 0 {
		// Mixed sizes including an empty value, mirroring scenario_core.go's own requirement —
		// exercised per-partition here too, not just once globally.
		value = []byte{}
	} else {
		value = []byte("mp-value-p" + itoaPad(int(p)) + "-" + itoaPad(idx) + "-" + repeatByte('y', (idx%5)+1))
	}
	headers := scenarioCoreHeaderSets[idx%len(scenarioCoreHeaderSets)]
	return Record{Key: key, Value: value, Headers: headers}
}

// scenarioMultiPartitionProduceRecords builds count keyed records for partition p, starting at
// index startIdx (so the follow-up round's keys never collide with the initial round's).
func scenarioMultiPartitionProduceRecords(p int32, startIdx, count int) []Record {
	records := make([]Record, 0, count)
	for i := 0; i < count; i++ {
		records = append(records, scenarioMultiPartitionRecord(p, startIdx+i))
	}
	return records
}

// ScenarioMultiPartition is the multi-partition conformance scenario: a 3-partition topic,
// per-partition keyed produce (two rounds) / fetch-from-earliest / ListOffsets, with every
// record's partition pinned explicitly via Step.Partition (not hash-derived). See the file doc
// above for the full step-by-step contract.
func ScenarioMultiPartition() Scenario {
	steps := []Step{
		{
			Action:        ActionCreateTopic,
			Topic:         scenarioMultiPartitionName,
			Partition:     0,
			NumPartitions: scenarioMultiPartitionCount,
		},
		{
			Action:    ActionMetadata,
			Topic:     scenarioMultiPartitionName,
			Partition: 0,
		},
	}

	// Round 1: per-partition initial produce.
	for p := int32(0); p < scenarioMultiPartitionCount; p++ {
		steps = append(steps, Step{
			Action:    ActionProduce,
			Topic:     scenarioMultiPartitionName,
			Partition: p,
			Records:   scenarioMultiPartitionProduceRecords(p, 0, scenarioMultiPartitionInitialPerPartition),
		})
	}

	// Per-partition fetch-from-earliest (count-terminated) of round 1's records.
	for p := int32(0); p < scenarioMultiPartitionCount; p++ {
		steps = append(steps, Step{
			Action:      ActionFetchFrom,
			Topic:       scenarioMultiPartitionName,
			Partition:   p,
			FetchOffset: 0,
			FetchCount:  scenarioMultiPartitionInitialPerPartition,
		})
	}

	// Per-partition ListOffsets (earliest/latest) after round 1.
	for p := int32(0); p < scenarioMultiPartitionCount; p++ {
		steps = append(steps, Step{
			Action:          ActionListOffsets,
			Topic:           scenarioMultiPartitionName,
			Partition:       p,
			ListOffsetsSpec: ListOffsetsSpec{Earliest: true, Latest: true},
		})
	}

	// Round 2: per-partition follow-up produce.
	for p := int32(0); p < scenarioMultiPartitionCount; p++ {
		steps = append(steps, Step{
			Action:    ActionProduce,
			Topic:     scenarioMultiPartitionName,
			Partition: p,
			Records:   scenarioMultiPartitionProduceRecords(p, scenarioMultiPartitionInitialPerPartition, scenarioMultiPartitionFollowupPerPartition),
		})
	}

	// Per-partition fetch-from-earliest through the new end (spans both produce generations,
	// mirroring ScenarioCore's mid-fetch-spans-both-generations shape).
	for p := int32(0); p < scenarioMultiPartitionCount; p++ {
		steps = append(steps, Step{
			Action:      ActionFetchFrom,
			Topic:       scenarioMultiPartitionName,
			Partition:   p,
			FetchOffset: 0,
			FetchCount:  scenarioMultiPartitionTotalPerPartition,
		})
	}

	// Final per-partition ListOffsets (latest should have advanced by the follow-up count).
	for p := int32(0); p < scenarioMultiPartitionCount; p++ {
		steps = append(steps, Step{
			Action:          ActionListOffsets,
			Topic:           scenarioMultiPartitionName,
			Partition:       p,
			ListOffsetsSpec: ListOffsetsSpec{Earliest: true, Latest: true},
		})
	}

	return Scenario{Name: scenarioMultiPartitionName, Steps: steps}
}

// scenarioMultiPartitionCheckKeyOrder walks events (assumed to be one transcript's FetchFrom
// events, any order) and asserts that, WITHIN each (topic,partition) and WITHIN each contiguous
// fetch RUN (see below), records appear in the SAME relative order they were produced — i.e. the
// PRODUCE-ORDER INDEX embedded in each key (scenarioMultiPartitionRecord's key format,
// "<partition>-k<idx>" — idx is the record's position in its produce round) must be
// non-decreasing across the run's events, in the order those events were RECORDED (which is
// fetch/offset order — see driver_franzgo.go's runFetchFrom, a single ascending poll loop). This
// is the actual "produce order preserved" invariant: a broker that served
// records out of the order they were appended would show a LOWER produce-order index appearing
// AFTER a higher one within the same run, even though RelOffset itself is ascending by
// construction (checking RelOffset ascension alone would be checking nothing — the driver already
// guarantees that; see errKeyOrderKeyRegression's doc for why this index-based check, not an
// offset-based one, is what actually has teeth).
//
// RUN BOUNDARY (found empirically): ScenarioMultiPartition's step 4/generation-2
// re-reads each partition from earliest (offset 0) through the new end — deliberately mirroring
// ScenarioCore's own two-generation shape, so a mid-fetch/contiguity check spans two produce
// generations. That means the SAME (topic,partition)'s FetchFrom events legitimately
// restart at RelOffset 0 (and produce-order index 0) partway through the transcript — a naive
// whole-transcript monotonicity check would misclassify that intentional re-fetch as an "out of
// order" violation (confirmed empirically: an earlier version of this checker, keyed on raw
// RelOffset without run-boundary detection, flagged a legitimate "prior=9 got=0" transition on
// BOTH the kubemq and kfake transcripts identically, proving it was a checker bug, not a
// server-side ordering bug). A RelOffset STRICTLY LESS than the previous event's RelOffset for the
// same (topic,partition) is therefore treated as the start of a NEW fetch run (resetting the
// "last seen produce-order index" tracking for that pair), not a violation in itself.
//
// Deliberately NOT asserted: any
// cross-client key-PLACEMENT comparison — every record's target partition in this scenario is
// explicit (Step.Partition), never hash-derived, so there is nothing to compare across drivers on
// that axis.
func scenarioMultiPartitionCheckKeyOrder(events []Event) error {
	type partitionKey struct {
		topic     string
		partition int32
	}
	// last RelOffset seen per (topic,partition), used only to detect a new fetch run (see doc
	// above) — never itself the invariant being checked.
	lastOffset := make(map[partitionKey]int64)
	haveLast := make(map[partitionKey]bool)
	// last produce-order index seen per (topic,partition) WITHIN THE CURRENT RUN, reset whenever
	// a new run starts for that (topic,partition). This IS the invariant being checked.
	lastIdx := make(map[partitionKey]int)
	haveIdx := make(map[partitionKey]bool)

	for _, e := range events {
		if e.Action != string(ActionFetchFrom) || e.ErrCode != 0 {
			continue
		}
		pk := partitionKey{topic: e.Topic, partition: e.Partition}

		newRun := !haveLast[pk] || e.RelOffset < lastOffset[pk]
		if newRun {
			delete(lastIdx, pk)
			delete(haveIdx, pk)
		}
		lastOffset[pk] = e.RelOffset
		haveLast[pk] = true

		if e.Key == nil {
			continue
		}
		idx, ok := parseScenarioMultiPartitionKeyIndex(string(e.Key))
		if !ok {
			// A key that doesn't match this scenario's own "<partition>-k<idx>" format carries no
			// produce-order signal this checker can use — skip it rather than false-positive on
			// unrelated key shapes (defensive; scenarioMultiPartitionRecord always produces the
			// expected format, so this branch is not expected to fire in practice).
			continue
		}
		if haveIdx[pk] && idx < lastIdx[pk] {
			return errKeyOrderKeyRegression(e.Topic, e.Partition, string(e.Key), lastIdx[pk], idx)
		}
		lastIdx[pk] = idx
		haveIdx[pk] = true
	}

	return nil
}

// errKeyOrderKeyRegression builds the descriptive error scenarioMultiPartitionCheckKeyOrder
// returns for a produce-order violation WITHIN one fetch run — a small helper so the function
// body above stays focused on the invariant logic, not string formatting.
func errKeyOrderKeyRegression(topic string, partition int32, key string, priorIdx, gotIdx int) error {
	return &keyOrderError{
		msg: "topic=" + topic + " partition=" + itoaSigned(partition) + " key=" + key +
			": record observed out of produce order within one fetch run: prior produce-order index=" + itoaSigned64(int64(priorIdx)) + " got=" + itoaSigned64(int64(gotIdx)),
	}
}

// parseScenarioMultiPartitionKeyIndex parses the trailing "k<idx>" component of a key produced by
// scenarioMultiPartitionRecord ("<partition>-k<idx>", both zero-padded 2-digit via itoaPad) and
// returns the produce-order index. ok is false if key does not match this exact format (see the
// caller's doc for why that is skipped rather than treated as an error).
func parseScenarioMultiPartitionKeyIndex(key string) (idx int, ok bool) {
	const marker = "-k"
	// Find the LAST occurrence of "-k" (the partition prefix is zero-padded digits, never
	// containing "-k", so the last occurrence is unambiguous).
	pos := -1
	for i := 0; i+len(marker) <= len(key); i++ {
		if key[i:i+len(marker)] == marker {
			pos = i
		}
	}
	if pos < 0 {
		return 0, false
	}
	digits := key[pos+len(marker):]
	if len(digits) == 0 {
		return 0, false
	}
	n := 0
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

// keyOrderError is a plain error type for scenarioMultiPartitionCheckKeyOrder's violations.
type keyOrderError struct{ msg string }

func (e *keyOrderError) Error() string { return "kafkaconformance: per-key order violated: " + e.msg }

// itoaSigned/itoaSigned64 render a possibly-negative int32/int64 as decimal without pulling in
// strconv/fmt at this small, hot-path-free call site (mirrors scenario_core.go's itoaPad
// convention, extended to handle negative values since RelOffset -1 is a legitimate "no offset"
// sentinel that could appear in a malformed/error transcript this helper must still describe
// clearly rather than panic on).
func itoaSigned(n int32) string { return itoaSigned64(int64(n)) }

func itoaSigned64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
