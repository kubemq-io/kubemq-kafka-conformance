package conformance

// scenario_core.go — the ScenarioCore scenario.
//
// ScenarioCore is the ONE scenario this suite ships. It deliberately pins a 1-partition topic (kills
// murmur2-vs-crc32 partition-assignment divergence between drivers and gives total order — see
// harness.go's Step.Partition doc and README.md's "Scenario catalog"), which is what makes
// cross-driver/cross-oracle comparison meaningful.
//
// Full-transcript (kgo) path — the eight steps documented in README.md's "Scenario catalog":
//
//  1. CreateTopic  — admin API, 1 partition.
//  2. Metadata     — confirm the topic is visible.
//  3. Produce      — 20 keyed + 10 unkeyed records, mixed sizes including an empty value; headers
//     included (their digest is part of the Event identity, so header fidelity is
//     actually verified on the kgo path, not just claimed).
//  4. FetchFrom    — earliest, a full COUNT-terminated read of all 30 records (never
//     timeout-terminated).
//  5. ListOffsets  — earliest and latest (should observe 30 records total so far).
//  6. Produce      — 5 more records.
//  7. FetchFrom    — mid-topic: starts partway through the log (offset 15) and reads the
//     remaining records through the new end (20 records: offsets 15..34).
//  8. ListOffsets  — earliest and latest again (latest should have advanced by 5).
//
// Smoke drivers (kcat, the Java kafka-*.sh CLI) do not run ScenarioCore itself — they run
// a produce+consume PROJECTION of it (see ScenarioCoreSmokeProduceCount / the smoke drivers'
// firstProduceStep helper in driver_kcat.go / driver_kafkacli.go, which picks the FIRST
// ActionProduce step's records — that is exactly ScenarioCore's step 3 keyed+unkeyed batch).
// scenario_core.go does not need a separate smoke-only scenario value: the smoke drivers derive
// their subset directly from ScenarioCore's Steps, so ScenarioCoreSmokeProduceCount below exists
// purely as a documented, testable constant other files/tests can assert against without
// re-deriving it from the Scenario's step contents.

// scenarioCoreName is the LogicalTopic / Scenario.Name for ScenarioCore. Every physical,
// per-run-unique topic name a Driver produces against normalizes back to this logical name (see
// normalize.go's Normalize doc).
const scenarioCoreName = "core"

// scenarioCoreKeyedProduceCount and scenarioCoreUnkeyedProduceCount are the exact record counts
// the plan mandates for the initial produce step: "20 keyed + 10 unkeyed (mixed sizes incl.
// empty value; headers included ...)".
const (
	scenarioCoreKeyedProduceCount   = 20
	scenarioCoreUnkeyedProduceCount = 10
	// scenarioCoreInitialProduceCount is the total record count of ScenarioCore's first Produce
	// step (step 3) — also the count the smoke drivers' produce+consume projection round-trips
	// (see driver_kcat.go/driver_kafkacli.go's firstProduceStep, which reads exactly this step).
	scenarioCoreInitialProduceCount = scenarioCoreKeyedProduceCount + scenarioCoreUnkeyedProduceCount

	// scenarioCoreFollowupProduceCount is the record count of ScenarioCore's second Produce step
	// (step 6 — "produce 5 more").
	scenarioCoreFollowupProduceCount = 5

	// scenarioCoreTotalProduceCount is the total records produced across both Produce steps,
	// i.e. the full log length by the time step 8 (the final ListOffsets) runs.
	scenarioCoreTotalProduceCount = scenarioCoreInitialProduceCount + scenarioCoreFollowupProduceCount

	// scenarioCoreMidFetchOffset is the starting offset for the "fetch-from-mid" step (step 7):
	// partway through the log, strictly between 0 and scenarioCoreTotalProduceCount-1, chosen so
	// the read spans both the pre-followup and post-followup regions of the log (offsets 0..29
	// from the first produce, 30..34 from the second) without being the true earliest or latest
	// boundary — offset 15 sits inside the first (30-record) batch, so the mid-fetch reads
	// records 15..29 (15 records from the first batch) plus 30..34 (5 records from the
	// follow-up), i.e. every record from there to the new end.
	scenarioCoreMidFetchOffset = 15

	// scenarioCoreMidFetchCount is the number of records the mid-topic fetch (step 7) must
	// observe: everything from scenarioCoreMidFetchOffset through the new end of the log
	// (scenarioCoreTotalProduceCount - scenarioCoreMidFetchOffset records).
	scenarioCoreMidFetchCount = scenarioCoreTotalProduceCount - scenarioCoreMidFetchOffset
)

// ScenarioCoreSmokeProduceCount is the record count a Smoke driver's produce+consume projection
// of ScenarioCore round-trips (see driver_kcat.go / driver_kafkacli.go's firstProduceStep, which
// reads the Scenario's FIRST ActionProduce step — ScenarioCore's step 3). Exported as a named
// constant so tests (and any future smoke driver) can assert against it without re-deriving it
// from ScenarioCore's step contents.
const ScenarioCoreSmokeProduceCount = scenarioCoreInitialProduceCount

// scenarioCoreHeaderSets provides a small, deterministic rotation of header sets applied across
// the produced records so header fidelity is exercised without needing a distinct header set per
// record. Index 0 is "no headers" (nil) — Kafka records legitimately carry zero headers, and
// HashHeaders documents that as returning "" distinctly from a hash of zero bytes, so this
// scenario must include that case to actually exercise it.
var scenarioCoreHeaderSets = [][]RecordHeader{
	nil, // no headers
	{{Key: "trace-id", Value: []byte("abc-123")}},
	{{Key: "content-type", Value: []byte("application/json")}, {Key: "source", Value: []byte("kafka-conformance")}},
}

// scenarioCoreRecord builds one deterministic Record for ScenarioCore's produce steps. keyed
// selects whether the record carries a key (nil Key otherwise — Kafka distinguishes "no key"
// from an empty-but-present key, and Record.Key's doc requires nil for "no key"). idx is the
// record's position within its keyed/unkeyed group, used to vary size (including one empty
// value, per the plan's "mixed sizes incl. empty value" requirement) and to rotate through
// scenarioCoreHeaderSets deterministically.
func scenarioCoreRecord(keyed bool, idx int) Record {
	var key []byte
	if keyed {
		key = []byte("key-" + itoaPad(idx))
	}

	// Mixed sizes including an empty value: every 5th record (idx%5==0) gets an empty (non-nil)
	// value, per the plan's explicit "incl. empty value" requirement; the rest scale with idx so
	// payload sizes vary across the batch (never all identical).
	var value []byte
	if idx%5 == 0 {
		value = []byte{}
	} else {
		value = []byte("value-" + itoaPad(idx) + "-" + repeatByte('x', (idx%7)+1))
	}

	headers := scenarioCoreHeaderSets[idx%len(scenarioCoreHeaderSets)]

	return Record{Key: key, Value: value, Headers: headers}
}

// itoaPad renders a small non-negative int as a zero-padded 2-digit decimal string (sufficient
// for ScenarioCore's record counts, all < 100), without pulling in strconv/fmt for such a small,
// hot-path-free helper.
func itoaPad(n int) string {
	digits := "0123456789"
	tens := n / 10
	ones := n % 10
	return string([]byte{digits[tens], digits[ones]})
}

// repeatByte returns a string of n copies of b, used to vary ScenarioCore's record value sizes
// deterministically.
func repeatByte(b byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return string(out)
}

// scenarioCoreInitialProduceRecords builds the 20 keyed + 10 unkeyed records for step 3, in a
// fixed, deterministic order (keyed records first, then unkeyed) so RelOffset assignment is
// predictable for the mid-fetch offset math above.
func scenarioCoreInitialProduceRecords() []Record {
	records := make([]Record, 0, scenarioCoreInitialProduceCount)
	for i := 0; i < scenarioCoreKeyedProduceCount; i++ {
		records = append(records, scenarioCoreRecord(true, i))
	}
	for i := 0; i < scenarioCoreUnkeyedProduceCount; i++ {
		records = append(records, scenarioCoreRecord(false, i))
	}
	return records
}

// scenarioCoreFollowupProduceRecords builds the 5 additional records for step 6, keyed (varying
// the idx range so their keys don't collide with the initial batch's keyed records).
func scenarioCoreFollowupProduceRecords() []Record {
	records := make([]Record, 0, scenarioCoreFollowupProduceCount)
	for i := 0; i < scenarioCoreFollowupProduceCount; i++ {
		records = append(records, scenarioCoreRecord(true, scenarioCoreKeyedProduceCount+i))
	}
	return records
}

// ScenarioCore is the conformance scenario: 1-partition topic create ->
// metadata -> 20 keyed + 10 unkeyed produce (incl. empty value + headers) -> fetch-from-earliest
// count-terminated -> ListOffsets earliest/latest -> +5 produce -> fetch-from-mid ->
// ListOffsets. See the file doc above for the exact step-by-step contract.
func ScenarioCore() Scenario {
	return Scenario{
		Name: scenarioCoreName,
		Steps: []Step{
			{
				Action:    ActionCreateTopic,
				Topic:     scenarioCoreName,
				Partition: 0,
			},
			{
				Action:    ActionMetadata,
				Topic:     scenarioCoreName,
				Partition: 0,
			},
			{
				Action:    ActionProduce,
				Topic:     scenarioCoreName,
				Partition: 0,
				Records:   scenarioCoreInitialProduceRecords(),
			},
			{
				Action:      ActionFetchFrom,
				Topic:       scenarioCoreName,
				Partition:   0,
				FetchOffset: 0,
				FetchCount:  scenarioCoreInitialProduceCount,
			},
			{
				Action:          ActionListOffsets,
				Topic:           scenarioCoreName,
				Partition:       0,
				ListOffsetsSpec: ListOffsetsSpec{Earliest: true, Latest: true},
			},
			{
				Action:    ActionProduce,
				Topic:     scenarioCoreName,
				Partition: 0,
				Records:   scenarioCoreFollowupProduceRecords(),
			},
			{
				Action:      ActionFetchFrom,
				Topic:       scenarioCoreName,
				Partition:   0,
				FetchOffset: scenarioCoreMidFetchOffset,
				FetchCount:  scenarioCoreMidFetchCount,
			},
			{
				Action:          ActionListOffsets,
				Topic:           scenarioCoreName,
				Partition:       0,
				ListOffsetsSpec: ListOffsetsSpec{Earliest: true, Latest: true},
			},
		},
	}
}
