package conformance

// scenario_transactions.go — the EOS (transactions) conformance scenario.
//
// ScenarioTransactions mirrors scenario_core.go's shape (a 1-partition topic — total order and
// predictable offset math, per scenario_core.go's pin) but drives a kgo.TransactionalID
// producer (Scenario.TransactionalID set, so driver_franzgo.go builds a transactional client and
// the ActionBeginTxn/ActionCommitTxn/ActionAbortTxn steps drive InitProducerId + AddPartitionsToTxn
// + EndTxn on the wire) through:
//
//   - a COMMITTED batch (keys c-00..c-05),
//   - an OPEN-txn ListOffsets probe pair (LSO < HWM while txn 1 is still open — the isolation-level
//     tripwire),
//   - the first COMMIT,
//   - an ABORTED batch (keys a-00..a-03),
//   - a second COMMITTED batch (keys c-06..c-08),
//   - a read_committed read-back (ONLY the 9 c-* records; a-* filtered client-side via the broker's
//     AbortedTransactions, never past LSO — asserted AT THE CLIENT),
//   - a read_uncommitted read-back over the SAME log (ALL 13 data records incl. a-* — proving the
//     broker did NOT filter server-side),
//   - a final ListOffsets probe pair (LSO == HWM once every txn has decided).
//
// The fixed offset layout (identical on every oracle — each control marker consumes exactly one
// offset, exactly as real Kafka + kfake; markers are control batches, never delivered under EITHER
// isolation level):
//
//	absolute offsets: c-00..c-05 -> 0..5 | COMMIT -> 6 | a-00..a-03 -> 7..10 | ABORT -> 11 | c-06..c-08 -> 12..14 | COMMIT -> 15 | HWM = 16
//	read_committed delivered:   0,1,2,3,4,5, 12,13,14            (the 9 c-*; a-* filtered client-side; markers never delivered)
//	read_uncommitted delivered: 0,1,2,3,4,5, 7,8,9,10, 12,13,14  (all 13 data records; markers never delivered in EITHER isolation)
//
// Assertions layer: the primary proof is the CompareTranscripts/Normalize verdict=identical diff
// kfake<->KubeMQ (see conformance_transactions_kubemq_test.go). Two pure invariant checkers
// (mirroring scenario_multipartition.go's scenarioMultiPartitionCheckKeyOrder) assert the isolation
// binders RELATIONALLY per-target, independent of the cross-oracle compare:
// scenarioTransactionsCheckClientFiltering (read_committed never delivers an a-*; read_uncommitted
// delivers every a-*) and scenarioTransactionsCheckLSOTripwire (LSO < HWM while open, LSO == HWM
// once decided). Both run on the RAW transcript, before Normalize (like the multipartition checker).

const (
	// scenarioTxnName is the LogicalTopic / Scenario.Name for ScenarioTransactions. Every physical,
	// per-run-unique topic name a Driver produces against normalizes back to this logical name (see
	// normalize.go's Normalize doc).
	scenarioTxnName = "txn"

	// scenarioTxnTransactionalID is the kgo.TransactionalID the driver builds its producer with
	// (Scenario.TransactionalID) — it drives InitProducerId + fencing on the wire. Any
	// stable non-empty value works; this one is descriptive.
	scenarioTxnTransactionalID = "kconf-txn-producer"
)

// Record-set sizes. Distinct key prefixes (scenarioTxnCommittedKeyPrefix / scenarioTxnAbortedKeyPrefix)
// make the client-filtering invariant a trivial positional walk (see
// scenarioTransactionsCheckClientFiltering): every delivered read_committed record MUST be c-*,
// never a-*.
const (
	scenarioTxnCommitted1Count = 6 // c-00..c-05, offsets 0..5, COMMITTED
	scenarioTxnAbortedCount    = 4 // a-00..a-03, offsets 7..10, ABORTED (must never be RC-delivered)
	scenarioTxnCommitted2Count = 3 // c-06..c-08, offsets 12..14, COMMITTED

	// scenarioTxnCommittedTotal is the number of records a read_committed read-back must deliver
	// (== the RC FetchFrom step's FetchCount): every committed record, none aborted.
	scenarioTxnCommittedTotal = scenarioTxnCommitted1Count + scenarioTxnCommitted2Count // 9
	// scenarioTxnDataTotal is the number of records a read_uncommitted read-back must deliver
	// (== the RU FetchFrom step's FetchCount): every data record, committed AND aborted.
	scenarioTxnDataTotal = scenarioTxnCommittedTotal + scenarioTxnAbortedCount // 13

	// scenarioTxnOpenProbeLSO / scenarioTxnOpenProbeHWM are the expected values of the OPEN-txn
	// ListOffsets probe pair (issued after the first committed batch is produced but BEFORE its
	// COMMIT): read_committed latest returns the LSO (0 — the open txn pins the LSO at its first
	// offset) while read_uncommitted latest returns the HWM (6 — all 6 records already appended).
	// LSO < HWM is the tripwire; the checker asserts the relation, the cross-oracle
	// diff asserts the absolute values. Documented as named constants so the tripwire test can drive
	// the exact happy-path offsets.
	scenarioTxnOpenProbeLSO = 0
	scenarioTxnOpenProbeHWM = scenarioTxnCommitted1Count // 6
)

const (
	// scenarioTxnCommittedKeyPrefix / scenarioTxnAbortedKeyPrefix are the deterministic key prefixes
	// committedTxnKey / abortedTxnKey stamp. They are disjoint by construction so
	// scenarioTransactionsCheckClientFiltering can classify a delivered record's disposition
	// positionally from its key alone (a c-* record must be RC-delivered; an a-* record must be
	// filtered client-side under read_committed).
	scenarioTxnCommittedKeyPrefix = "c-"
	scenarioTxnAbortedKeyPrefix   = "a-"
)

// committedTxnKey builds the deterministic key for the idx-th committed record ("c-<idx>", idx
// zero-padded 2-digit via itoaPad). Committed records MUST be read_committed-delivered.
func committedTxnKey(idx int) []byte { return []byte(scenarioTxnCommittedKeyPrefix + itoaPad(idx)) }

// abortedTxnKey builds the deterministic key for the idx-th aborted record ("a-<idx>"). Aborted
// records MUST be filtered client-side under read_committed (never delivered), yet MUST still be
// served under read_uncommitted (proving no server-side filter).
func abortedTxnKey(idx int) []byte { return []byte(scenarioTxnAbortedKeyPrefix + itoaPad(idx)) }

// scenarioTxnRecord builds one deterministic keyed Record for ScenarioTransactions' produce steps.
// The value embeds the key (so committed/aborted records at overlapping idx values are still fully
// distinct records) and varies in size with idx; headers rotate through scenario_core.go's shared
// scenarioCoreHeaderSets so header fidelity is exercised through the transactional produce path too
// (its digest is part of the Event identity — see normalize.go's HashHeaders).
func scenarioTxnRecord(key []byte, idx int) Record {
	value := []byte("txn-value-" + string(key) + "-" + repeatByte('z', (idx%4)+1))
	headers := scenarioCoreHeaderSets[idx%len(scenarioCoreHeaderSets)]
	return Record{Key: key, Value: value, Headers: headers}
}

// scenarioTxnCommitted1Records builds the first committed batch (c-00..c-05, idx 0..5).
func scenarioTxnCommitted1Records() []Record {
	records := make([]Record, 0, scenarioTxnCommitted1Count)
	for i := 0; i < scenarioTxnCommitted1Count; i++ {
		records = append(records, scenarioTxnRecord(committedTxnKey(i), i))
	}
	return records
}

// scenarioTxnAbortedRecords builds the aborted batch (a-00..a-03, idx 0..3).
func scenarioTxnAbortedRecords() []Record {
	records := make([]Record, 0, scenarioTxnAbortedCount)
	for i := 0; i < scenarioTxnAbortedCount; i++ {
		records = append(records, scenarioTxnRecord(abortedTxnKey(i), i))
	}
	return records
}

// scenarioTxnCommitted2Records builds the second committed batch (c-06..c-08, idx 6..8 — the idx
// range continues past the first committed batch so the keys never collide with c-00..c-05).
func scenarioTxnCommitted2Records() []Record {
	records := make([]Record, 0, scenarioTxnCommitted2Count)
	for i := 0; i < scenarioTxnCommitted2Count; i++ {
		idx := scenarioTxnCommitted1Count + i // 6..8
		records = append(records, scenarioTxnRecord(committedTxnKey(idx), idx))
	}
	return records
}

// ScenarioTransactions is the EOS conformance scenario (mirrors ScenarioCore's
// 1-partition pin for total order + predictable offset math). It drives a kgo.TransactionalID
// producer through a committed txn, an aborted txn, and a second committed txn, probes the LSO
// while a txn is open, then reads the log back under BOTH isolation levels — proving (cross-oracle,
// verdict=identical, and relationally via the two checkers) the read_committed contract at the
// client. See the file doc above for the exact offset layout.
func ScenarioTransactions() Scenario {
	return Scenario{
		Name:            scenarioTxnName,
		TransactionalID: scenarioTxnTransactionalID,
		Steps: []Step{
			{Action: ActionCreateTopic, Topic: scenarioTxnName, Partition: 0}, // 1 partition (NumPartitions:0 => 1)
			{Action: ActionMetadata, Topic: scenarioTxnName, Partition: 0},
			// --- txn 1: COMMITTED (offsets 0..5, COMMIT marker at 6) ---
			{Action: ActionBeginTxn, Topic: scenarioTxnName, Partition: 0},
			{Action: ActionProduce, Topic: scenarioTxnName, Partition: 0, Records: scenarioTxnCommitted1Records()},
			// --- OPEN-txn tripwire probe: LSO(0) < HWM(6) while txn 1 is still open ---
			{Action: ActionListOffsets, Topic: scenarioTxnName, Partition: 0,
				ListOffsetsSpec: ListOffsetsSpec{Latest: true, ReadCommitted: true}}, // -> LSO (0)
			{Action: ActionListOffsets, Topic: scenarioTxnName, Partition: 0,
				ListOffsetsSpec: ListOffsetsSpec{Latest: true}}, // -> HWM (6)
			{Action: ActionCommitTxn, Topic: scenarioTxnName, Partition: 0},
			// --- txn 2: ABORTED (offsets 7..10, ABORT marker at 11) ---
			{Action: ActionBeginTxn, Topic: scenarioTxnName, Partition: 0},
			{Action: ActionProduce, Topic: scenarioTxnName, Partition: 0, Records: scenarioTxnAbortedRecords()},
			{Action: ActionAbortTxn, Topic: scenarioTxnName, Partition: 0},
			// --- txn 3: COMMITTED (offsets 12..14, COMMIT marker at 15) ---
			{Action: ActionBeginTxn, Topic: scenarioTxnName, Partition: 0},
			{Action: ActionProduce, Topic: scenarioTxnName, Partition: 0, Records: scenarioTxnCommitted2Records()},
			{Action: ActionCommitTxn, Topic: scenarioTxnName, Partition: 0},
			// --- read_committed read-back: ONLY the 9 committed records (a-* filtered client-side) ---
			{Action: ActionFetchFrom, Topic: scenarioTxnName, Partition: 0,
				FetchOffset: 0, FetchCount: scenarioTxnCommittedTotal, ReadCommitted: true},
			// --- read_uncommitted read-back: ALL 13 data records incl. a-* (NO server-side filter) ---
			{Action: ActionFetchFrom, Topic: scenarioTxnName, Partition: 0,
				FetchOffset: 0, FetchCount: scenarioTxnDataTotal},
			// --- final tripwire: all txns decided => LSO == HWM (== 16) ---
			{Action: ActionListOffsets, Topic: scenarioTxnName, Partition: 0,
				ListOffsetsSpec: ListOffsetsSpec{Latest: true, ReadCommitted: true}}, // -> HWM (16)
			{Action: ActionListOffsets, Topic: scenarioTxnName, Partition: 0,
				ListOffsetsSpec: ListOffsetsSpec{Latest: true}}, // -> HWM (16)
		},
	}
}

// scenarioTxnBoundaryLatest / scenarioTxnBoundaryLatestCommitted are the boundary labels
// driver_franzgo.go's runListOffsets stamps into Event.Key for a Latest ListOffsets query:
// "latest" for read_uncommitted (kadm.ListEndOffsets => the HWM) and "latest-committed" for
// read_committed (kadm.ListCommittedOffsets => the LSO). scenarioTransactionsCheckLSOTripwire keys
// on these EXACT strings to pick the RC and RU probe events apart, so they MUST stay in lockstep
// with runListOffsets' labels (harness_test.go locks the StepAction strings; these mirror the
// driver's boundary literals).
const (
	scenarioTxnBoundaryLatest          = "latest"
	scenarioTxnBoundaryLatestCommitted = "latest-committed"
)

// txnCheckError is the plain error type both ScenarioTransactions checkers return (mirroring
// scenario_multipartition.go's keyOrderError) — a small type so the checker bodies stay focused on
// invariant logic, not error-formatting friction.
type txnCheckError struct{ msg string }

func (e *txnCheckError) Error() string {
	return "kafkaconformance: transactions invariant violated: " + e.msg
}

// hasKeyPrefix reports whether key begins with prefix. Hand-rolled (like scenario_multipartition.go's
// own parsing helpers) so scenario_transactions.go stays import-free, matching the scenario files'
// house style.
func hasKeyPrefix(key []byte, prefix string) bool {
	if len(key) < len(prefix) {
		return false
	}
	return string(key[:len(prefix)]) == prefix
}

// scenarioTransactionsCheckClientFiltering asserts, positionally, the client-filtering
// contract on a transcript's FetchFrom events (recorded in fetch order by the driver — see
// driver_franzgo.go's runFetchFrom, a single ascending poll loop):
//
//   - the FIRST scenarioTxnCommittedTotal (9) SUCCESSFUL FetchFrom events are the read_committed
//     read-back: EVERY one MUST carry a c-* (committed) key and NONE may carry an a-* (aborted) key
//     — the client never delivered an aborted record (the HARD BINDER, asserted AT THE CLIENT).
//   - the NEXT scenarioTxnDataTotal (13) SUCCESSFUL FetchFrom events are the read_uncommitted
//     read-back and MUST include every a-* key (and every c-* key) — proving the broker served the
//     aborted DATA (no server-side filter).
//
// For robustness, FetchFrom events with ErrCode != 0 are SKIPPED (a transient fetch-error
// Event is a diffable Event the driver records — see runFetchFrom — but it carries no delivered
// record, so it must never shift the positional RC/RU boundary nor be misread as a c-*/a-* key).
// The positional split is deterministic because the scenario fixes both FetchCounts and the driver
// records one Event per DELIVERED record in order (control markers are never delivered under either
// isolation level — franz-go's EachRecord skips them, exactly like Java's Fetcher).
func scenarioTransactionsCheckClientFiltering(events []Event) error {
	// Collect only the SUCCESSFUL FetchFrom events, in recorded order (skip errors so
	// they never desync the positional boundary).
	var delivered []Event
	for _, e := range events {
		if e.Action != string(ActionFetchFrom) || e.ErrCode != 0 {
			continue
		}
		delivered = append(delivered, e)
	}

	want := scenarioTxnCommittedTotal + scenarioTxnDataTotal
	if len(delivered) < want {
		return &txnCheckError{msg: "expected at least " + itoaSigned64(int64(want)) +
			" successful FetchFrom events (" + itoaSigned64(int64(scenarioTxnCommittedTotal)) +
			" read_committed + " + itoaSigned64(int64(scenarioTxnDataTotal)) +
			" read_uncommitted), got " + itoaSigned64(int64(len(delivered))) +
			" — the read-back(s) dropped records (a record was withheld or a fetch errored)"}
	}

	rc := delivered[:scenarioTxnCommittedTotal]
	ru := delivered[scenarioTxnCommittedTotal : scenarioTxnCommittedTotal+scenarioTxnDataTotal]

	// (1) read_committed: EVERY delivered record is committed (c-*), NONE aborted (a-*).
	for i, e := range rc {
		key := string(e.Key)
		if hasKeyPrefix(e.Key, scenarioTxnAbortedKeyPrefix) {
			return &txnCheckError{msg: "read_committed read-back delivered an ABORTED record (key=" + key +
				") at position " + itoaSigned64(int64(i)) +
				" — a read_committed consumer must NEVER be delivered an aborted record (asserted AT THE CLIENT)"}
		}
		if !hasKeyPrefix(e.Key, scenarioTxnCommittedKeyPrefix) {
			return &txnCheckError{msg: "read_committed read-back delivered an unexpected non-committed key=" + key +
				" at position " + itoaSigned64(int64(i)) +
				" — read_committed must deliver only c-* committed records"}
		}
	}

	// (2) read_uncommitted: the broker served EVERY data record (no server-side filter). The
	// load-bearing half is the aborted keys — an a-* missing here is the no-server-side-filter
	// regression — but we assert the full 13-key set (all committed AND all aborted) for teeth.
	seen := make(map[string]bool, len(ru))
	for _, e := range ru {
		seen[string(e.Key)] = true
	}
	for i := 0; i < scenarioTxnAbortedCount; i++ {
		if k := string(abortedTxnKey(i)); !seen[k] {
			return &txnCheckError{msg: "read_uncommitted read-back did not deliver aborted record key=" + k +
				" — the broker filtered aborted data SERVER-SIDE (read_uncommitted must see ALL records, incl. aborted)"}
		}
	}
	for i := 0; i < scenarioTxnCommittedTotal; i++ {
		if k := string(committedTxnKey(i)); !seen[k] {
			return &txnCheckError{msg: "read_uncommitted read-back did not deliver committed record key=" + k +
				" — the read_uncommitted read-back dropped a committed record"}
		}
	}

	return nil
}

// scenarioTransactionsCheckLSOTripwire asserts the
// ListOffsets(latest,read_committed)==LSO tripwire on the RAW transcript's ListOffsets events
// (RelOffset is passed through as the ABSOLUTE returned offset by Normalize for ListOffsets events
// — isOffsetBearing excludes them from the base-relative rewrite — and CheckFreshTopicBaseOffsetZero
// guarantees the fresh-topic base is 0, so the absolute compare is valid; here we assert the
// RELATION per-target, independent of the cross-oracle diff, so even a benign absolute skew is
// caught). The read_committed events are the two Key=="latest-committed" (LSO) events, the
// read_uncommitted events the two Key=="latest" (HWM) events — the boundary labels driver_franzgo.go
// stamps (see scenarioTxnBoundaryLatest/scenarioTxnBoundaryLatestCommitted). In recorded order the
// FIRST of each pair is the OPEN-txn probe (LSO < HWM while txn 1 is open) and the SECOND is the
// final probe (LSO == HWM once every txn has decided).
//
// ErrCode != 0 ListOffsets events are SKIPPED (an errored boundary query carries the -1 "no offset"
// sentinel / a garbage offset, never a real LSO/HWM). Any "earliest" boundary events (there are
// none in ScenarioTransactions) are ignored — the tripwire is a latest-boundary invariant.
func scenarioTransactionsCheckLSOTripwire(events []Event) error {
	var rc, ru []Event // latest-committed (LSO) and latest (HWM) probe events, in recorded order
	for _, e := range events {
		if e.Action != string(ActionListOffsets) || e.ErrCode != 0 {
			continue
		}
		switch string(e.Key) {
		case scenarioTxnBoundaryLatestCommitted:
			rc = append(rc, e)
		case scenarioTxnBoundaryLatest:
			ru = append(ru, e)
		}
	}

	if len(rc) != 2 || len(ru) != 2 {
		return &txnCheckError{msg: "expected exactly 2 read_committed (\"" + scenarioTxnBoundaryLatestCommitted +
			"\") and 2 read_uncommitted (\"" + scenarioTxnBoundaryLatest + "\") latest ListOffsets events, got " +
			itoaSigned64(int64(len(rc))) + " and " + itoaSigned64(int64(len(ru)))}
	}

	// OPEN-txn probe (pair 0): LSO strictly below HWM while txn 1 is still open — the exact
	// false-latest-at-HWM EOS data-loss the tripwire exists to prevent.
	if rc[0].RelOffset >= ru[0].RelOffset {
		return &txnCheckError{msg: "open-txn ListOffsets(latest,read_committed)=LSO=" + itoaSigned64(rc[0].RelOffset) +
			" must be STRICTLY LESS THAN ListOffsets(latest)=HWM=" + itoaSigned64(ru[0].RelOffset) +
			" while txn 1 is open (the tripwire — a read_committed latest at the HWM would expose uncommitted records)"}
	}

	// FINAL probe (pair 1): every txn decided => LSO caught up to HWM.
	if rc[1].RelOffset != ru[1].RelOffset {
		return &txnCheckError{msg: "final ListOffsets(latest,read_committed)=LSO=" + itoaSigned64(rc[1].RelOffset) +
			" must EQUAL ListOffsets(latest)=HWM=" + itoaSigned64(ru[1].RelOffset) +
			" once every txn has decided (LSO must have advanced to the HWM)"}
	}

	return nil
}
