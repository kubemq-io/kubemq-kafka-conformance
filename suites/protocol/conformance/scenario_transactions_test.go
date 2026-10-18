package conformance

// scenario_transactions_test.go — pure-logic unit tests for scenario_transactions.go (EOS
// conformance). Runs under plain `go test -tags conformance` — no env gate, no containers
// (mirrors scenario_multipartition_test.go/normalize_test.go/diff_test.go's "local development
// without run.sh"). Covers the scenario shape (step count/order, record counts + prefix-distinct
// keys, FetchCounts, TransactionalID) and the two pure invariant checkers (incl. an
// error-event-in-stream robustness case for the filtering checker).

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// txnFetchEvent builds a successful FetchFrom Event for the client-filtering checker's tables.
func txnFetchEvent(key []byte, relOffset int64) Event {
	return Event{
		Action:    string(ActionFetchFrom),
		Topic:     scenarioTxnName,
		Partition: 0,
		Key:       key,
		RelOffset: relOffset,
	}
}

// txnListOffsetEvent builds a successful ListOffsets Event (boundary label in Key) for the
// LSO-tripwire checker's tables.
func txnListOffsetEvent(boundary string, relOffset int64) Event {
	return Event{
		Action:    string(ActionListOffsets),
		Topic:     scenarioTxnName,
		Partition: 0,
		Key:       []byte(boundary),
		RelOffset: relOffset,
	}
}

// happyClientFilteringEvents builds the transcript FetchFrom-event stream ScenarioTransactions
// produces on a clean run: the 9-record read_committed read-back (c-00..c-08) followed by the
// 13-record read_uncommitted read-back (all committed AND aborted, in offset order). Individual
// tests mutate a copy to inject regressions.
func happyClientFilteringEvents() []Event {
	evs := make([]Event, 0, scenarioTxnCommittedTotal+scenarioTxnDataTotal)

	// read_committed read-back: c-00..c-08 (the 9 committed records; a-* filtered client-side).
	for i := 0; i < scenarioTxnCommittedTotal; i++ {
		evs = append(evs, txnFetchEvent(committedTxnKey(i), int64(i)))
	}

	// read_uncommitted read-back: all 13 data records, offset order c-00..c-05, a-00..a-03,
	// c-06..c-08 (offsets skip the COMMIT/ABORT markers, but the checker inspects Key, not offset).
	for i := 0; i < scenarioTxnCommitted1Count; i++ {
		evs = append(evs, txnFetchEvent(committedTxnKey(i), int64(i)))
	}
	for i := 0; i < scenarioTxnAbortedCount; i++ {
		evs = append(evs, txnFetchEvent(abortedTxnKey(i), int64(scenarioTxnCommitted1Count+i)))
	}
	for i := 0; i < scenarioTxnCommitted2Count; i++ {
		idx := scenarioTxnCommitted1Count + i
		evs = append(evs, txnFetchEvent(committedTxnKey(idx), int64(idx)))
	}
	return evs
}

// cloneEvents returns a shallow copy of evs so a test can mutate positions without disturbing the
// shared happy-path builder.
func cloneEvents(evs []Event) []Event {
	out := make([]Event, len(evs))
	copy(out, evs)
	return out
}

func TestScenarioTransactions_Shape(t *testing.T) {
	sc := ScenarioTransactions()

	assert.Equal(t, scenarioTxnName, sc.Name)
	assert.Equal(t, scenarioTxnTransactionalID, sc.TransactionalID,
		"Scenario.TransactionalID must be set so driver_franzgo.go builds a transactional producer")

	// Exact ordered action sequence — locks the committed / open-probe / commit / aborted /
	// committed / RC-readback / RU-readback / final-probe shape the tripwires depend on.
	wantActions := []StepAction{
		ActionCreateTopic,
		ActionMetadata,
		ActionBeginTxn,
		ActionProduce,     // committed batch 1
		ActionListOffsets, // open-txn LSO probe (read_committed)
		ActionListOffsets, // open-txn HWM probe (read_uncommitted)
		ActionCommitTxn,
		ActionBeginTxn,
		ActionProduce, // aborted batch
		ActionAbortTxn,
		ActionBeginTxn,
		ActionProduce, // committed batch 2
		ActionCommitTxn,
		ActionFetchFrom,   // read_committed read-back
		ActionFetchFrom,   // read_uncommitted read-back
		ActionListOffsets, // final LSO probe (read_committed)
		ActionListOffsets, // final HWM probe (read_uncommitted)
	}
	gotActions := make([]StepAction, len(sc.Steps))
	for i, step := range sc.Steps {
		gotActions[i] = step.Action
	}
	require.Equal(t, wantActions, gotActions, "ScenarioTransactions step action sequence")

	// Single-partition pin (like ScenarioCore): every step targets partition 0, CreateTopic uses
	// the 1-partition default (NumPartitions zero-value).
	for i, step := range sc.Steps {
		assert.EqualValues(t, 0, step.Partition, "step %d (%s): partition must be 0 (1-partition pin)", i, step.Action)
	}
	assert.EqualValues(t, 0, sc.Steps[0].NumPartitions, "CreateTopic must use the 1-partition default (NumPartitions zero-value)")

	// Produce steps: committed1 (6, c-00..c-05), aborted (4, a-00..a-03), committed2 (3, c-06..c-08).
	var produce []Step
	for _, step := range sc.Steps {
		if step.Action == ActionProduce {
			produce = append(produce, step)
		}
	}
	require.Len(t, produce, 3, "ScenarioTransactions must have exactly 3 Produce steps (committed/aborted/committed)")
	assert.Len(t, produce[0].Records, scenarioTxnCommitted1Count, "first committed batch record count")
	assert.Len(t, produce[1].Records, scenarioTxnAbortedCount, "aborted batch record count")
	assert.Len(t, produce[2].Records, scenarioTxnCommitted2Count, "second committed batch record count")

	// Prefix-distinct keys: committed batches carry c-*, the aborted batch carries a-*.
	assertBatchPrefix(t, produce[0].Records, scenarioTxnCommittedKeyPrefix, "committed batch 1")
	assertBatchPrefix(t, produce[1].Records, scenarioTxnAbortedKeyPrefix, "aborted batch")
	assertBatchPrefix(t, produce[2].Records, scenarioTxnCommittedKeyPrefix, "committed batch 2")

	// The exact committed key set across both committed batches is c-00..c-08 (contiguous idx),
	// disjoint from the aborted key set a-00..a-03.
	committedKeys := map[string]bool{}
	for _, r := range produce[0].Records {
		committedKeys[string(r.Key)] = true
	}
	for _, r := range produce[2].Records {
		committedKeys[string(r.Key)] = true
	}
	for i := 0; i < scenarioTxnCommittedTotal; i++ {
		assert.Truef(t, committedKeys[string(committedTxnKey(i))], "committed key %s must be produced", committedTxnKey(i))
	}
	for _, r := range produce[1].Records {
		assert.Falsef(t, committedKeys[string(r.Key)], "aborted key %s must not collide with any committed key", r.Key)
	}

	// FetchFrom steps: RC read-back (FetchCount=9, ReadCommitted) then RU read-back (FetchCount=13,
	// read_uncommitted), both from offset 0.
	var fetch []Step
	for _, step := range sc.Steps {
		if step.Action == ActionFetchFrom {
			fetch = append(fetch, step)
		}
	}
	require.Len(t, fetch, 2, "ScenarioTransactions must have exactly 2 FetchFrom steps (RC then RU read-back)")
	assert.EqualValues(t, 0, fetch[0].FetchOffset, "RC read-back must start at offset 0")
	assert.Equal(t, scenarioTxnCommittedTotal, fetch[0].FetchCount, "RC read-back FetchCount == committed total (9)")
	assert.True(t, fetch[0].ReadCommitted, "first FetchFrom must be read_committed")
	assert.EqualValues(t, 0, fetch[1].FetchOffset, "RU read-back must start at offset 0")
	assert.Equal(t, scenarioTxnDataTotal, fetch[1].FetchCount, "RU read-back FetchCount == data total (13)")
	assert.False(t, fetch[1].ReadCommitted, "second FetchFrom must be read_uncommitted (zero-value)")

	// ListOffsets steps: 2 read_committed (LSO) + 2 read_uncommitted (HWM), all Latest, none Earliest.
	var rcProbes, ruProbes int
	for _, step := range sc.Steps {
		if step.Action != ActionListOffsets {
			continue
		}
		assert.True(t, step.ListOffsetsSpec.Latest, "every ListOffsets probe queries the Latest boundary")
		assert.False(t, step.ListOffsetsSpec.Earliest, "ScenarioTransactions issues no Earliest boundary query")
		if step.ListOffsetsSpec.ReadCommitted {
			rcProbes++
		} else {
			ruProbes++
		}
	}
	assert.Equal(t, 2, rcProbes, "exactly 2 read_committed (LSO) ListOffsets probes")
	assert.Equal(t, 2, ruProbes, "exactly 2 read_uncommitted (HWM) ListOffsets probes")

	// Open-probe placement: the FIRST ListOffsets pair must sit AFTER the first Produce and BEFORE
	// the first CommitTxn — i.e. issued while txn 1 is still open (the LSO<HWM tripwire depends on
	// this ordering; a probe moved after the commit would see LSO==HWM and silently defeat the tripwire).
	firstProduce := indexOfAction(sc.Steps, ActionProduce)
	firstCommit := indexOfAction(sc.Steps, ActionCommitTxn)
	firstListOffsets := indexOfAction(sc.Steps, ActionListOffsets)
	require.GreaterOrEqual(t, firstProduce, 0, "must contain a Produce step")
	require.GreaterOrEqual(t, firstCommit, 0, "must contain a CommitTxn step")
	require.GreaterOrEqual(t, firstListOffsets, 0, "must contain a ListOffsets step")
	assert.Greater(t, firstListOffsets, firstProduce, "open-txn ListOffsets probe must follow the first Produce")
	assert.Less(t, firstListOffsets, firstCommit, "open-txn ListOffsets probe must precede the first CommitTxn (txn still open)")
}

// assertBatchPrefix asserts every record in batch carries the given key prefix.
func assertBatchPrefix(t *testing.T, batch []Record, prefix, label string) {
	t.Helper()
	for i, r := range batch {
		assert.Truef(t, hasKeyPrefix(r.Key, prefix), "%s record %d key=%q must have prefix %q", label, i, r.Key, prefix)
	}
}

// indexOfAction returns the index of the first step with the given action, or -1.
func indexOfAction(steps []Step, action StepAction) int {
	for i, s := range steps {
		if s.Action == action {
			return i
		}
	}
	return -1
}

func TestScenarioTransactions_RecordBuilders(t *testing.T) {
	c1 := scenarioTxnCommitted1Records()
	require.Len(t, c1, scenarioTxnCommitted1Count)
	for i, r := range c1 {
		assert.Equal(t, string(committedTxnKey(i)), string(r.Key), "committed1 record %d key", i)
		assert.NotEmpty(t, r.Value, "committed1 record %d must carry a value", i)
	}

	ab := scenarioTxnAbortedRecords()
	require.Len(t, ab, scenarioTxnAbortedCount)
	for i, r := range ab {
		assert.Equal(t, string(abortedTxnKey(i)), string(r.Key), "aborted record %d key", i)
	}

	c2 := scenarioTxnCommitted2Records()
	require.Len(t, c2, scenarioTxnCommitted2Count)
	for i, r := range c2 {
		idx := scenarioTxnCommitted1Count + i
		assert.Equal(t, string(committedTxnKey(idx)), string(r.Key), "committed2 record %d key", i)
	}

	// Committed and aborted key prefixes are disjoint (the positional filtering checker relies on it).
	assert.NotEqual(t, scenarioTxnCommittedKeyPrefix, scenarioTxnAbortedKeyPrefix)
	assert.False(t, hasKeyPrefix(committedTxnKey(0), scenarioTxnAbortedKeyPrefix))
	assert.False(t, hasKeyPrefix(abortedTxnKey(0), scenarioTxnCommittedKeyPrefix))

	// The derived totals are internally consistent.
	assert.Equal(t, scenarioTxnCommitted1Count+scenarioTxnCommitted2Count, scenarioTxnCommittedTotal)
	assert.Equal(t, scenarioTxnCommittedTotal+scenarioTxnAbortedCount, scenarioTxnDataTotal)
	assert.Equal(t, scenarioTxnCommitted1Count, scenarioTxnOpenProbeHWM, "open-probe HWM == first committed batch size")
	assert.Less(t, scenarioTxnOpenProbeLSO, scenarioTxnOpenProbeHWM, "open-probe LSO must be below HWM by construction")
}

func TestHasKeyPrefix(t *testing.T) {
	tests := []struct {
		key    string
		prefix string
		want   bool
	}{
		{"c-00", "c-", true},
		{"a-03", "a-", true},
		{"c-00", "a-", false},
		{"a-03", "c-", false},
		{"c", "c-", false}, // shorter than prefix
		{"", "c-", false},
		{"c-", "c-", true}, // exactly the prefix
	}
	for _, tc := range tests {
		t.Run(tc.key+"/"+tc.prefix, func(t *testing.T) {
			assert.Equal(t, tc.want, hasKeyPrefix([]byte(tc.key), tc.prefix))
		})
	}
}

func TestScenarioTransactionsCheckClientFiltering(t *testing.T) {
	// "aborted smuggled into RC": replace the 4th RC event's key with an aborted key.
	smuggled := cloneEvents(happyClientFilteringEvents())
	smuggled[3] = txnFetchEvent(abortedTxnKey(0), 3)

	// "aborted missing from RU": replace the RU a-00 event (at RC total offset) with a committed dup
	// so the count stays 22 but a-00 is absent from the RU set.
	missingAborted := cloneEvents(happyClientFilteringEvents())
	// RU segment starts at scenarioTxnCommittedTotal; a-00 is the (scenarioTxnCommitted1Count)-th RU
	// event (after the 6 committed-1 records). Overwrite it with a committed dup.
	missingAborted[scenarioTxnCommittedTotal+scenarioTxnCommitted1Count] = txnFetchEvent(committedTxnKey(0), 99)

	// "non-committed key smuggled into RC": replace an RC event with an unrelated prefix.
	unexpectedRC := cloneEvents(happyClientFilteringEvents())
	unexpectedRC[0] = txnFetchEvent([]byte("x-00"), 0)

	// "error event interspersed": inject an ErrCode!=0 FetchFrom event mid-RC — it must
	// be SKIPPED and never shift the positional RC/RU boundary, so the happy split still holds.
	withErrorEvent := func() []Event {
		base := happyClientFilteringEvents()
		errEvt := Event{Action: string(ActionFetchFrom), Topic: scenarioTxnName, Partition: 0, ErrCode: 6}
		out := make([]Event, 0, len(base)+1)
		out = append(out, base[:4]...)
		out = append(out, errEvt)
		out = append(out, base[4:]...)
		return out
	}()

	// "non-FetchFrom events ignored": Produce/ListOffsets events interleaved must not participate.
	withNoise := func() []Event {
		base := happyClientFilteringEvents()
		out := make([]Event, 0, len(base)+2)
		out = append(out, Event{Action: string(ActionProduce), Topic: scenarioTxnName, Key: abortedTxnKey(0), RelOffset: 7})
		out = append(out, base[:5]...)
		out = append(out, Event{Action: string(ActionListOffsets), Topic: scenarioTxnName, Key: []byte("latest"), RelOffset: 6})
		out = append(out, base[5:]...)
		return out
	}()

	// "too few delivered": drop the last RU record.
	tooFew := happyClientFilteringEvents()
	tooFew = tooFew[:len(tooFew)-1]

	tests := []struct {
		name    string
		events  []Event
		wantErr bool
	}{
		{"happy path (9 committed RC, 13 all RU)", happyClientFilteringEvents(), false},
		{"aborted record smuggled into read_committed read-back", smuggled, true},
		{"aborted record missing from read_uncommitted read-back (server-side filter)", missingAborted, true},
		{"unexpected non-committed key in read_committed read-back", unexpectedRC, true},
		{"error FetchFrom event interspersed is skipped (no desync)", withErrorEvent, false},
		{"non-FetchFrom events ignored", withNoise, false},
		{"too few delivered records", tooFew, true},
		{"empty input", nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := scenarioTransactionsCheckClientFiltering(tc.events)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestScenarioTransactionsCheckLSOTripwire(t *testing.T) {
	// Happy: open probe LSO(0) < HWM(6); final probe LSO(16) == HWM(16). Recorded order is
	// open-RC, open-RU, final-RC, final-RU (as ScenarioTransactions emits them).
	happy := []Event{
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, scenarioTxnOpenProbeLSO), // 0
		txnListOffsetEvent(scenarioTxnBoundaryLatest, scenarioTxnOpenProbeHWM),          // 6
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 16),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 16),
	}

	// Open probe LSO == HWM (not strictly below) — the tripwire must fire (>=).
	openEqual := []Event{
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 6),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 6),
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 16),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 16),
	}

	// Open probe LSO above HWM — must fire.
	openAbove := []Event{
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 7),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 6),
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 16),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 16),
	}

	// Final probe LSO != HWM (LSO never caught up) — must fire.
	finalUnequal := []Event{
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 0),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 6),
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 15),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 16),
	}

	// Wrong count: only one RC probe.
	wrongCount := []Event{
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 0),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 6),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 16),
	}

	// Errored latest-committed events are skipped, so the 4 clean probes still classify correctly.
	withErroredProbe := []Event{
		{Action: string(ActionListOffsets), Topic: scenarioTxnName, Key: []byte(scenarioTxnBoundaryLatestCommitted), RelOffset: -1, ErrCode: 3},
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 0),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 6),
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 16),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 16),
	}

	// Earliest boundary events (and non-ListOffsets events) are ignored.
	withEarliestAndNoise := []Event{
		txnListOffsetEvent("earliest", 0),
		{Action: string(ActionFetchFrom), Topic: scenarioTxnName, Key: committedTxnKey(0), RelOffset: 0},
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 0),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 6),
		txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 16),
		txnListOffsetEvent(scenarioTxnBoundaryLatest, 16),
	}

	tests := []struct {
		name    string
		events  []Event
		wantErr bool
	}{
		{"happy 0/6/16/16", happy, false},
		{"open probe LSO == HWM (not strictly below)", openEqual, true},
		{"open probe LSO above HWM", openAbove, true},
		{"final probe LSO != HWM", finalUnequal, true},
		{"wrong probe count", wrongCount, true},
		{"errored probe skipped, clean probes still classify", withErroredProbe, false},
		{"earliest + non-ListOffsets noise ignored", withEarliestAndNoise, false},
		{"empty input", nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := scenarioTransactionsCheckLSOTripwire(tc.events)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestTransactionsOracleInvariantProblems pins TestConformanceTransactions' per-target invariant
// attribution: a clean oracle pair yields no problems, and a violation on ONE oracle is reported
// against that oracle by name (so a broken real-Kafka control is never blamed on kfake or hidden).
func TestTransactionsOracleInvariantProblems(t *testing.T) {
	happy := func(target string) Transcript {
		evs := cloneEvents(happyClientFilteringEvents())
		evs = append(evs,
			txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, scenarioTxnOpenProbeLSO),
			txnListOffsetEvent(scenarioTxnBoundaryLatest, scenarioTxnOpenProbeHWM),
			txnListOffsetEvent(scenarioTxnBoundaryLatestCommitted, 16),
			txnListOffsetEvent(scenarioTxnBoundaryLatest, 16),
		)
		return Transcript{Driver: "kgo", Target: target, LogicalTopic: scenarioTxnName, Events: evs}
	}
	assert.Empty(t, transactionsOracleInvariantProblems(happy("real-kafka"), happy("kfake")))

	// real-kafka delivers an aborted record under read_committed: attributed to real-kafka only.
	leaky := happy("real-kafka")
	leaky.Events[0].Key = abortedTxnKey(0)
	problems := transactionsOracleInvariantProblems(leaky, happy("kfake"))
	require.Len(t, problems, 1)
	assert.Contains(t, problems[0], "real-kafka")
	assert.Contains(t, problems[0], "client-filtering")

	// kfake reports LSO == HWM while a transaction is open: attributed to kfake only.
	stale := happy("kfake")
	n := len(stale.Events)
	stale.Events[n-4].RelOffset = scenarioTxnOpenProbeHWM
	problems = transactionsOracleInvariantProblems(happy("real-kafka"), stale)
	require.Len(t, problems, 1)
	assert.Contains(t, problems[0], "kfake")
	assert.Contains(t, problems[0], "LSO")

	// A transcript with no fetches at all is a failure, never an empty pass.
	empty := Transcript{Target: "real-kafka"}
	problems = transactionsOracleInvariantProblems(empty, happy("kfake"))
	assert.NotEmpty(t, problems)
	assert.Contains(t, problems[0], "baseOffset==0")
}
