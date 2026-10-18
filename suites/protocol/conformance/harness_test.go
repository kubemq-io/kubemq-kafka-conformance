package conformance

// harness_test.go — pure-logic unit tests for harness.go's additive, zero-value-safe fields:
// the un-pin addition (Step.NumPartitions) and the transactions addition
// (ActionBeginTxn/ActionCommitTxn/ActionAbortTxn, Step.ReadCommitted, ListOffsetsSpec.ReadCommitted,
// Scenario.TransactionalID). Runs under plain `go test -tags conformance` — no env gate, no
// containers.

import "testing"

// TestStep_NumPartitions_ZeroValueBackCompat proves harness.go's Step.NumPartitions addition is a
// true zero-value back-compat field: ScenarioCore()'s existing ActionCreateTopic step literal
// (scenario_core.go, unmodified — explicitly not changed) never sets
// NumPartitions, so it must read back as the zero value (0) — the exact "use the driver's
// 1-partition default" signal resolveCreateTopicPartitionCount (driver_franzgo_test.go) consumes.
func TestStep_NumPartitions_ZeroValueBackCompat(t *testing.T) {
	sc := ScenarioCore()
	require := func(cond bool, msg string) {
		if !cond {
			t.Fatal(msg)
		}
	}
	require(len(sc.Steps) > 0, "ScenarioCore must have at least one step")

	found := false
	for i, step := range sc.Steps {
		if step.Action != ActionCreateTopic {
			continue
		}
		found = true
		if step.NumPartitions != 0 {
			t.Errorf("ScenarioCore step %d (ActionCreateTopic): NumPartitions = %d, want 0 (zero-value back-compat — ScenarioCore stays 1-partition, unmodified by the un-pin)", i, step.NumPartitions)
		}
	}
	require(found, "ScenarioCore must contain exactly one ActionCreateTopic step")
}

// TestStep_NumPartitions_ExplicitValuePreserved proves the OTHER half: a Step literal that DOES
// set NumPartitions (as scenario_multipartition.go's ScenarioMultiPartition does) round-trips that
// value unchanged through the struct — trivial by construction, but documents the contract
// resolveCreateTopicPartitionCount depends on (a non-zero Step.NumPartitions is never silently
// reset before reaching the driver).
func TestStep_NumPartitions_ExplicitValuePreserved(t *testing.T) {
	step := Step{Action: ActionCreateTopic, Topic: "t", NumPartitions: 7}
	if step.NumPartitions != 7 {
		t.Errorf("Step.NumPartitions = %d, want 7", step.NumPartitions)
	}
}

// TestScenario_TransactionalID_ZeroValueBackCompat proves harness.go's
// Scenario.TransactionalID addition is a true zero-value back-compat field: every existing
// scenario literal (ScenarioCore, ScenarioMultiPartition) never sets TransactionalID, so it must
// read back as the zero value ("") — the exact "build a non-transactional client" signal
// driver_franzgo.go's Run gates its kgo.TransactionalID opt on (scenario.TransactionalID != "").
func TestScenario_TransactionalID_ZeroValueBackCompat(t *testing.T) {
	for _, sc := range []Scenario{ScenarioCore(), ScenarioMultiPartition()} {
		if sc.TransactionalID != "" {
			t.Errorf("Scenario %q: TransactionalID = %q, want \"\" (zero-value back-compat — existing scenarios stay non-transactional, unmodified by this addition)", sc.Name, sc.TransactionalID)
		}
	}
}

// TestScenario_TransactionalID_ExplicitValuePreserved proves the OTHER half: a Scenario literal
// that DOES set TransactionalID (as scenario_transactions.go's ScenarioTransactions will)
// round-trips that value unchanged through the struct — trivial by construction, but documents
// the contract driver_franzgo.go's Run depends on (a non-empty Scenario.TransactionalID is never
// silently reset before reaching the driver).
func TestScenario_TransactionalID_ExplicitValuePreserved(t *testing.T) {
	sc := Scenario{Name: "txn", TransactionalID: "kconf-txn-producer"}
	if sc.TransactionalID != "kconf-txn-producer" {
		t.Errorf("Scenario.TransactionalID = %q, want %q", sc.TransactionalID, "kconf-txn-producer")
	}
}

// TestStep_ReadCommitted_ZeroValueBackCompat proves harness.go's Step.ReadCommitted
// addition is a true zero-value back-compat field: no Step literal in any existing scenario
// (ScenarioCore, ScenarioMultiPartition) sets it, so every one must read back false (== today's
// read_uncommitted fetch, byte-unchanged) — the exact signal driver_franzgo.go's runFetchFrom
// gates its kgo.FetchIsolationLevel(kgo.ReadCommitted()) opt on.
func TestStep_ReadCommitted_ZeroValueBackCompat(t *testing.T) {
	for _, sc := range []Scenario{ScenarioCore(), ScenarioMultiPartition()} {
		for i, step := range sc.Steps {
			if step.ReadCommitted {
				t.Errorf("Scenario %q step %d (%s): ReadCommitted = true, want false (zero-value back-compat — existing scenarios stay read_uncommitted, unmodified by this addition)", sc.Name, i, step.Action)
			}
		}
	}
}

// TestStep_ReadCommitted_ExplicitValuePreserved proves the OTHER half: a Step literal that DOES
// set ReadCommitted (as scenario_transactions.go's ScenarioTransactions will, on its
// read_committed FetchFrom step) round-trips that value unchanged through the struct.
func TestStep_ReadCommitted_ExplicitValuePreserved(t *testing.T) {
	step := Step{Action: ActionFetchFrom, Topic: "t", ReadCommitted: true}
	if !step.ReadCommitted {
		t.Errorf("Step.ReadCommitted = %v, want true", step.ReadCommitted)
	}
}

// TestListOffsetsSpec_ReadCommitted_ZeroValueBackCompat proves harness.go's
// ListOffsetsSpec.ReadCommitted addition is a true zero-value back-compat field: no
// ActionListOffsets step's ListOffsetsSpec in any existing scenario sets it, so every one must
// read back false (== today's read_uncommitted ListEndOffsets query, byte-unchanged) — the exact
// signal driver_franzgo.go's runListOffsets gates its kadm.ListCommittedOffsets branch on.
func TestListOffsetsSpec_ReadCommitted_ZeroValueBackCompat(t *testing.T) {
	for _, sc := range []Scenario{ScenarioCore(), ScenarioMultiPartition()} {
		for i, step := range sc.Steps {
			if step.Action != ActionListOffsets {
				continue
			}
			if step.ListOffsetsSpec.ReadCommitted {
				t.Errorf("Scenario %q step %d (ActionListOffsets): ListOffsetsSpec.ReadCommitted = true, want false (zero-value back-compat — existing scenarios stay read_uncommitted, unmodified by this addition)", sc.Name, i)
			}
		}
	}
}

// TestListOffsetsSpec_ReadCommitted_ExplicitValuePreserved proves the OTHER half: a
// ListOffsetsSpec literal that DOES set ReadCommitted (as scenario_transactions.go's
// ScenarioTransactions will, on its LSO-tripwire probes) round-trips that value unchanged
// through the struct, independently of Earliest/Latest.
func TestListOffsetsSpec_ReadCommitted_ExplicitValuePreserved(t *testing.T) {
	spec := ListOffsetsSpec{Latest: true, ReadCommitted: true}
	if !spec.Latest {
		t.Errorf("ListOffsetsSpec.Latest = %v, want true", spec.Latest)
	}
	if !spec.ReadCommitted {
		t.Errorf("ListOffsetsSpec.ReadCommitted = %v, want true", spec.ReadCommitted)
	}
	if spec.Earliest {
		t.Errorf("ListOffsetsSpec.Earliest = %v, want false (unset)", spec.Earliest)
	}
}

// TestStepAction_TxnConstants_StringValues locks the StepAction constants' underlying
// string values. These strings flow into Event.Action (driver_franzgo.go's runBeginTxn/runEndTxn
// record them via string(ActionBeginTxn) etc. — see harness.go's Event.Action doc) and are what
// scenario_transactions.go's scenarioTransactionsCheckLSOTripwire/CompareTranscripts ultimately
// diff cross-oracle, so an accidental rename here would silently break both the transcript
// dumps and any string-literal assertion downstream without this test catching it first.
func TestStepAction_TxnConstants_StringValues(t *testing.T) {
	cases := []struct {
		action StepAction
		want   string
	}{
		{ActionBeginTxn, "BeginTxn"},
		{ActionCommitTxn, "CommitTxn"},
		{ActionAbortTxn, "AbortTxn"},
	}
	for _, c := range cases {
		if string(c.action) != c.want {
			t.Errorf("StepAction %v = %q, want %q", c.action, string(c.action), c.want)
		}
	}
}

// TestStepAction_TxnConstants_Distinct proves the three new StepActions (and the pre-existing
// ones they sit alongside) are pairwise distinct — a copy/paste regression that aliased e.g.
// ActionAbortTxn to ActionCommitTxn's string would silently corrupt scenarioTransactionsCheckLSOTripwire's
// event classification without tripping a compile error (StepAction is a plain string type).
func TestStepAction_TxnConstants_Distinct(t *testing.T) {
	all := []StepAction{
		ActionCreateTopic, ActionProduce, ActionFetchFrom, ActionListOffsets, ActionMetadata,
		ActionBeginTxn, ActionCommitTxn, ActionAbortTxn,
	}
	seen := make(map[StepAction]int, len(all))
	for i, a := range all {
		if j, ok := seen[a]; ok {
			t.Errorf("StepAction %q used at both index %d and %d — must be pairwise distinct", a, j, i)
			continue
		}
		seen[a] = i
	}
}
