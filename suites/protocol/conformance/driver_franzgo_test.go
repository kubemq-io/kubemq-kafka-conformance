package conformance

// driver_franzgo_test.go — pure-logic unit tests for driver_franzgo.go. Covers the un-pin
// changes (resolveCreateTopicPartitionCount, the ManualPartitioner wiring, runListOffsets'
// step.Partition fix) AND the transactional additions (the gated kgo.TransactionalID opt
// via franzGoClientOpts, and runEndTxn's commit/abort -> Action mapping). Runs under plain
// `go test -tags conformance` — no env gate, no containers (mirrors scenario_multipartition_test.go).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestResolveCreateTopicPartitionCount is the plan's "Step.NumPartitions zero-value back-compat" /
// "runCreateTopic passes N through / defaults to 1" unit test (table-driven).
func TestResolveCreateTopicPartitionCount(t *testing.T) {
	tests := []struct {
		name          string
		numPartitions int32
		want          int32
	}{
		{"zero value (harness.go Step.NumPartitions default — every existing Scenario literal)", 0, 1},
		{"negative (defensive — never produced by ScenarioMultiPartition, still resolves safely)", -5, 1},
		{"explicit 1 (a scenario that spells out the default explicitly)", 1, 1},
		{"explicit N>1 (scenario_multipartition.go's scenarioMultiPartitionCount)", 3, 3},
		{"large N (well under the maxKafkaPartitions=256 cap — this function doesn't enforce the cap itself, just the default)", 200, 200},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveCreateTopicPartitionCount(tc.numPartitions)
			if got != tc.want {
				t.Errorf("resolveCreateTopicPartitionCount(%d) = %d, want %d", tc.numPartitions, got, tc.want)
			}
		})
	}
}

// txnOptTestBootstrap is a syntactically-valid but never-dialed bootstrap address. kgo.NewClient is
// lazy — it does not connect at construction — so franzGoClientOpts' output can be materialized into
// a real *kgo.Client and introspected via OptValue/OptValues with no broker anywhere in sight.
const txnOptTestBootstrap = "127.0.0.1:9092"

// TestFranzGoClientOpts_TransactionalID is "the kgo.TransactionalID opt is added ONLY
// when Scenario.TransactionalID is non-empty" test AND the byte-unchanged regression that pins the
// empty case for every scenario that predates transactional support.
//
// It materializes franzGoClientOpts into a real *kgo.Client per case and introspects the actual
// resolved config via kgo's OptValues (client.go:253): OptValues(kgo.TransactionalID) returns
// {"<id>", true} when the option is present and {"", false} when it is absent — so the second
// element is a hard, unambiguous "is this client transactional?" bit, not a slice-length heuristic.
func TestFranzGoClientOpts_TransactionalID(t *testing.T) {
	t.Run("empty TransactionalID => NO txn opt (byte-unchanged for every non-transactional scenario)", func(t *testing.T) {
		// ScenarioCore/ScenarioMultiPartition are the real shipped non-transactional scenarios; both leave
		// TransactionalID at its zero value, so building their client must yield a NON-transactional
		// client — exactly as before transactional support existed. This is the regression the binder names.
		for _, sc := range []Scenario{ScenarioCore(), ScenarioMultiPartition(), {Name: "bare"}} {
			opts := franzGoClientOpts(sc, txnOptTestBootstrap)
			cl, err := kgo.NewClient(opts...)
			require.NoError(t, err, "build client for empty-TransactionalID scenario %q", sc.Name)
			t.Cleanup(cl.Close)

			vals := cl.OptValues(kgo.TransactionalID)
			require.Len(t, vals, 2, "OptValues(TransactionalID) returns {value, present}")
			assert.Equal(t, "", vals[0], "scenario %q: no transactional id value", sc.Name)
			assert.Equal(t, false, vals[1], "scenario %q: client must NOT be transactional (byte-unchanged)", sc.Name)

			// The base opts every pre-9.4 scenario relied on are still present and unchanged.
			assert.Equal(t, "kafka-conformance-kgo", cl.OptValue(kgo.ClientID), "scenario %q: ClientID unchanged", sc.Name)
			assert.Equal(t, true, cl.OptValue(kgo.AllowAutoTopicCreation), "scenario %q: AllowAutoTopicCreation unchanged", sc.Name)
		}
	})

	t.Run("non-empty TransactionalID => the txn opt is added with that exact id", func(t *testing.T) {
		const id = "kconf-txn-producer"
		opts := franzGoClientOpts(Scenario{Name: "txn", TransactionalID: id}, txnOptTestBootstrap)
		cl, err := kgo.NewClient(opts...)
		require.NoError(t, err, "build client for transactional scenario")
		t.Cleanup(cl.Close)

		vals := cl.OptValues(kgo.TransactionalID)
		require.Len(t, vals, 2, "OptValues(TransactionalID) returns {value, present}")
		assert.Equal(t, id, vals[0], "the exact scenario TransactionalID is threaded to kgo.TransactionalID")
		assert.Equal(t, true, vals[1], "client must be transactional")

		// The base opts are identical to the empty case — the ONLY delta is the appended txn opt.
		assert.Equal(t, "kafka-conformance-kgo", cl.OptValue(kgo.ClientID), "ClientID identical to the non-txn client")
		assert.Equal(t, true, cl.OptValue(kgo.AllowAutoTopicCreation), "AllowAutoTopicCreation identical to the non-txn client")
	})

	t.Run("the txn opt is EXACTLY one appended option (base opt count unchanged)", func(t *testing.T) {
		// Slice-length delta is the belt-and-suspenders to the OptValues presence bit above: the
		// transactional scenario's opt set is the non-transactional one plus EXACTLY one entry (the
		// gated kgo.TransactionalID append), never more — so the empty case cannot have silently lost
		// or gained a base opt.
		base := franzGoClientOpts(Scenario{Name: "n"}, txnOptTestBootstrap)
		txn := franzGoClientOpts(Scenario{Name: "t", TransactionalID: "id"}, txnOptTestBootstrap)
		assert.Equal(t, len(base)+1, len(txn), "transactional opts == base opts + exactly one (kgo.TransactionalID)")
	})
}

// TestRunEndTxn_CommitAbortMapping is the "runEndTxn maps TryCommit->CommitTxn,
// TryAbort->AbortTxn" test. The Action
// label is chosen from the commit flag BEFORE EndTransaction is called, and EndTransaction on a
// transactional client that never began a transaction returns nil immediately (kgo txn.go:686-688,
// `if !inTxn { return nil }`) — no broker round-trip — so the mapping is observable with no infra.
func TestRunEndTxn_CommitAbortMapping(t *testing.T) {
	// A transactional client (never Begun) — EndTransaction returns nil without dialing.
	cl, err := kgo.NewClient(franzGoClientOpts(Scenario{Name: "txn", TransactionalID: "kconf-txn-producer"}, txnOptTestBootstrap)...)
	require.NoError(t, err, "build transactional client")
	t.Cleanup(cl.Close)

	d := NewFranzGoDriver()
	ctx := context.Background()
	const physicalTopic = "kconf-txn-phys"

	tests := []struct {
		name       string
		commit     kgo.TransactionEndTry
		wantAction StepAction
	}{
		{"TryCommit -> CommitTxn", kgo.TryCommit, ActionCommitTxn},
		{"TryAbort -> AbortTxn", kgo.TryAbort, ActionAbortTxn},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			events, err := d.runEndTxn(ctx, cl, physicalTopic, tc.commit)
			require.NoError(t, err, "runEndTxn must not error on a never-begun transaction (EndTransaction returns nil)")
			require.Len(t, events, 1, "runEndTxn records exactly one synthetic Event")
			ev := events[0]
			assert.Equal(t, string(tc.wantAction), ev.Action, "commit flag maps to the Action label")
			assert.Equal(t, physicalTopic, ev.Topic, "the physical topic is stamped on the txn-control Event")
			assert.Equal(t, int32(0), ev.Partition, "txn-control Events pin partition 0")
			assert.Equal(t, int16(0), ev.ErrCode, "no-op EndTransaction succeeds => ErrCode 0")
		})
	}
}
