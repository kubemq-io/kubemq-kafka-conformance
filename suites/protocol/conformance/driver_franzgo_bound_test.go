package conformance

// driver_franzgo_bound_test.go — the kgo driver's hang bounds. TestConformanceTransactions hung
// for 30 minutes (go test's own timeout) against a single real Kafka broker whose transaction
// coordinator could never come up, although it runs under a 5-minute context: Driver.Run took no
// context and FranzGoDriver ran every step under context.Background(), so the transactional
// produce waited forever for a producer id. These tests stand a transaction coordinator that
// answers COORDINATOR_NOT_AVAILABLE (exactly what that broker answered) up in an in-process kfake
// and hold the driver to its bounds. No env gate, no containers.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// FindCoordinator CoordinatorType values.
const (
	findCoordinatorTypeGroup int8 = 0
	findCoordinatorTypeTxn   int8 = 1
)

// coordinatorErrorResponse answers every key of req with code, in the request's own version's
// shape (top-level fields up to v3, one entry per key from v4).
func coordinatorErrorResponse(req *kmsg.FindCoordinatorRequest, code int16) *kmsg.FindCoordinatorResponse {
	resp := req.ResponseKind().(*kmsg.FindCoordinatorResponse)
	if req.Version <= 3 {
		resp.ErrorCode = code
		resp.NodeID = -1
		return resp
	}
	for _, key := range req.CoordinatorKeys {
		c := kmsg.NewFindCoordinatorResponseCoordinator()
		c.Key = key
		c.NodeID = -1
		c.ErrorCode = code
		resp.Coordinators = append(resp.Coordinators, c)
	}
	return resp
}

// kfakeRefusingCoordinator starts an in-process kfake whose coordinator of coordinatorType answers
// code to the first `failures` FindCoordinator lookups (every lookup when failures < 0); other
// coordinator types are served normally. onRefuse, when non-nil, is called for each refusal (on
// kfake's request loop, in request order). It returns the target and a counter of the refusals.
func kfakeRefusingCoordinator(t *testing.T, coordinatorType int8, failures int, code int16, onRefuse func()) (*KfakeTarget, *atomic.Int64) {
	t.Helper()
	target, err := NewKfakeTarget()
	require.NoError(t, err, "start in-process kfake")
	t.Cleanup(func() { _ = target.Close() })

	refused := new(atomic.Int64)
	target.cluster.ControlKey(int16(kmsg.FindCoordinator), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		target.cluster.KeepControl()
		req := kreq.(*kmsg.FindCoordinatorRequest)
		if req.CoordinatorType != coordinatorType {
			return nil, nil, false
		}
		if failures >= 0 && refused.Load() >= int64(failures) {
			target.cluster.DropControl()
			return nil, nil, false
		}
		refused.Add(1)
		if onRefuse != nil {
			onRefuse()
		}
		return coordinatorErrorResponse(req, code), nil, true
	})
	return target, refused
}

// kfakeWithCoordinatorDown is kfakeRefusingCoordinator answering COORDINATOR_NOT_AVAILABLE(15) —
// what a single real Kafka broker answers while it cannot create the coordinator's internal topic
// (forever, when that topic's replication factor exceeds the broker count).
func kfakeWithCoordinatorDown(t *testing.T, coordinatorType int8, failures int) (*KfakeTarget, *atomic.Int64) {
	t.Helper()
	return kfakeRefusingCoordinator(t, coordinatorType, failures, kerr.CoordinatorNotAvailable.Code, nil)
}

// boundedRunResult is one watched driver run.
type boundedRunResult struct {
	transcript Transcript
	err        error
	took       time.Duration
}

// watchRun runs fn on its own goroutine and returns its result, or fails the test when fn is still
// running after watchdog — the symptom under test is a call that never returns, so the test must
// not block on it.
func watchRun(t *testing.T, watchdog time.Duration, what string, fn func() (Transcript, error)) boundedRunResult {
	t.Helper()
	done := make(chan boundedRunResult, 1)
	start := time.Now()
	go func() {
		tr, err := fn()
		done <- boundedRunResult{transcript: tr, err: err, took: time.Since(start)}
	}()
	select {
	case r := <-done:
		return r
	case <-time.After(watchdog):
		t.Fatalf("%s is still running after %s — a step ignores its bound and would hang until go test's timeout", what, watchdog)
		return boundedRunResult{}
	}
}

// TestFranzGoDriver_StepBound_TxnCoordinatorNeverReady: with no caller deadline at all (plain
// Run), every step is still bounded — the BeginTxn step fails at the per-step bound, and no later
// step runs.
func TestFranzGoDriver_StepBound_TxnCoordinatorNeverReady(t *testing.T) {
	broken, _ := kfakeWithCoordinatorDown(t, findCoordinatorTypeTxn, -1)

	const bound = 2 * time.Second
	d := &FranzGoDriver{stepTimeout: bound}
	r := watchRun(t, 20*time.Second, "FranzGoDriver.Run", func() (Transcript, error) {
		return d.Run(ScenarioTransactions(), broken)
	})

	require.Error(t, r.err)
	assert.Less(t, r.took, bound+5*time.Second, "the step must fail at its %s bound", bound)
	assert.Contains(t, r.err.Error(), "step 2 (BeginTxn)")
	assert.Contains(t, r.err.Error(), "did not complete within its 2s bound")
	assert.Contains(t, r.err.Error(), "COORDINATOR_NOT_AVAILABLE", "the error must carry the broker's last answer")
	assert.Empty(t, r.transcript.Events, "a failed run returns no partial transcript")
}

// TestFranzGoDriver_TxnCoordinatorLateReady_Succeeds: the wait for the producer id must not fail a
// coordinator that is merely slow — a fresh real broker answers COORDINATOR_NOT_AVAILABLE while it
// creates __transaction_state. Refuse the first lookups, then serve normally: the full scenario
// runs and BeginTxn records success.
func TestFranzGoDriver_TxnCoordinatorLateReady_Succeeds(t *testing.T) {
	slow, refused := kfakeWithCoordinatorDown(t, findCoordinatorTypeTxn, 3)

	d := &FranzGoDriver{stepTimeout: 30 * time.Second}
	r := watchRun(t, 60*time.Second, "FranzGoDriver.Run", func() (Transcript, error) {
		return d.Run(ScenarioTransactions(), slow)
	})

	require.NoError(t, r.err, "a coordinator that comes up within the bound must not fail the run")
	assert.EqualValues(t, 3, refused.Load(), "the first three lookups were refused")
	var begins int
	for _, e := range r.transcript.Events {
		if e.Action == string(ActionBeginTxn) {
			begins++
			assert.Zero(t, e.ErrCode, "BeginTxn must succeed once the coordinator is up")
		}
	}
	assert.Equal(t, 3, begins, "all three transactions began")
	require.NoError(t, scenarioTransactionsCheckClientFiltering(r.transcript.Events))
	require.NoError(t, scenarioTransactionsCheckLSOTripwire(r.transcript.Events))
}

// TestAwaitTxnProducerID_DefinitiveAnswerIsLeftToBeginTxn: the wait retries only answers that mean
// "the coordinator is still coming up". A definitive refusal (here TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
// must not be waited out: the wait returns at once and BeginTransaction reports that code, which
// runBeginTxn records as the step's ErrCode — diffable across oracles, as before the wait existed.
func TestAwaitTxnProducerID_DefinitiveAnswerIsLeftToBeginTxn(t *testing.T) {
	denied, refused := kfakeRefusingCoordinator(t, findCoordinatorTypeTxn, -1, kerr.TransactionalIDAuthorizationFailed.Code, nil)

	cl, err := kgo.NewClient(franzGoClientOpts(ScenarioTransactions(), denied.BootstrapAddr())...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	events, err := (&FranzGoDriver{}).runBeginTxn(ctx, cl)
	took := time.Since(start)

	require.NoError(t, err, "a definitive broker answer is a recorded error code, not a step failure")
	require.Len(t, events, 1)
	assert.Equal(t, kerr.TransactionalIDAuthorizationFailed.Code, events[0].ErrCode)
	assert.Less(t, took, 10*time.Second, "a definitive answer must not be retried for the whole bound")
	assert.Positive(t, refused.Load())
}

// TestFranzGoRunProduce_ContextEndIsStepFailure: a record that fails because the step's context
// ended was never answered by the broker. runProduce must fail the step, not record it as a Kafka
// error code (UNKNOWN_SERVER_ERROR) and carry on — which let every later record fail instantly
// and pushed the hang on to the next step.
func TestFranzGoRunProduce_ContextEndIsStepFailure(t *testing.T) {
	target, err := NewKfakeTarget()
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })

	cl, err := kgo.NewClient(franzGoClientOpts(ScenarioCore(), target.BootstrapAddr())...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	step := Step{Action: ActionProduce, Topic: "bound", Partition: 0, Records: []Record{
		{Key: []byte("k0"), Value: []byte("v0")},
		{Key: []byte("k1"), Value: []byte("v1")},
	}}
	events, err := (&FranzGoDriver{}).runProduce(ctx, cl, "bounded-produce", step)

	require.Error(t, err, "a produce whose context ended must fail the step (got events %+v)", events)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "never acknowledged")
	assert.Nil(t, events)
}

// openTxnOnKfake starts a kfake, builds the transactional scenario client on it and leaves one
// transaction open with one record produced in it — the state a CommitTxn/AbortTxn step ends.
func openTxnOnKfake(t *testing.T) (*KfakeTarget, *kgo.Client, string) {
	t.Helper()
	target, err := NewKfakeTarget()
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })

	cl, err := kgo.NewClient(franzGoClientOpts(ScenarioTransactions(), target.BootstrapAddr())...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const topic = "bounded-endtxn"
	d := &FranzGoDriver{}
	_, err = d.runCreateTopic(ctx, kadm.NewClient(cl), topic, 1)
	require.NoError(t, err)
	begin, err := d.runBeginTxn(ctx, cl)
	require.NoError(t, err)
	require.Zero(t, begin[0].ErrCode, "the transaction must begin")
	produced, err := d.runProduce(ctx, cl, topic, Step{Action: ActionProduce, Partition: 0, Records: []Record{{Key: []byte("k0"), Value: []byte("v0")}}})
	require.NoError(t, err)
	require.Zero(t, produced[0].ErrCode, "the transactional record must be acknowledged")
	return target, cl, topic
}

// TestFranzGoRunEndTxn_ContextEndIsStepFailure: an EndTxn cut short by the step's context was never
// answered by the broker. runEndTxn must fail the step — not record the context error as
// UNKNOWN_SERVER_ERROR (-1), an error code the broker never sent, and let the run carry on.
func TestFranzGoRunEndTxn_ContextEndIsStepFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  kgo.TransactionEndTry
	}{
		{"commit", kgo.TryCommit},
		{"abort", kgo.TryAbort},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, cl, topic := openTxnOnKfake(t)

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			events, err := (&FranzGoDriver{}).runEndTxn(ctx, cl, topic, tc.end)

			require.Error(t, err, "an EndTxn whose context ended must fail the step (got events %+v)", events)
			assert.ErrorIs(t, err, context.Canceled)
			assert.Contains(t, err.Error(), "never answered")
			assert.Nil(t, events)
		})
	}
}

// TestFranzGoDriver_StalledCommit_FailsTheCommitStep: a broker that does not answer EndTxn within
// the step bound must fail the run AT the commit step, naming it and its bound. Before the fix the
// commit step recorded ErrCode -1 and the run went on to the next transaction with a fresh bound.
func TestFranzGoDriver_StalledCommit_FailsTheCommitStep(t *testing.T) {
	target, err := NewKfakeTarget()
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })

	// Hold the first EndTxn unanswered until the test ends; later ones are served normally.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var stalled atomic.Bool
	target.cluster.ControlKey(int16(kmsg.EndTxn), func(kmsg.Request) (kmsg.Response, error, bool) {
		if stalled.CompareAndSwap(false, true) {
			target.cluster.KeepControl()
			target.cluster.SleepControl(func() { <-release })
			return nil, nil, false
		}
		target.cluster.DropControl()
		return nil, nil, false
	})

	const bound = 2 * time.Second
	d := &FranzGoDriver{stepTimeout: bound}
	r := watchRun(t, time.Minute, "FranzGoDriver.Run", func() (Transcript, error) {
		return d.Run(ScenarioTransactions(), target)
	})

	require.True(t, stalled.Load(), "the scenario must have reached its first commit")
	require.Error(t, r.err, "a commit the broker never answered must fail the run (transcript %+v)", r.transcript.Events)
	assert.Contains(t, r.err.Error(), "step 6 (CommitTxn)", "the error must name the commit step")
	assert.Contains(t, r.err.Error(), "did not complete within its 2s bound")
	assert.ErrorIs(t, r.err, context.DeadlineExceeded)
	assert.Less(t, r.took, bound+10*time.Second, "the run must end at the commit's bound")
}

// TestFranzGoRunContext_CallerCanceled_NeverSucceeds: a step whose context ended never stands, even
// when its handler returned without an error — a client call cut short by the context can surface
// as an ordinary error answer (kgo's EndTransaction reports OPERATION_NOT_ATTEMPTED when the context
// ends while it reloads the producer id), which the handler would record as the broker's. Here the
// caller has already given up and the step (an empty produce) never consults its context: the run
// must still fail, not return a transcript.
func TestFranzGoRunContext_CallerCanceled_NeverSucceeds(t *testing.T) {
	target, err := NewKfakeTarget()
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scenario := Scenario{Name: "caller-canceled", Steps: []Step{{Action: ActionProduce, Topic: "caller-canceled", Partition: 0}}}
	tr, err := NewFranzGoDriver().RunContext(ctx, scenario, target)

	require.Error(t, err, "a run whose caller has given up must not report success (transcript %+v)", tr)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "step 0 (Produce)")
	assert.Empty(t, tr.Events)
}
