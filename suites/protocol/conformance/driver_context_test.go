package conformance

// driver_context_test.go — a caller's deadline must reach every in-process driver. Driver.Run takes
// no context, so the conformance tests' overall deadline used to reach only the kgo driver: the
// sarama and segmentio smoke drivers ran on their own fixed budgets (the segmentio group arm alone
// can wait a minute for a group coordinator before its ten rounds), and the kfake oracle leg and
// every smoke leg called plain Run. These tests pin runDriver's routing and hold each in-process
// driver, and the harness legs that call them, to the caller's context. In-process kfake, no env
// gate, no containers.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runDriverCtxKey tags the caller's context so a stub can prove it received that exact context.
type runDriverCtxKey struct{}

// plainStubDriver is a Driver without RunContext.
type plainStubDriver struct{ runs int }

func (d *plainStubDriver) Run(Scenario, Target) (Transcript, error) {
	d.runs++
	return Transcript{Driver: "plain-stub"}, nil
}
func (d *plainStubDriver) Name() string     { return "plain-stub" }
func (d *plainStubDriver) Kind() DriverKind { return DriverKindSmoke }

// contextStubDriver is a ContextDriver that records the context it was given.
type contextStubDriver struct {
	runs, contextRuns int
	got               context.Context
}

func (d *contextStubDriver) Run(Scenario, Target) (Transcript, error) {
	d.runs++
	return Transcript{Driver: "context-stub"}, nil
}
func (d *contextStubDriver) RunContext(ctx context.Context, _ Scenario, _ Target) (Transcript, error) {
	d.contextRuns++
	d.got = ctx
	return Transcript{Driver: "context-stub"}, nil
}
func (d *contextStubDriver) Name() string     { return "context-stub" }
func (d *contextStubDriver) Kind() DriverKind { return DriverKindSmoke }

// stubTarget is a Target nothing dials.
type stubTarget struct{}

func (stubTarget) BootstrapAddr() string { return "127.0.0.1:1" }
func (stubTarget) Name() string          { return "stub" }
func (stubTarget) Close() error          { return nil }

// TestRunDriver_RoutesByCapability covers both runDriver branches: a ContextDriver gets the
// caller's own context through RunContext and is never run through plain Run; a Driver without
// RunContext is run through Run.
func TestRunDriver_RoutesByCapability(t *testing.T) {
	ctx := context.WithValue(context.Background(), runDriverCtxKey{}, "caller")

	withCtx := &contextStubDriver{}
	tr, err := runDriver(ctx, withCtx, ScenarioCore(), stubTarget{})
	require.NoError(t, err)
	assert.Equal(t, "context-stub", tr.Driver)
	assert.Equal(t, 1, withCtx.contextRuns, "a ContextDriver must run through RunContext")
	assert.Zero(t, withCtx.runs, "a ContextDriver must never run through plain Run")
	require.NotNil(t, withCtx.got)
	assert.Equal(t, "caller", withCtx.got.Value(runDriverCtxKey{}), "RunContext must receive the caller's own context")

	plain := &plainStubDriver{}
	tr, err = runDriver(ctx, plain, ScenarioCore(), stubTarget{})
	require.NoError(t, err)
	assert.Equal(t, "plain-stub", tr.Driver)
	assert.Equal(t, 1, plain.runs, "a Driver without RunContext runs through Run")
}

// TestInProcessDriversAreContextDrivers: every driver that dials the broker from inside the test
// process takes the caller's context (the subprocess drivers are bounded by their own per-command
// timeouts instead — see runDriver).
func TestInProcessDriversAreContextDrivers(t *testing.T) {
	for _, d := range []Driver{NewFranzGoDriver(), NewSaramaDriver(), NewSegmentioDriver()} {
		_, ok := d.(ContextDriver)
		assert.True(t, ok, "in-process driver %q must implement ContextDriver so a caller's deadline reaches it", d.Name())
	}
}

// TestRunDriver_SegmentioHonoursCallerDeadline: against a broker whose group coordinator never
// comes up, the segmentio driver run under a 3-second caller deadline must end at that deadline —
// not wait out its own one-minute coordinator budget — with an error that names the wait.
func TestRunDriver_SegmentioHonoursCallerDeadline(t *testing.T) {
	broken, refused := kfakeWithCoordinatorDown(t, findCoordinatorTypeGroup, -1)

	const deadline = 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	r := watchRun(t, 20*time.Second, "the segmentio driver under a caller deadline", func() (Transcript, error) {
		return runDriver(ctx, NewSegmentioDriver(), ScenarioCore(), broken)
	})

	require.Error(t, r.err, "a group coordinator that never comes up must fail the run")
	assert.Less(t, r.took, deadline+5*time.Second, "the run must end at the caller's %s deadline", deadline)
	assert.Contains(t, r.err.Error(), "before round 1")
	assert.Contains(t, r.err.Error(), "no group coordinator")
	assert.ErrorIs(t, r.err, context.DeadlineExceeded)
	assert.Positive(t, refused.Load(), "the refusing coordinator must actually have been asked")
}

// TestRunDriver_SaramaHonoursCallerDeadline: a sarama run whose caller has already given up must
// fail with the caller's context error, not run to completion on its own budget.
func TestRunDriver_SaramaHonoursCallerDeadline(t *testing.T) {
	target, err := NewKfakeTarget()
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := watchRun(t, 20*time.Second, "the sarama driver under a canceled caller context", func() (Transcript, error) {
		return runDriver(ctx, NewSaramaDriver(), ScenarioCore(), target)
	})

	require.Error(t, r.err, "a run whose caller has given up must not report success (it round-tripped %d events)", len(r.transcript.Events))
	assert.ErrorIs(t, r.err, context.Canceled)
}
