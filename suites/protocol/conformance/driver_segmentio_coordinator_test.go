package conformance

// driver_segmentio_coordinator_test.go — the segmentio group arm's wait for a group coordinator
// before round 1. A fresh real Kafka broker answers COORDINATOR_NOT_AVAILABLE until it has created
// __consumer_offsets, and a single broker left at offsets.topic.replication.factor=3 answers it
// forever (the first real-Kafka run of this arm failed round 1 that way). The arm must absorb the
// first before round 1 starts, fail the second with an error that says so, and never wait out a
// non-loading answer. In-process kfake, no env gate, no containers.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// requestLog records, in kfake's request order, the coordinator refusals and the topics created.
type requestLog struct {
	mu      sync.Mutex
	entries []string
}

func (l *requestLog) add(entry string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
}

func (l *requestLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.entries...)
}

// logCreateTopics makes target record every CreateTopics topic name into log ("create:<topic>")
// without intercepting the request.
func logCreateTopics(target *KfakeTarget, log *requestLog) {
	target.cluster.ControlKey(int16(kmsg.CreateTopics), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		target.cluster.KeepControl()
		for _, tp := range kreq.(*kmsg.CreateTopicsRequest).Topics {
			log.add("create:" + tp.Topic)
		}
		return nil, nil, false
	})
}

// TestSegmentioGroupArm_WaitsForCoordinatorBeforeRound1: a group coordinator that refuses the first
// lookups (a fresh broker creating __consumer_offsets) must be waited for BEFORE round 1 creates its
// topic — round 1 itself must never meet a coordinator that is still loading — and the wait must be
// recorded as a note.
func TestSegmentioGroupArm_WaitsForCoordinatorBeforeRound1(t *testing.T) {
	log := &requestLog{}
	target, refused := kfakeRefusingCoordinator(t, findCoordinatorTypeGroup, 2, kerr.CoordinatorNotAvailable.Code,
		func() { log.add("refuse") })
	logCreateTopics(target, log)

	r := watchRun(t, 3*time.Minute, "SegmentioDriver.Run", func() (Transcript, error) {
		return NewSegmentioDriver().Run(ScenarioCore(), target)
	})
	require.NoError(t, r.err, "a coordinator that comes up must not fail the arm")
	require.EqualValues(t, 2, refused.Load(), "the first two group-coordinator lookups were refused")

	entries := log.snapshot()
	round1 := -1
	for i, e := range entries {
		if strings.HasPrefix(e, "create:") && strings.Contains(e, "-grp00-") {
			round1 = i
			break
		}
	}
	require.GreaterOrEqual(t, round1, 0, "round 1 created its topic (request log %v)", entries)
	for _, e := range entries[round1:] {
		assert.NotEqual(t, "refuse", e, "a coordinator refusal reached round 1 — the arm did not wait for the coordinator first (request log %v)", entries)
	}

	var noted bool
	for _, n := range r.transcript.Notes {
		if strings.Contains(n, "group coordinator became ready") && strings.Contains(n, "2 coordinator-loading answers") {
			noted = true
		}
	}
	assert.True(t, noted, "the wait must be recorded as a note, never silent (notes %v)", r.transcript.Notes)
}

// TestSegmentioGroupArm_CoordinatorNeverReady_FailsBeforeRound1: a coordinator that never comes up
// fails the arm at the wait's bound, with an error that names the coordinator and the broker's
// answer, before any round topic is created.
func TestSegmentioGroupArm_CoordinatorNeverReady_FailsBeforeRound1(t *testing.T) {
	log := &requestLog{}
	target, refused := kfakeRefusingCoordinator(t, findCoordinatorTypeGroup, -1, kerr.CoordinatorNotAvailable.Code, nil)
	logCreateTopics(target, log)

	const bound = 2 * time.Second
	d := &SegmentioDriver{coordinatorWait: bound}
	r := watchRun(t, time.Minute, "SegmentioDriver.Run", func() (Transcript, error) {
		return d.Run(ScenarioCore(), target)
	})

	require.Error(t, r.err, "a coordinator that never comes up must fail the arm")
	assert.Contains(t, r.err.Error(), "before round 1")
	assert.Contains(t, r.err.Error(), "no group coordinator")
	assert.Contains(t, r.err.Error(), "Group Coordinator Not Available", "the error must carry the broker's last answer")
	assert.Contains(t, r.err.Error(), "offsets.topic.replication.factor=1")
	assert.Positive(t, refused.Load())
	for _, e := range log.snapshot() {
		assert.NotContains(t, e, "-grp", "no group-arm round may start before the coordinator is ready")
	}
}

// TestSegmentioAwaitGroupCoordinator_NonLoadingAnswerFailsAtOnce: only the coordinator-loading
// answers are waited for. Any other answer (here GROUP_AUTHORIZATION_FAILED) is not a coordinator
// that is still coming up and fails immediately instead of being retried for the whole bound.
func TestSegmentioAwaitGroupCoordinator_NonLoadingAnswerFailsAtOnce(t *testing.T) {
	target, refused := kfakeRefusingCoordinator(t, findCoordinatorTypeGroup, -1, kerr.GroupAuthorizationFailed.Code, nil)

	start := time.Now()
	_, refusals, err := segmentioAwaitGroupCoordinator(context.Background(), target.BootstrapAddr(), "g", 30*time.Second)
	took := time.Since(start)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a coordinator that is still loading")
	assert.Less(t, took, 10*time.Second, "a non-loading answer must not be waited out")
	assert.Zero(t, refusals)
	assert.EqualValues(t, 1, refused.Load(), "exactly one lookup")
}

// TestSegmentioAwaitGroupCoordinator_ReadyAtOnce: a ready coordinator costs one lookup and reports
// no refusals (so no note is written).
func TestSegmentioAwaitGroupCoordinator_ReadyAtOnce(t *testing.T) {
	target, err := NewKfakeTarget()
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })

	_, refusals, err := segmentioAwaitGroupCoordinator(context.Background(), target.BootstrapAddr(), "g", 30*time.Second)
	require.NoError(t, err)
	assert.Zero(t, refusals)
}
