package conformance

// scenario_multipartition_test.go — pure-logic unit tests for scenario_multipartition.go.
// Runs under plain `go test -tags conformance` — no env gate, no
// containers (mirrors normalize_test.go/diff_test.go's "local development without run.sh").

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mpEvent is a small builder for ActionFetchFrom Events, keeping the table below readable.
func mpEvent(topic string, partition int32, key string, relOffset int64) Event {
	var k []byte
	if key != "" {
		k = []byte(key)
	}
	return Event{
		Action:    string(ActionFetchFrom),
		Topic:     topic,
		Partition: partition,
		Key:       k,
		RelOffset: relOffset,
	}
}

func TestScenarioMultiPartitionCheckKeyOrder(t *testing.T) {
	tests := []struct {
		name    string
		events  []Event
		wantErr bool
	}{
		{
			name: "in-order single partition",
			events: []Event{
				mpEvent("t", 0, "00-k00", 0),
				mpEvent("t", 0, "00-k01", 1),
				mpEvent("t", 0, "00-k02", 2),
			},
			wantErr: false,
		},
		{
			name: "in-order multiple partitions interleaved (cross-partition noise)",
			events: []Event{
				mpEvent("t", 0, "00-k00", 0),
				mpEvent("t", 1, "01-k00", 0),
				mpEvent("t", 0, "00-k01", 1),
				mpEvent("t", 2, "02-k00", 0),
				mpEvent("t", 1, "01-k01", 1),
				mpEvent("t", 0, "00-k02", 2),
			},
			wantErr: false,
		},
		{
			name: "out-of-order: a later-produced-index record is recorded BEFORE an earlier one, within one run",
			events: []Event{
				mpEvent("t", 0, "00-k05", 0), // produce-order index 5 recorded first...
				mpEvent("t", 0, "00-k02", 1), // ...then index 2 — regressed, same run (RelOffset never decreased)
			},
			wantErr: true,
		},
		{
			name: "legitimate re-fetch run boundary: offsets restart at 0 for the same partition (NOT a violation)",
			events: []Event{
				// First fetch run: produce-order indices 0..2.
				mpEvent("t", 0, "00-k00", 0),
				mpEvent("t", 0, "00-k01", 1),
				mpEvent("t", 0, "00-k02", 2),
				// Second fetch run (a full re-read from earliest through a new end — mirrors
				// ScenarioMultiPartition's step-4 mid-fetch-spans-both-generations shape):
				// legitimately restarts at RelOffset 0 AND produce-order index 0.
				mpEvent("t", 0, "00-k00", 0),
				mpEvent("t", 0, "00-k01", 1),
				mpEvent("t", 0, "00-k02", 2),
				mpEvent("t", 0, "00-k03", 3),
			},
			wantErr: false,
		},
		{
			name: "out-of-order WITHIN the second run (after a legitimate run-boundary reset)",
			events: []Event{
				mpEvent("t", 0, "00-k00", 0),
				mpEvent("t", 0, "00-k01", 1),
				// New run starts (RelOffset regresses to 0) — legitimate reset.
				mpEvent("t", 0, "00-k00", 0),
				mpEvent("t", 0, "00-k05", 1), // index 5 recorded...
				mpEvent("t", 0, "00-k02", 2), // ...then index 2 WITHIN the new run: violation
			},
			wantErr: true,
		},
		{
			name:    "empty input",
			events:  nil,
			wantErr: false,
		},
		{
			name: "non-FetchFrom events are ignored",
			events: []Event{
				{Action: string(ActionProduce), Topic: "t", Partition: 0, Key: []byte("00-k05"), RelOffset: 5},
				{Action: string(ActionListOffsets), Topic: "t", Partition: 0, Key: []byte("earliest"), RelOffset: 0},
				mpEvent("t", 0, "00-k00", 0),
			},
			wantErr: false,
		},
		{
			name: "error FetchFrom events (ErrCode != 0) are ignored",
			events: []Event{
				mpEvent("t", 0, "00-k05", 5),
				{Action: string(ActionFetchFrom), Topic: "t", Partition: 0, Key: []byte("00-k00"), RelOffset: 0, ErrCode: 6},
			},
			wantErr: false,
		},
		{
			name: "unkeyed records (nil Key) never contribute to a violation",
			events: []Event{
				mpEvent("t", 0, "", 0),
				mpEvent("t", 0, "", 1),
				mpEvent("t", 0, "", 0), // same nil key at a lower offset — nil keys are skipped entirely
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := scenarioMultiPartitionCheckKeyOrder(tc.events)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestParseScenarioMultiPartitionKeyIndex(t *testing.T) {
	tests := []struct {
		key     string
		wantIdx int
		wantOk  bool
	}{
		{"00-k00", 0, true},
		{"00-k05", 5, true},
		{"02-k17", 17, true},
		{"00-k99", 99, true},
		{"", 0, false},
		{"no-marker-here", 0, false},
		{"00-k", 0, false},           // empty digits after the marker
		{"00-kab", 0, false},         // non-digit after the marker
		{"weird-k-suffix", 0, false}, // "-k" present but trailing text isn't all digits
	}
	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			idx, ok := parseScenarioMultiPartitionKeyIndex(tc.key)
			assert.Equal(t, tc.wantOk, ok)
			if tc.wantOk {
				assert.Equal(t, tc.wantIdx, idx)
			}
		})
	}
}

func TestScenarioMultiPartition_Shape(t *testing.T) {
	sc := ScenarioMultiPartition()
	assert.Equal(t, scenarioMultiPartitionName, sc.Name)
	require.NotEmpty(t, sc.Steps)

	// First step must be the CreateTopic with the multi-partition count.
	require.Equal(t, ActionCreateTopic, sc.Steps[0].Action)
	assert.EqualValues(t, scenarioMultiPartitionCount, sc.Steps[0].NumPartitions)

	// Every ActionProduce/ActionFetchFrom/ActionListOffsets step must target one of the N
	// partitions (0..N-1) — no step accidentally targets an out-of-range or negative partition.
	for i, step := range sc.Steps {
		switch step.Action {
		case ActionProduce, ActionFetchFrom, ActionListOffsets:
			assert.GreaterOrEqualf(t, step.Partition, int32(0), "step %d (%s): partition must be >= 0", i, step.Action)
			assert.Lessf(t, step.Partition, scenarioMultiPartitionCount, "step %d (%s): partition must be < %d", i, step.Action, scenarioMultiPartitionCount)
		}
	}

	// Every partition must have at least one Produce, one FetchFrom, and one ListOffsets step.
	seenProduce := map[int32]bool{}
	seenFetch := map[int32]bool{}
	seenListOffsets := map[int32]bool{}
	for _, step := range sc.Steps {
		switch step.Action {
		case ActionProduce:
			seenProduce[step.Partition] = true
		case ActionFetchFrom:
			seenFetch[step.Partition] = true
		case ActionListOffsets:
			seenListOffsets[step.Partition] = true
		}
	}
	for p := int32(0); p < scenarioMultiPartitionCount; p++ {
		assert.True(t, seenProduce[p], "partition %d must have at least one Produce step", p)
		assert.True(t, seenFetch[p], "partition %d must have at least one FetchFrom step", p)
		assert.True(t, seenListOffsets[p], "partition %d must have at least one ListOffsets step", p)
	}
}

func TestScenarioMultiPartitionRecord_KeyEmbedsPartitionAndIndex(t *testing.T) {
	r0 := scenarioMultiPartitionRecord(0, 3)
	r1 := scenarioMultiPartitionRecord(1, 3)
	assert.NotEqual(t, string(r0.Key), string(r1.Key), "records for different partitions at the same index must have distinct keys")

	// Empty-value case (idx%5==0) must still be exercised per-partition.
	rEmpty := scenarioMultiPartitionRecord(0, 0)
	assert.Equal(t, []byte{}, rEmpty.Value, "idx%5==0 must produce an empty (non-nil) value, mirroring scenario_core.go's requirement")
}
