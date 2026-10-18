package conformance

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashValue_DistinctAndStable(t *testing.T) {
	tests := []struct {
		name string
		a    []byte
		b    []byte
		same bool
	}{
		{name: "same bytes hash equal", a: []byte("hello"), b: []byte("hello"), same: true},
		{name: "different bytes hash different", a: []byte("hello"), b: []byte("world"), same: false},
		{name: "nil and empty hash equal (both zero-length)", a: nil, b: []byte{}, same: true},
		{name: "empty differs from non-empty", a: []byte{}, b: []byte("x"), same: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ha := HashValue(tt.a)
			hb := HashValue(tt.b)
			require.NotEmpty(t, ha)
			require.NotEmpty(t, hb)
			if tt.same {
				assert.Equal(t, ha, hb)
			} else {
				assert.NotEqual(t, ha, hb)
			}
		})
	}
}

func TestHashValue_Deterministic(t *testing.T) {
	v := []byte("repeatable payload")
	assert.Equal(t, HashValue(v), HashValue(v))
}

func TestHashHeaders_EmptyIsEmptyString(t *testing.T) {
	tests := []struct {
		name    string
		headers []RecordHeader
	}{
		{name: "nil headers", headers: nil},
		{name: "empty slice headers", headers: []RecordHeader{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, "", HashHeaders(tt.headers))
		})
	}
}

func TestHashHeaders_OrderSignificant(t *testing.T) {
	a := []RecordHeader{{Key: "k1", Value: []byte("v1")}, {Key: "k2", Value: []byte("v2")}}
	b := []RecordHeader{{Key: "k2", Value: []byte("v2")}, {Key: "k1", Value: []byte("v1")}}

	ha := HashHeaders(a)
	hb := HashHeaders(b)

	require.NotEmpty(t, ha)
	require.NotEmpty(t, hb)
	assert.NotEqual(t, ha, hb, "header order must be significant — Kafka headers are an ordered list, not a map")
}

func TestHashHeaders_NoAmbiguityAcrossKeyValueBoundaries(t *testing.T) {
	// "ab" + "" must not collide with "a" + "b" under a naive concatenation scheme — the
	// length-prefixed canonical encoding must keep these distinct.
	a := []RecordHeader{{Key: "ab", Value: []byte("")}}
	b := []RecordHeader{{Key: "a", Value: []byte("b")}}

	assert.NotEqual(t, HashHeaders(a), HashHeaders(b))
}

func TestHashHeaders_Deterministic(t *testing.T) {
	h := []RecordHeader{{Key: "trace-id", Value: []byte("abc123")}}
	assert.Equal(t, HashHeaders(h), HashHeaders(h))
}

func TestNormalize_RewritesTopicToLogicalTopic(t *testing.T) {
	tr := Transcript{
		Driver:       "kgo",
		Target:       "real-kafka",
		LogicalTopic: "core",
		Events: []Event{
			{Action: "FetchFrom", Topic: "run-abc123-core", Partition: 0, RelOffset: 0},
			{Action: "FetchFrom", Topic: "run-abc123-core", Partition: 0, RelOffset: 1},
		},
	}

	got := Normalize(tr)

	require.Len(t, got.Events, 2)
	for _, e := range got.Events {
		assert.Equal(t, "core", e.Topic)
	}
}

func TestNormalize_BaseRelativeOffsets(t *testing.T) {
	tests := []struct {
		name   string
		events []Event
		want   []int64
	}{
		{
			name: "base already zero — offsets unchanged",
			events: []Event{
				{Action: "FetchFrom", Topic: "t", Partition: 0, RelOffset: 0},
				{Action: "FetchFrom", Topic: "t", Partition: 0, RelOffset: 1},
				{Action: "FetchFrom", Topic: "t", Partition: 0, RelOffset: 2},
			},
			want: []int64{0, 1, 2},
		},
		{
			name: "nonzero base subtracted",
			events: []Event{
				{Action: "FetchFrom", Topic: "t", Partition: 0, RelOffset: 100},
				{Action: "FetchFrom", Topic: "t", Partition: 0, RelOffset: 101},
				{Action: "FetchFrom", Topic: "t", Partition: 0, RelOffset: 105},
			},
			want: []int64{0, 1, 5},
		},
		{
			name: "per-partition base is independent",
			events: []Event{
				{Action: "FetchFrom", Topic: "t", Partition: 0, RelOffset: 50},
				{Action: "FetchFrom", Topic: "t", Partition: 1, RelOffset: 900},
				{Action: "FetchFrom", Topic: "t", Partition: 0, RelOffset: 51},
				{Action: "FetchFrom", Topic: "t", Partition: 1, RelOffset: 901},
			},
			want: []int64{0, 0, 1, 1},
		},
		{
			// The P1 fix: a CreateTopic event's zero-value RelOffset(0) must NOT floor the base
			// when the real produced offsets start at 1000+. CreateTopic is not offset-bearing,
			// so base = min over the Produce events = 1000; the CreateTopic passes through at 0.
			name: "non-offset-bearing CreateTopic does not floor the base",
			events: []Event{
				{Action: "CreateTopic", Topic: "t", Partition: 0, RelOffset: 0},
				{Action: "Produce", Topic: "t", Partition: 0, RelOffset: 1000},
				{Action: "Produce", Topic: "t", Partition: 0, RelOffset: 1001},
			},
			want: []int64{0, 0, 1},
		},
		{
			// The P1 fix: a failed produce carrying the -1 "no offset" sentinel must NOT drag the
			// base to -1 (which would shift every real offset by +1). The error event is not
			// offset-bearing (ErrCode != 0 and RelOffset < 0), so base = 1000; its -1 passes
			// through unchanged.
			name: "error sentinel (-1) does not shift the base",
			events: []Event{
				{Action: "Produce", Topic: "t", Partition: 0, RelOffset: -1, ErrCode: 5},
				{Action: "Produce", Topic: "t", Partition: 0, RelOffset: 1000},
				{Action: "Produce", Topic: "t", Partition: 0, RelOffset: 1001},
			},
			want: []int64{-1, 0, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := Transcript{LogicalTopic: "core", Events: tt.events}
			got := Normalize(tr)
			require.Len(t, got.Events, len(tt.want))
			for i, e := range got.Events {
				assert.Equal(t, tt.want[i], e.RelOffset, "event %d", i)
			}
		})
	}
}

func TestIsOffsetBearing(t *testing.T) {
	tests := []struct {
		name string
		e    Event
		want bool
	}{
		{name: "successful Produce is offset-bearing", e: Event{Action: "Produce", RelOffset: 0}, want: true},
		{name: "successful FetchFrom is offset-bearing", e: Event{Action: "FetchFrom", RelOffset: 5}, want: true},
		{name: "CreateTopic is not offset-bearing", e: Event{Action: "CreateTopic", RelOffset: 0}, want: false},
		{name: "Metadata is not offset-bearing", e: Event{Action: "Metadata", RelOffset: 0}, want: false},
		{name: "ListOffsets is not offset-bearing", e: Event{Action: "ListOffsets", RelOffset: 0}, want: false},
		{name: "Produce with non-zero ErrCode is not offset-bearing", e: Event{Action: "Produce", RelOffset: 7, ErrCode: 5}, want: false},
		{name: "FetchFrom with negative RelOffset is not offset-bearing", e: Event{Action: "FetchFrom", RelOffset: -1}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isOffsetBearing(tt.e))
		})
	}
}

func TestNormalize_DoesNotMutateInput(t *testing.T) {
	tr := Transcript{
		LogicalTopic: "core",
		Events: []Event{
			{Action: "FetchFrom", Topic: "run-xyz-core", Partition: 0, RelOffset: 42},
		},
	}
	original := tr.Events[0]

	_ = Normalize(tr)

	assert.Equal(t, original, tr.Events[0], "Normalize must not mutate its input transcript")
}

func TestNormalize_PreservesActionKeyErrCode(t *testing.T) {
	tr := Transcript{
		LogicalTopic: "core",
		Events: []Event{
			{
				Action:     "Produce",
				Topic:      "run-1-core",
				Partition:  0,
				Key:        []byte("k1"),
				ValueSHA:   HashValue([]byte("v1")),
				HeadersSHA: "",
				RelOffset:  10,
				ErrCode:    0,
			},
		},
	}

	got := Normalize(tr)

	require.Len(t, got.Events, 1)
	e := got.Events[0]
	assert.Equal(t, "Produce", e.Action)
	assert.Equal(t, []byte("k1"), e.Key)
	assert.Equal(t, HashValue([]byte("v1")), e.ValueSHA)
	assert.Equal(t, int16(0), e.ErrCode)
}

func TestCheckFreshTopicBaseOffsetZero(t *testing.T) {
	tests := []struct {
		name    string
		events  []Event
		wantErr error  // sentinel to check with errors.Is; nil means "no error"
		wantSub string // substring the error message must contain, if non-empty
	}{
		{
			name: "single fresh topic starts at 0 — invariant holds",
			events: []Event{
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 0},
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 1},
			},
			wantErr: nil,
		},
		{
			name: "starts at nonzero — invariant violated",
			events: []Event{
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 1},
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 2},
			},
			wantSub: "first fetched baseOffset=1, want 0",
		},
		{
			name:    "no FetchFrom events — ErrNoEvents",
			events:  []Event{{Action: string(ActionProduce), Topic: "core", Partition: 0, RelOffset: 0}},
			wantErr: ErrNoEvents,
		},
		{
			name:    "empty transcript — ErrNoEvents",
			events:  nil,
			wantErr: ErrNoEvents,
		},
		{
			name: "non-FetchFrom events with nonzero offsets do not trip the invariant",
			events: []Event{
				{Action: string(ActionProduce), Topic: "core", Partition: 0, RelOffset: 999},
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 0},
			},
			wantErr: nil,
		},
		{
			name: "multiple partitions — one violates",
			events: []Event{
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 0},
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 1, RelOffset: 3},
			},
			wantSub: "partition=1",
		},
		{
			name: "minimum offset among multiple FetchFrom events is used, not the first seen",
			events: []Event{
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 5},
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 0},
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 7},
			},
			wantErr: nil,
		},
		{
			// A FetchFrom ERROR event leaves RelOffset at its zero-value (0); it must NOT be
			// counted as a successful serve. With no OTHER successful FetchFrom event, the
			// invariant must classify this like the no-FetchFrom case (ErrNoEvents), NOT falsely
			// certify SUCCESS by treating the error's RelOffset==0 as a served baseOffset==0.
			name: "single FetchFrom error event (ErrCode!=0, RelOffset 0) — ErrNoEvents, not nil",
			events: []Event{
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 0, ErrCode: 3},
			},
			wantErr: ErrNoEvents,
		},
		{
			// A partition whose ONLY FetchFrom events are errors contributes nothing to the min;
			// if it is the only partition with FetchFrom events, the result is ErrNoEvents.
			name: "partition with only error FetchFrom events — ErrNoEvents",
			events: []Event{
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 0, ErrCode: 1},
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 0, ErrCode: 2},
			},
			wantErr: ErrNoEvents,
		},
		{
			// Error FetchFrom events are skipped, so they cannot floor the minimum at 0: only the
			// SUCCESSFUL FetchFrom offsets participate, and here they start at 4 => violation.
			name: "error events do not floor the min — successful offsets start nonzero => violation",
			events: []Event{
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 0, ErrCode: 5},
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 4, ErrCode: 0},
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 5, ErrCode: 0},
			},
			wantSub: "first fetched baseOffset=4, want 0",
		},
		{
			// A successful FetchFrom at 0 alongside an error event still holds the invariant.
			name: "successful FetchFrom at 0 with an error event present — invariant holds",
			events: []Event{
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 0, ErrCode: 9},
				{Action: string(ActionFetchFrom), Topic: "core", Partition: 0, RelOffset: 0, ErrCode: 0},
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := Transcript{Target: "real-kafka", Events: tt.events}
			err := CheckFreshTopicBaseOffsetZero(tr)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tt.wantErr) || err == tt.wantErr)
				return
			}
			if tt.wantSub != "" {
				require.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.wantSub), "error %q must contain %q", err.Error(), tt.wantSub)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestCheckFreshTopicBaseOffsetZero_UsesRawOffsetsNotNormalized(t *testing.T) {
	// Regression guard for the file-doc rule: the invariant MUST be checked on raw
	// (pre-normalization) offsets. If it were checked post-Normalize, a leaked 1-based start
	// would be silently rewritten to a base-relative 0 and this bug would never be caught.
	tr := Transcript{
		Target:       "leaky-target",
		LogicalTopic: "core",
		Events: []Event{
			{Action: string(ActionFetchFrom), Topic: "run-1-core", Partition: 0, RelOffset: 1}, // leaked 1-based start
			{Action: string(ActionFetchFrom), Topic: "run-1-core", Partition: 0, RelOffset: 2},
		},
	}

	// Raw check must fail.
	err := CheckFreshTopicBaseOffsetZero(tr)
	require.Error(t, err)

	// But after normalization the same transcript's offsets start at 0 (by construction of
	// base-relative rewriting) — proving why the invariant must run on the raw transcript.
	norm := Normalize(tr)
	require.NotEmpty(t, norm.Events)
	assert.Equal(t, int64(0), norm.Events[0].RelOffset)
}
