package conformance

// driver_segmentio_test.go — pure-unit coverage for driver_segmentio.go's coverable-without-broker
// surface (mirrors driver_sarama_test.go's precedent: no broker, no env gate, no network dial).

import (
	"errors"
	"fmt"
	"testing"

	skafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

// TestNewSegmentioDriver proves the constructor's identity (Name/Kind).
func TestNewSegmentioDriver(t *testing.T) {
	d := NewSegmentioDriver()
	assert.Equal(t, "segmentio", d.Name())
	assert.Equal(t, DriverKindSmoke, d.Kind())
}

// TestToSegmentioHeaders_FromSegmentioHeaders_RoundTrip proves the header conversion is a
// no-silent-drift 1:1 field copy (skafka.Header is structurally identical to RecordHeader), and
// that nil headers round-trip to nil.
func TestToSegmentioHeaders_FromSegmentioHeaders_RoundTrip(t *testing.T) {
	t.Run("nil headers", func(t *testing.T) {
		assert.Nil(t, toSegmentioHeaders(nil))
		assert.Nil(t, fromSegmentioHeaders(nil))
	})

	t.Run("non-empty headers round-trip", func(t *testing.T) {
		in := []RecordHeader{
			{Key: "trace-id", Value: []byte("abc-123")},
			{Key: "content-type", Value: []byte("application/json")},
		}
		segHeaders := toSegmentioHeaders(in)
		assert.Equal(t, []skafka.Header{
			{Key: "trace-id", Value: []byte("abc-123")},
			{Key: "content-type", Value: []byte("application/json")},
		}, segHeaders)

		out := fromSegmentioHeaders(segHeaders)
		assert.Equal(t, in, out)
	})
}

// TestSegmentioConsumedToEvents_Success proves a matching count converts cleanly into Events
// carrying the produce action, physical topic, key/offset, and content+header digests via the
// real HashValue/HashHeaders — proving symmetric behavior with saramaConsumedToEvents.
func TestSegmentioConsumedToEvents_Success(t *testing.T) {
	produced := []Record{
		{Key: []byte("k0"), Value: []byte("v0"), Headers: []RecordHeader{{Key: "h", Value: []byte("1")}}},
		{Key: nil, Value: []byte(""), Headers: nil},
	}
	consumed := []segmentioConsumedRecord{
		{Key: []byte("k0"), Value: []byte("v0"), Headers: []RecordHeader{{Key: "h", Value: []byte("1")}}, Offset: 0},
		{Key: nil, Value: []byte(""), Headers: nil, Offset: 1},
	}

	events, err := segmentioConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.NoError(t, err)
	assert.Len(t, events, 2)

	for i, ev := range events {
		assert.Equal(t, string(ActionProduce), ev.Action)
		assert.Equal(t, "physical-topic-1", ev.Topic)
		assert.Equal(t, int32(0), ev.Partition)
		assert.Equal(t, consumed[i].Key, ev.Key)
		assert.Equal(t, HashValue(consumed[i].Value), ev.ValueSHA)
		assert.Equal(t, HashHeaders(consumed[i].Headers), ev.HeadersSHA)
		assert.Equal(t, consumed[i].Offset, ev.RelOffset)
		assert.Equal(t, int16(0), ev.ErrCode)
	}
	assert.NotEmpty(t, events[0].HeadersSHA)
	assert.Empty(t, events[1].HeadersSHA)
}

// TestSegmentioConsumedToEvents_CountMismatch proves the round-trip count check errors loudly on
// a produced/consumed count mismatch.
func TestSegmentioConsumedToEvents_CountMismatch(t *testing.T) {
	produced := []Record{{Key: []byte("k0"), Value: []byte("v0")}, {Key: []byte("k1"), Value: []byte("v1")}}
	consumed := []segmentioConsumedRecord{{Key: []byte("k0"), Value: []byte("v0"), Offset: 0}}

	_, err := segmentioConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "count mismatch")
}

// TestSegmentioConsumedToEvents_KeyMismatch proves a position-i key mismatch errors loudly.
func TestSegmentioConsumedToEvents_KeyMismatch(t *testing.T) {
	produced := []Record{{Key: []byte("k0"), Value: []byte("v0")}}
	consumed := []segmentioConsumedRecord{{Key: []byte("wrong-key"), Value: []byte("v0"), Offset: 0}}

	_, err := segmentioConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key mismatch")
}

// TestSegmentioConsumedToEvents_ValueMismatch proves a position-i value mismatch errors loudly.
func TestSegmentioConsumedToEvents_ValueMismatch(t *testing.T) {
	produced := []Record{{Key: []byte("k0"), Value: []byte("v0")}}
	consumed := []segmentioConsumedRecord{{Key: []byte("k0"), Value: []byte("wrong-value"), Offset: 0}}

	_, err := segmentioConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "value mismatch")
}

// TestSegmentioConsumedToEvents_NilVsEmptyKeyPreserved proves nil-vs-empty-key distinction is
// preserved through the conversion (harness.go's Record.Key doc: nil means unkeyed).
func TestSegmentioConsumedToEvents_NilVsEmptyKeyPreserved(t *testing.T) {
	produced := []Record{{Key: nil, Value: []byte("v0")}}
	consumed := []segmentioConsumedRecord{{Key: nil, Value: []byte("v0"), Offset: 0}}

	events, err := segmentioConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.NoError(t, err)
	assert.Nil(t, events[0].Key)
}

// TestSegmentioConsumedToEvents_HeaderMismatch is a regression test: a position-i HEADER
// divergence between produced and consumed must error loudly, exactly like a key or value mismatch
// does — mirrors driver_sarama_test.go's identical test, proving both Smoke drivers actually verify
// header round-trip fidelity rather than merely hashing-and-ignoring it into HeadersSHA.
func TestSegmentioConsumedToEvents_HeaderMismatch(t *testing.T) {
	produced := []Record{{Key: []byte("k0"), Value: []byte("v0"), Headers: []RecordHeader{{Key: "h", Value: []byte("1")}}}}
	consumed := []segmentioConsumedRecord{{Key: []byte("k0"), Value: []byte("v0"), Headers: []RecordHeader{{Key: "h", Value: []byte("CORRUPTED")}}, Offset: 0}}

	_, err := segmentioConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "header mismatch")
}

// TestSegmentioConsumedToEvents_HeaderDropped_ErrorsNotSilentlyAccepted proves the specific
// failure scenario: a target that DROPS headers entirely (consumed carries none) while
// the produced record had headers must error, not silently produce a VerdictIdentical-shaped
// Event with an empty HeadersSHA.
func TestSegmentioConsumedToEvents_HeaderDropped_ErrorsNotSilentlyAccepted(t *testing.T) {
	produced := []Record{{Key: []byte("k0"), Value: []byte("v0"), Headers: []RecordHeader{{Key: "h", Value: []byte("1")}}}}
	consumed := []segmentioConsumedRecord{{Key: []byte("k0"), Value: []byte("v0"), Headers: nil, Offset: 0}}

	_, err := segmentioConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "header mismatch")
}

// TestSegmentioGroupCommitCount pins the per-round commit points: always strictly inside the
// record range (1..segmentioGroupRecords-1), so a resume at "start" (0) or "end" can never pass for
// a correct resume, and every value is exercised across the ten rounds.
func TestSegmentioGroupCommitCount(t *testing.T) {
	seen := map[int]bool{}
	for round := 0; round < segmentioGroupRounds; round++ {
		c := segmentioGroupCommitCount(round)
		assert.GreaterOrEqual(t, c, 1, "round %d", round)
		assert.LessOrEqual(t, c, segmentioGroupRecords-1, "round %d", round)
		seen[c] = true
	}
	assert.Len(t, seen, segmentioGroupRecords-1, "the rounds must cover every commit point 1..%d", segmentioGroupRecords-1)
}

// TestSegmentioCheckResume is the resume rule without a broker: the second member must start at
// exactly the committed offset, continue contiguously, and deliver the produced bytes.
func TestSegmentioCheckResume(t *testing.T) {
	records := make([]Record, segmentioGroupRecords)
	for i := range records {
		records[i] = Record{Key: []byte{byte('a' + i)}, Value: []byte{byte('0' + i)}, Headers: []RecordHeader{{Key: "h", Value: []byte{byte(i)}}}}
	}
	restFrom := func(start int) []segmentioConsumedRecord {
		var out []segmentioConsumedRecord
		for i := start; i < len(records); i++ {
			out = append(out, segmentioConsumedRecord{Key: records[i].Key, Value: records[i].Value, Headers: records[i].Headers, Offset: int64(i)})
		}
		return out
	}

	assert.NoError(t, segmentioCheckResume(restFrom(3), records, 3))

	// Resumed at the start instead of the committed offset (a broker that lost the commit).
	err := segmentioCheckResume(restFrom(0)[:3], records, 3)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "resumed at offset 0, want exactly the committed offset 3")
	}
	// Resumed one past the commit (an off-by-one committed offset).
	err = segmentioCheckResume(append(restFrom(4), segmentioConsumedRecord{Offset: 6}), records, 3)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "resumed at offset 4")
	}
	// Right offset, wrong bytes.
	bad := restFrom(3)
	bad[1].Value = []byte("corrupt")
	err = segmentioCheckResume(bad, records, 3)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "value mismatch")
	}
	// Short delivery.
	err = segmentioCheckResume(restFrom(3)[:1], records, 3)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "delivered 1 records after the commit point, want 3")
	}
}

// TestSegmentioMetadataLagAllowed pins the immediate-write allowance: only on the real-Kafka
// oracle, and only for the metadata-propagation answers (UNKNOWN_TOPIC_OR_PARTITION,
// LEADER_NOT_AVAILABLE) — bare or inside WriteErrors. KubeMQ never gets it, and no other error
// qualifies on either target.
func TestSegmentioMetadataLagAllowed(t *testing.T) {
	unknown := fmt.Errorf("wrapped: %w", skafka.UnknownTopicOrPartition)
	assert.True(t, segmentioMetadataLagAllowed(true, unknown))
	assert.True(t, segmentioMetadataLagAllowed(true, skafka.LeaderNotAvailable))
	assert.True(t, segmentioMetadataLagAllowed(true, skafka.WriteErrors{skafka.UnknownTopicOrPartition, nil, skafka.UnknownTopicOrPartition}))

	assert.False(t, segmentioMetadataLagAllowed(false, unknown), "KubeMQ gets no allowance: the synchronous cache drop is what the step tests")
	assert.False(t, segmentioMetadataLagAllowed(false, skafka.WriteErrors{skafka.UnknownTopicOrPartition}))
	assert.False(t, segmentioMetadataLagAllowed(true, nil))
	assert.False(t, segmentioMetadataLagAllowed(true, skafka.NotLeaderForPartition))
	assert.False(t, segmentioMetadataLagAllowed(true, errors.New("dial tcp: connection refused")))
	assert.False(t, segmentioMetadataLagAllowed(true, skafka.WriteErrors{skafka.UnknownTopicOrPartition, skafka.MessageSizeTooLarge}),
		"a lag error mixed with a real failure is not lag")
	assert.False(t, segmentioMetadataLagAllowed(true, skafka.WriteErrors{nil, nil}))
}
