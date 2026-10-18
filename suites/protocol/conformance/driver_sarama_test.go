package conformance

// driver_sarama_test.go — pure-unit coverage for driver_sarama.go's coverable-without-broker
// surface (mirrors driver_java_test.go's precedent: no broker, no env gate, no network dial —
// only string/struct-building and pure conversion/diffing logic).

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

// TestNewSaramaDriver proves the constructor's identity (Name/Kind) — the required baseline,
// mirroring driver_java_test.go's TestNewJavaDriver.
func TestNewSaramaDriver(t *testing.T) {
	d := NewSaramaDriver()
	assert.Equal(t, "sarama", d.Name())
	assert.Equal(t, DriverKindSmoke, d.Kind())
}

// TestSaramaConfig proves saramaConfig sets the fields NewSyncProducer's verifyProducerConfig
// requires once any Producer field is customized (Return.Successes), the durable-ack policy
// (RequiredAcks == WaitForAll), and Consumer.Return.Errors == true — the last is what keeps
// saramaConsume's pc.Errors() arm live (sarama defaults it to false, which would silently route
// consume errors to the logger instead). No network call, just inspecting the returned struct.
func TestSaramaConfig(t *testing.T) {
	cfg := saramaConfig()
	assert.True(t, cfg.Producer.Return.Successes)
	assert.Equal(t, sarama.WaitForAll, cfg.Producer.RequiredAcks)
	assert.True(t, cfg.Consumer.Return.Errors)
}

// TestToSaramaHeaders_FromSaramaHeaders_RoundTrip proves the header conversion is byte-exact in
// both directions, including the []byte(key)/string(key) conversion point, and that nil headers
// round-trip to nil (matching harness.go's "headers[0] is nil (no headers)" convention used by
// scenarioCoreHeaderSets).
func TestToSaramaHeaders_FromSaramaHeaders_RoundTrip(t *testing.T) {
	t.Run("nil headers", func(t *testing.T) {
		assert.Nil(t, toSaramaHeaders(nil))
		assert.Nil(t, fromSaramaHeaders(nil))
	})

	t.Run("non-empty headers round-trip", func(t *testing.T) {
		in := []RecordHeader{
			{Key: "trace-id", Value: []byte("abc-123")},
			{Key: "content-type", Value: []byte("application/json")},
		}
		saramaHeaders := toSaramaHeaders(in)
		assert.Equal(t, []sarama.RecordHeader{
			{Key: []byte("trace-id"), Value: []byte("abc-123")},
			{Key: []byte("content-type"), Value: []byte("application/json")},
		}, saramaHeaders)

		// fromSaramaHeaders takes []*sarama.RecordHeader (a consumed ConsumerMessage's shape).
		ptrs := make([]*sarama.RecordHeader, len(saramaHeaders))
		for i := range saramaHeaders {
			ptrs[i] = &saramaHeaders[i]
		}
		out := fromSaramaHeaders(ptrs)
		assert.Equal(t, in, out)
	})
}

// TestSaramaConsumedToEvents_Success proves a matching count converts cleanly into Events carrying
// the produce action, physical topic, key/offset, and content+header digests via the real
// HashValue/HashHeaders.
func TestSaramaConsumedToEvents_Success(t *testing.T) {
	produced := []Record{
		{Key: []byte("k0"), Value: []byte("v0"), Headers: []RecordHeader{{Key: "h", Value: []byte("1")}}},
		{Key: nil, Value: []byte(""), Headers: nil},
	}
	consumed := []saramaConsumedRecord{
		{Key: []byte("k0"), Value: []byte("v0"), Headers: []RecordHeader{{Key: "h", Value: []byte("1")}}, Offset: 0},
		{Key: nil, Value: []byte(""), Headers: nil, Offset: 1},
	}

	events, err := saramaConsumedToEvents(consumed, produced, "physical-topic-1")
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
	// Distinguish "no headers" (empty string) from a real digest.
	assert.NotEmpty(t, events[0].HeadersSHA)
	assert.Empty(t, events[1].HeadersSHA)
}

// TestSaramaConsumedToEvents_CountMismatch proves the round-trip count check — a Smoke driver's
// core success signal — errors loudly on a produced/consumed count mismatch.
func TestSaramaConsumedToEvents_CountMismatch(t *testing.T) {
	produced := []Record{{Key: []byte("k0"), Value: []byte("v0")}, {Key: []byte("k1"), Value: []byte("v1")}}
	consumed := []saramaConsumedRecord{{Key: []byte("k0"), Value: []byte("v0"), Offset: 0}}

	_, err := saramaConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "count mismatch")
}

// TestSaramaConsumedToEvents_KeyMismatch proves a position-i key mismatch errors loudly rather
// than silently accepting a wrong-but-same-length round-trip.
func TestSaramaConsumedToEvents_KeyMismatch(t *testing.T) {
	produced := []Record{{Key: []byte("k0"), Value: []byte("v0")}}
	consumed := []saramaConsumedRecord{{Key: []byte("wrong-key"), Value: []byte("v0"), Offset: 0}}

	_, err := saramaConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key mismatch")
}

// TestSaramaConsumedToEvents_ValueMismatch proves a position-i value mismatch errors loudly.
func TestSaramaConsumedToEvents_ValueMismatch(t *testing.T) {
	produced := []Record{{Key: []byte("k0"), Value: []byte("v0")}}
	consumed := []saramaConsumedRecord{{Key: []byte("k0"), Value: []byte("wrong-value"), Offset: 0}}

	_, err := saramaConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "value mismatch")
}

// TestSaramaConsumedToEvents_NilVsEmptyKeyPreserved proves nil-vs-empty-key distinction is
// preserved through the conversion (harness.go's Record.Key doc: nil means unkeyed, must not
// collapse to empty).
func TestSaramaConsumedToEvents_NilVsEmptyKeyPreserved(t *testing.T) {
	produced := []Record{{Key: nil, Value: []byte("v0")}}
	consumed := []saramaConsumedRecord{{Key: nil, Value: []byte("v0"), Offset: 0}}

	events, err := saramaConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.NoError(t, err)
	assert.Nil(t, events[0].Key)
}

// TestSaramaConsumedToEvents_HeaderMismatch is a regression test:
// a position-i HEADER divergence (key/value/count/order — anything that changes the canonical
// HashHeaders digest) between produced and consumed must error loudly, exactly like a key or
// value mismatch does — a header-dropping/corrupting target must never be reported
// VerdictIdentical by this Smoke driver, which documents itself as the first driver able to
// verify header round-trip fidelity.
func TestSaramaConsumedToEvents_HeaderMismatch(t *testing.T) {
	produced := []Record{{Key: []byte("k0"), Value: []byte("v0"), Headers: []RecordHeader{{Key: "h", Value: []byte("1")}}}}
	consumed := []saramaConsumedRecord{{Key: []byte("k0"), Value: []byte("v0"), Headers: []RecordHeader{{Key: "h", Value: []byte("CORRUPTED")}}, Offset: 0}}

	_, err := saramaConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "header mismatch")
}

// TestSaramaConsumedToEvents_HeaderDropped_ErrorsNotSilentlyAccepted proves the specific
// failure scenario: a target that DROPS headers entirely (consumed carries none)
// while the produced record had headers must error, not silently produce a VerdictIdentical-shaped
// Event with an empty HeadersSHA.
func TestSaramaConsumedToEvents_HeaderDropped_ErrorsNotSilentlyAccepted(t *testing.T) {
	produced := []Record{{Key: []byte("k0"), Value: []byte("v0"), Headers: []RecordHeader{{Key: "h", Value: []byte("1")}}}}
	consumed := []saramaConsumedRecord{{Key: []byte("k0"), Value: []byte("v0"), Headers: nil, Offset: 0}}

	_, err := saramaConsumedToEvents(consumed, produced, "physical-topic-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "header mismatch")
}
