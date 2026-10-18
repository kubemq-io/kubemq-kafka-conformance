package probe

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	register("fetch-sessions", fetchSessions)
	register("fetch-kip74-oversized-first-record", fetchOversizedFirstRecord)
	register("fetch-long-poll", fetchLongPoll)
}

// fetchTarget is a one-partition topic with its id and leader, for raw Fetch requests.
type fetchTarget struct {
	cl     *kgo.Client
	topic  string
	id     [16]byte
	leader int32
}

func newFetchTarget(ctx context.Context, env Env, name string, records [][]byte) (*fetchTarget, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	topic := topicName(env, name)
	if err := createTopic(ctx, adm, topic, 1, nil); err != nil {
		cl.Close()
		return nil, err
	}
	for _, v := range records {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: v}).FirstErr(); err != nil {
			cl.Close()
			return nil, fmt.Errorf("produce: %w", err)
		}
	}
	md, err := adm.Metadata(ctx, topic)
	if err != nil {
		cl.Close()
		return nil, err
	}
	t, ok := md.Topics[topic]
	if !ok || len(t.Partitions) == 0 {
		cl.Close()
		return nil, fmt.Errorf("metadata for %s lists no partition", topic)
	}
	return &fetchTarget{cl: cl, topic: topic, id: t.ID, leader: t.Partitions[0].Leader}, nil
}

// fetch sends one Fetch to the partition leader at the version the client negotiated.
func (f *fetchTarget) fetch(ctx context.Context, sessionID, epoch, minBytes, maxWait, maxBytes, partMax int32, offset int64) (*kmsg.FetchResponse, error) {
	req := kmsg.NewPtrFetchRequest()
	req.ReplicaID = -1
	req.MaxWaitMillis, req.MinBytes, req.MaxBytes = maxWait, minBytes, maxBytes
	req.SessionID, req.SessionEpoch = sessionID, epoch
	t := kmsg.NewFetchRequestTopic()
	t.Topic, t.TopicID = f.topic, f.id
	p := kmsg.NewFetchRequestTopicPartition()
	p.Partition, p.FetchOffset, p.PartitionMaxBytes, p.CurrentLeaderEpoch, p.LogStartOffset = 0, offset, partMax, -1, -1
	t.Partitions = append(t.Partitions, p)
	req.Topics = append(req.Topics, t)
	return req.RequestWith(ctx, f.cl.Broker(int(f.leader)))
}

func partition(resp *kmsg.FetchResponse) (*kmsg.FetchResponseTopicPartition, bool) {
	if len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 {
		return &resp.Topics[0].Partitions[0], true
	}
	return nil, false
}

// fetchSessions (KIP-227): a full fetch with session id 0 and epoch 0 asks the broker for a
// session; an incremental fetch on that session at epoch 1 must succeed; a fetch naming an
// unknown session must answer FETCH_SESSION_ID_NOT_FOUND (70). A broker may legitimately
// answer session id 0 ("no session, always full fetch"); the observation records which.
func fetchSessions(ctx context.Context, env Env) (Observation, error) {
	f, err := newFetchTarget(ctx, env, "fetch-session", [][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		return nil, err
	}
	defer f.cl.Close()
	full, err := f.fetch(ctx, 0, 0, 1, 500, 1<<20, 1<<20, 0)
	if err != nil {
		return nil, fmt.Errorf("full fetch: %w", err)
	}
	obs := Observation{"full_fetch_error_code": int(full.ErrorCode), "session_created": full.SessionID != 0}
	if full.SessionID != 0 {
		inc, err := f.fetch(ctx, full.SessionID, 1, 1, 500, 1<<20, 1<<20, 2)
		if err != nil {
			return nil, fmt.Errorf("incremental fetch: %w", err)
		}
		obs["incremental_fetch_error_code"] = int(inc.ErrorCode)
	}
	bad, err := f.fetch(ctx, 0x7ffffff0, 1, 1, 500, 1<<20, 1<<20, 0)
	if err != nil {
		return nil, fmt.Errorf("unknown-session fetch: %w", err)
	}
	obs["unknown_session_error_code"] = int(bad.ErrorCode)
	return obs, nil
}

// fetchOversizedFirstRecord (KIP-74): with response and partition limits of 1 byte, the broker
// still returns the first record batch, so a consumer can always make progress.
func fetchOversizedFirstRecord(ctx context.Context, env Env) (Observation, error) {
	big := make([]byte, 10*1024)
	rand.New(rand.NewSource(7)).Read(big) // incompressible, so the stored batch really is >10 KiB
	f, err := newFetchTarget(ctx, env, "fetch-kip74", [][]byte{big, []byte("second")})
	if err != nil {
		return nil, err
	}
	defer f.cl.Close()
	resp, err := f.fetch(ctx, 0, -1, 1, 500, 1, 1, 0)
	if err != nil {
		return nil, err
	}
	p, ok := partition(resp)
	if !ok {
		return Observation{"partitions_in_response": false}, nil
	}
	// The limits must still bind after the first batch: the second record must not come back.
	return Observation{"error_code": int(p.ErrorCode), "first_batch_returned": len(p.RecordBatches) >= 10*1024,
		"second_record_returned": bytes.Contains(p.RecordBatches, []byte("second")), "high_watermark": p.HighWatermark}, nil
}

// fetchLongPoll: fetch.min.bytes not reached makes the broker wait fetch.max.wait.ms (2 s);
// data arriving during the wait answers early.
func fetchLongPoll(ctx context.Context, env Env) (Observation, error) {
	f, err := newFetchTarget(ctx, env, "fetch-longpoll", [][]byte{[]byte("x")})
	if err != nil {
		return nil, err
	}
	defer f.cl.Close()
	// Waits: nothing new at offset 1, min bytes 1 never reached.
	start := time.Now()
	_, err = f.fetch(ctx, 0, -1, 1, 2000, 1<<20, 1<<20, 1)
	if err != nil {
		return nil, err
	}
	waited := time.Since(start)
	// Early answer: a record arrives 500 ms into a 5 s wait.
	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = f.cl.ProduceSync(ctx, &kgo.Record{Topic: f.topic, Value: []byte("y")}).FirstErr()
	}()
	start = time.Now()
	resp, err := f.fetch(ctx, 0, -1, 1, 5000, 1<<20, 1<<20, 1)
	if err != nil {
		return nil, err
	}
	early := time.Since(start)
	p, _ := partition(resp)
	return Observation{
		"empty_poll_waited_at_least_1500ms": waited >= 1500*time.Millisecond,
		"empty_poll_returned_within_4000ms": waited < 4*time.Second,
		"data_answered_before_max_wait":     early < 4*time.Second,
		"data_returned":                     p != nil && len(p.RecordBatches) > 0,
	}, nil
}

func init() { register("fetch-min-bytes", fetchMinBytes) }

// fetchMinBytes: fetch.min.bytes=64 KiB with fetch.max.wait.ms=500. With 1 KiB available the
// broker waits out the 500 ms and then returns the data it has; with more than 64 KiB
// available it answers at once.
func fetchMinBytes(ctx context.Context, env Env) (Observation, error) {
	small := make([]byte, 1024)
	rand.New(rand.NewSource(11)).Read(small)
	f, err := newFetchTarget(ctx, env, "fetch-minbytes", [][]byte{small})
	if err != nil {
		return nil, err
	}
	defer f.cl.Close()
	start := time.Now()
	resp, err := f.fetch(ctx, 0, -1, 64*1024, 500, 1<<20, 1<<20, 0)
	if err != nil {
		return nil, err
	}
	waited := time.Since(start)
	p, _ := partition(resp)
	obs := Observation{"small_fetch_waited_at_least_400ms": waited >= 400*time.Millisecond, "small_fetch_returned_data": p != nil && len(p.RecordBatches) > 0}
	pr, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.DefaultProduceTopic(f.topic), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	big := make([]byte, 8*1024)
	rnd := rand.New(rand.NewSource(12))
	for i := 0; i < 10; i++ {
		rnd.Read(big)
		if err := pr.ProduceSync(ctx, &kgo.Record{Value: append([]byte(nil), big...)}).FirstErr(); err != nil {
			return Observation{"produce_error_code": errCode(err)}, nil
		}
	}
	start = time.Now()
	resp, err = f.fetch(ctx, 0, -1, 64*1024, 500, 1<<20, 1<<20, 1)
	if err != nil {
		return nil, err
	}
	took := time.Since(start)
	p, _ = partition(resp)
	obs["large_fetch_answered_within_400ms"] = took < 400*time.Millisecond
	obs["large_fetch_returned_at_least_64KiB"] = p != nil && len(p.RecordBatches) >= 64*1024
	return obs, nil
}
