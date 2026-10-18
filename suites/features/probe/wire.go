package probe

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Raw-protocol rows ported from kubemq-server tests/kafka-conformance host-mode legs
// (conformance_apiversions_test.go, conformance_listoffsets_timestamps_test.go,
// conformance_franzgo_reset_test.go), re-expressed as oracle-vs-KubeMQ observations.

func init() {
	register("wire-apiversions-downgrade", apiVersionsDowngrade)
	register("wire-listoffsets-in-batch", listOffsetsInBatch)
	register("wire-listoffsets-layouts", listOffsetsLayouts)
	register("wire-listoffsets-transactions", listOffsetsTransactions)
	register("wire-out-of-range-reset", outOfRangeReset)
}

// --- ApiVersions downgrade ---------------------------------------------------------------

func encodeAPIVersions(version int16, corr int32) []byte {
	var p []byte
	p = binary.BigEndian.AppendUint16(p, 18)
	p = binary.BigEndian.AppendUint16(p, uint16(version))
	p = binary.BigEndian.AppendUint32(p, uint32(corr))
	p = binary.BigEndian.AppendUint16(p, uint16(len("kcert")))
	p = append(p, "kcert"...)
	if version >= 3 {
		p = binary.AppendUvarint(p, 0)
		for _, s := range []string{"kcert-probe", "1.0.0"} {
			p = binary.AppendUvarint(p, uint64(len(s)+1))
			p = append(p, s...)
		}
		if version >= 5 {
			p = binary.AppendUvarint(p, 0)
			p = binary.BigEndian.AppendUint32(p, 0xFFFFFFFF)
		}
		p = binary.AppendUvarint(p, 0)
	}
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(p))), p...)
}

type apiVersionsReply struct {
	corr     int32
	code     int16
	key18Max int16
	v0Body   bool
}

func readAPIVersions(conn net.Conn, version int16) (apiVersionsReply, error) {
	var sz [4]byte
	if _, err := io.ReadFull(conn, sz[:]); err != nil {
		return apiVersionsReply{}, err
	}
	buf := make([]byte, binary.BigEndian.Uint32(sz[:]))
	if _, err := io.ReadFull(conn, buf); err != nil {
		return apiVersionsReply{}, err
	}
	r := apiVersionsReply{corr: int32(binary.BigEndian.Uint32(buf[0:4])), key18Max: -1}
	body := buf[4:]
	r.code = int16(binary.BigEndian.Uint16(body[0:2]))
	var keys []kmsg.ApiVersionsResponseApiKey
	if r.code == 35 {
		r.v0Body = true
		n := int(binary.BigEndian.Uint32(body[2:6]))
		for i, off := 0, 6; i < n && off+6 <= len(body); i, off = i+1, off+6 {
			keys = append(keys, kmsg.ApiVersionsResponseApiKey{ApiKey: int16(binary.BigEndian.Uint16(body[off:])),
				MinVersion: int16(binary.BigEndian.Uint16(body[off+2:])), MaxVersion: int16(binary.BigEndian.Uint16(body[off+4:]))})
		}
	} else {
		resp := kmsg.ApiVersionsResponse{Version: version}
		if err := resp.ReadFrom(body); err != nil {
			return r, err
		}
		keys = resp.ApiKeys
	}
	for _, k := range keys {
		if k.ApiKey == 18 {
			r.key18Max = k.MaxVersion
		}
	}
	return r, nil
}

// apiVersionsDowngrade: on one connection, ApiVersions v0 learns the broker's own maximum M for
// key 18; a request at M+1 must be answered UNSUPPORTED_VERSION (35) with a v0 body listing M,
// with the correlation id echoed and the connection left open; requests at M and v3 then
// succeed on the same connection. This is the negotiation every client does on connect.
func apiVersionsDowngrade(ctx context.Context, env Env) (Observation, error) {
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", env.Bootstrap)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	send := func(v int16, corr int32) (apiVersionsReply, error) {
		if _, err := conn.Write(encodeAPIVersions(v, corr)); err != nil {
			return apiVersionsReply{}, err
		}
		return readAPIVersions(conn, v)
	}
	base, err := send(0, 1)
	if err != nil {
		return nil, fmt.Errorf("ApiVersions v0: %w", err)
	}
	m := base.key18Max
	obs := Observation{"v0_error_code": int(base.code)}
	above, err := send(m+1, 2)
	if err != nil {
		obs["connection_open_after_unsupported"] = false
		return obs, nil
	}
	obs["above_max_error_code"] = int(above.code)
	obs["above_max_body_is_v0"] = above.v0Body
	obs["above_max_lists_own_max"] = above.key18Max == m
	obs["above_max_correlation_echoed"] = above.corr == 2
	at, err := send(m, 3)
	obs["connection_open_after_unsupported"] = err == nil
	if err == nil {
		obs["at_max_error_code"] = int(at.code)
		obs["at_max_correlation_echoed"] = at.corr == 3
	}
	v3, err := send(3, 4)
	if err == nil {
		obs["v3_error_code"] = int(v3.code)
	}
	return obs, nil
}

// --- ListOffsets by timestamp --------------------------------------------------------------

// batchProducer writes each call's records as exactly one batch with the given codec.
// keepForever turns off time-based retention (retention.ms=-1) for topics whose records carry
// timestamps from 1970. Kafka's default retention (7 days) otherwise deletes their segment as soon
// as a later append rolls it (the log.roll.ms check also sees the 1970 timestamp), within the
// oracle's 1 s retention check, so lookups would race the deletion of the records they look for.
var keepForever = map[string]*string{"retention.ms": str("-1")}

func produceBatch(ctx context.Context, env Env, topic string, codec kgo.CompressionCodec, tss []int64, opts ...kgo.Opt) error {
	base := []kgo.Opt{kgo.SeedBrokers(env.Bootstrap), kgo.DefaultProduceTopic(topic), kgo.ProducerBatchCompression(codec),
		kgo.ProducerLinger(time.Second), kgo.RecordPartitioner(kgo.ManualPartitioner())}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return err
	}
	defer cl.Close()
	var firstErr error
	done := make(chan struct{}, len(tss))
	for i, ts := range tss {
		v := make([]byte, 200)
		for j := range v {
			v[j] = byte('a' + (i+j)%26)
		}
		cl.Produce(ctx, &kgo.Record{Value: v, Timestamp: time.UnixMilli(ts)}, func(_ *kgo.Record, err error) {
			if err != nil && firstErr == nil {
				firstErr = err
			}
			done <- struct{}{}
		})
	}
	if err := cl.Flush(ctx); err != nil {
		return err
	}
	for range tss {
		<-done
	}
	return firstErr
}

// listOffsetsAt sends a raw ListOffsets for partition 0 and returns "offset@timestamp" or the
// error code, with the timestamp relative to base (or "-1" when the broker answers -1).
func listOffsetsAt(ctx context.Context, cl *kgo.Client, topic string, ts int64, isolation int8, base int64) string {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID, req.IsolationLevel = -1, isolation
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = topic
	p := kmsg.NewListOffsetsRequestTopicPartition()
	p.Partition, p.Timestamp, p.CurrentLeaderEpoch = 0, ts, -1
	t.Partitions = append(t.Partitions, p)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return "request error"
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		return "malformed"
	}
	r := resp.Topics[0].Partitions[0]
	if r.ErrorCode != 0 {
		return fmt.Sprintf("error %d", r.ErrorCode)
	}
	rel := "-1"
	if r.Timestamp >= 0 {
		rel = fmt.Sprint(r.Timestamp - base)
	}
	return fmt.Sprintf("offset %d at %s", r.Offset, rel)
}

func deleteRecordsTo(ctx context.Context, adm *kadm.Client, topic string, at int64) error {
	var os kadm.Offsets
	os.Add(kadm.Offset{Topic: topic, Partition: 0, At: at, LeaderEpoch: -1})
	r, err := adm.DeleteRecords(ctx, os)
	if err != nil {
		return err
	}
	d, _ := r.Lookup(topic, 0)
	return d.Err
}

// listOffsetsInBatch: batch [1000,2000,3000] uncompressed, then [5000,4000,6000] compressed with
// each codec; by-timestamp and max-timestamp answers must come from the records' own
// timestamps inside the compressed batch, before and after DeleteRecords to offset 4.
func listOffsetsInBatch(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	obs := Observation{}
	codecs := map[string]kgo.CompressionCodec{"gzip": kgo.GzipCompression(), "snappy": kgo.SnappyCompression(), "lz4": kgo.Lz4Compression(), "zstd": kgo.ZstdCompression()}
	names := []string{"gzip", "lz4", "snappy", "zstd"}
	for _, name := range names {
		topic := topicName(env, "lo-inbatch-"+name)
		if err := createTopic(ctx, adm, topic, 1, keepForever); err != nil {
			return nil, err
		}
		if err := produceBatch(ctx, env, topic, kgo.NoCompression(), []int64{1000, 2000, 3000}); err != nil {
			return Observation{name + "_produce_error_code": errCode(err)}, nil
		}
		if err := produceBatch(ctx, env, topic, codecs[name], []int64{5000, 4000, 6000}); err != nil {
			return Observation{name + "_produce_error_code": errCode(err)}, nil
		}
		look := func(stage string) {
			for _, q := range []int64{2500, 4500, 5500, 7000, -3} {
				obs[fmt.Sprintf("%s_%s_ts_%d", name, stage, q)] = listOffsetsAt(ctx, cl, topic, q, 0, 0)
			}
		}
		look("before")
		if err := deleteRecordsTo(ctx, adm, topic, 4); err != nil {
			obs[name+"_delete_records_error_code"] = errCode(err)
			continue
		}
		look("after_delete_to_4")
	}
	return obs, nil
}

// listOffsetsLayouts: layouts where answering from anything but the records' own timestamps
// shows: non-monotonic single-record batches, a tie inside one batch, max-timestamp stored
// below the log start after DeleteRecords, and an empty topic.
func listOffsetsLayouts(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	obs := Observation{}
	mk := func(name string) (string, error) {
		t := topicName(env, "lo-"+name)
		return t, createTopic(ctx, adm, t, 1, keepForever)
	}
	// Non-monotonic: single-record batches [100,300,200,300].
	t, err := mk("nonmono")
	if err != nil {
		return nil, err
	}
	for _, ts := range []int64{100, 300, 200, 300} {
		if err := produceBatch(ctx, env, t, kgo.NoCompression(), []int64{ts}); err != nil {
			return Observation{"produce_error_code": errCode(err)}, nil
		}
	}
	for _, q := range []int64{150, 250, 300, -3} {
		obs[fmt.Sprintf("nonmono_ts_%d", q)] = listOffsetsAt(ctx, cl, t, q, 0, 0)
	}
	// Tie: one batch [500,100,500].
	if t, err = mk("tie"); err != nil {
		return nil, err
	}
	if err := produceBatch(ctx, env, t, kgo.NoCompression(), []int64{500, 100, 500}); err != nil {
		return Observation{"produce_error_code": errCode(err)}, nil
	}
	for _, q := range []int64{100, 500, -3} {
		obs[fmt.Sprintf("tie_ts_%d", q)] = listOffsetsAt(ctx, cl, t, q, 0, 0)
	}
	// Below log start: batches [1000,9000] [3000,2000] [4000]; DeleteRecords to 2, then to 5.
	if t, err = mk("belowstart"); err != nil {
		return nil, err
	}
	for _, b := range [][]int64{{1000, 9000}, {3000, 2000}, {4000}} {
		if err := produceBatch(ctx, env, t, kgo.NoCompression(), b); err != nil {
			return Observation{"produce_error_code": errCode(err)}, nil
		}
	}
	for _, at := range []int64{2, 5} {
		if err := deleteRecordsTo(ctx, adm, t, at); err != nil {
			obs[fmt.Sprintf("belowstart_delete_to_%d_error_code", at)] = errCode(err)
			continue
		}
		for _, q := range []int64{1500, 9000, -3, -2} {
			obs[fmt.Sprintf("belowstart_after_delete_to_%d_ts_%d", at, q)] = listOffsetsAt(ctx, cl, t, q, 0, 0)
		}
	}
	// Empty topic.
	if t, err = mk("empty"); err != nil {
		return nil, err
	}
	for _, q := range []int64{1000, -3, -2, -1} {
		obs[fmt.Sprintf("empty_ts_%d", q)] = listOffsetsAt(ctx, cl, t, q, 0, 0)
	}
	return obs, nil
}

// listOffsetsTransactions: a committed record at 1000 and an open transaction's record at 5000;
// by-timestamp, max-timestamp and latest under read_uncommitted and read_committed; then the
// transaction aborts and the lookups repeat (the abort marker is a record of its own).
func listOffsetsTransactions(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	t := topicName(env, "lo-txn")
	if err := createTopic(ctx, adm, t, 1, keepForever); err != nil {
		return nil, err
	}
	if err := produceBatch(ctx, env, t, kgo.NoCompression(), []int64{1000}); err != nil {
		return Observation{"produce_error_code": errCode(err)}, nil
	}
	tx, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.DefaultProduceTopic(t), kgo.TransactionalID(t+"-tx"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return nil, err
	}
	defer tx.Close()
	if err := tx.BeginTransaction(); err != nil {
		return nil, err
	}
	if err := tx.ProduceSync(ctx, &kgo.Record{Value: []byte("open"), Timestamp: time.UnixMilli(5000)}).FirstErr(); err != nil {
		return Observation{"txn_produce_error_code": errCode(err)}, nil
	}
	obs := Observation{}
	look := func(stage string) {
		for _, iso := range []int8{0, 1} {
			name := map[int8]string{0: "uncommitted", 1: "committed"}[iso]
			for _, q := range []int64{2000, -3, -1} {
				obs[fmt.Sprintf("%s_%s_ts_%d", stage, name, q)] = listOffsetsAt(ctx, cl, t, q, iso, 0)
			}
		}
	}
	look("open")
	if err := tx.EndTransaction(ctx, kgo.TryAbort); err != nil {
		return nil, err
	}
	time.Sleep(500 * time.Millisecond) // let the abort marker land
	look("aborted")
	// The marker carries the abort time: normalize max-timestamp answers that name it.
	for k, v := range obs {
		s := v.(string)
		var off, ts int64
		if n, _ := fmt.Sscanf(s, "offset %d at %d", &off, &ts); n == 2 && ts > 1e12 {
			obs[k] = fmt.Sprintf("offset %d at NOW", off)
		}
	}
	return obs, nil
}

// outOfRangeReset: 10 records with CreateTime T-10min then 10 with CreateTime T, all produced
// after T. A consumer whose first fetch is at offset 1000 (past the end) resumes with franz-go
// v1.22's default policy (it rewinds by time; with nothing consumed yet it lands on the log
// start) and with an explicit by-timestamp reset to T. Kafka answers the by-timestamp lookup
// from CreateTime, so the second resumes at offset 10.
func outOfRangeReset(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	t := topicName(env, "oor-reset")
	if err := createTopic(ctx, adm, t, 1, nil); err != nil {
		return nil, err
	}
	T := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	pr, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.DefaultProduceTopic(t))
	if err != nil {
		return nil, err
	}
	for i := 0; i < 20; i++ {
		ts := T.Add(-10 * time.Minute)
		if i >= 10 {
			ts = T
		}
		if err := pr.ProduceSync(ctx, &kgo.Record{Value: []byte(fmt.Sprint(i)), Timestamp: ts}).FirstErr(); err != nil {
			pr.Close()
			return Observation{"produce_error_code": errCode(err)}, nil
		}
	}
	pr.Close()
	resume := func(reset *kgo.Offset) int64 {
		opts := []kgo.Opt{kgo.SeedBrokers(env.Bootstrap), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{t: {0: kgo.NewOffset().At(1000)}}),
			kgo.FetchMaxWait(300 * time.Millisecond)}
		if reset != nil {
			opts = append(opts, kgo.ConsumeResetOffset(*reset))
		}
		c, err := kgo.NewClient(opts...)
		if err != nil {
			return -2
		}
		defer c.Close()
		cc, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		for cc.Err() == nil {
			var first int64 = -1
			c.PollFetches(cc).EachRecord(func(r *kgo.Record) {
				if first < 0 {
					first = r.Offset
				}
			})
			if first >= 0 {
				return first
			}
		}
		return -1
	}
	byTime := kgo.NewOffset().AfterMilli(T.UnixMilli())
	return Observation{"default_reset_resumes_at": resume(nil), "reset_after_milli_T_resumes_at": resume(&byTime)}, nil
}
