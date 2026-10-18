package probe

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	register("log-timestamp-createtime", timestampCreateTime)
	register("log-timestamp-logappendtime", timestampLogAppendTime)
	register("log-timestamp-validation", timestampValidation)
	register("log-listoffsets-specs", listOffsetsSpecs)
	register("log-compaction", compactionProbe(false))
	register("log-compaction-batched-compressed", compactionProbe(true))
	register("log-retention-ms", retentionMs)
	register("log-retention-ms-zero", retentionMsZero)
	register("log-compaction-dirty-ratio", compactionKnob(false))
	register("log-compaction-max-lag", compactionKnob(true))
	register("log-retention-bytes", retentionBytesProbe(false))
	register("log-retention-bytes-compact-delete", retentionBytesProbe(true))
}

func str(s string) *string { return &s }

// createTopicCode creates a topic and returns the Kafka error code instead of failing, for
// probes where refusing the topic is itself the observation.
func createTopicCode(ctx context.Context, adm *kadm.Client, topic string, partitions int32, configs map[string]*string) int {
	resp, err := adm.CreateTopic(ctx, partitions, 1, configs, topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		return errCode(err)
	}
	if err := createTopic(ctx, adm, topic, partitions, nil); err != nil && errCode(err) != int(kerr.TopicAlreadyExists.Code) {
		return -1
	}
	return 0
}

func producer(env Env, topic string, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{kgo.SeedBrokers(env.Bootstrap), kgo.DefaultProduceTopic(topic), kgo.RecordPartitioner(kgo.ManualPartitioner())}
	return kgo.NewClient(append(base, opts...)...)
}

// timestampCreateTime: on a default topic the broker keeps the producer's timestamps.
func timestampCreateTime(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := topicName(env, "ts-create")
	if err := createTopic(ctx, adm, topic, 1, nil); err != nil {
		return nil, err
	}
	pr, err := producer(env, topic)
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	base := time.Now().Add(-10 * time.Minute).Truncate(time.Millisecond)
	for i := 0; i < 20; i++ {
		if err := pr.ProduceSync(ctx, &kgo.Record{Value: []byte(fmt.Sprint(i)), Timestamp: base.Add(time.Duration(i) * time.Second)}).FirstErr(); err != nil {
			return Observation{"produce_error_code": errCode(err)}, nil
		}
	}
	recs, err := consumeAll(ctx, env, topic, 20)
	matches, types := 0, map[string]bool{}
	for i, r := range recs {
		if i < 20 && r.Timestamp.Equal(base.Add(time.Duration(i)*time.Second)) {
			matches++
		}
		types[tsType(r)] = true
	}
	obs := Observation{"consumed": len(recs), "timestamps_as_sent": matches, "timestamp_types": sortedKeys(types)}
	if err != nil {
		obs["consume_error"] = "incomplete"
	}
	return obs, nil
}

func tsType(r *kgo.Record) string {
	if r.Attrs.TimestampType() == 1 {
		return "LogAppendTime"
	}
	return "CreateTime"
}

// timestampLogAppendTime: with message.timestamp.type=LogAppendTime the broker overwrites the
// producer's (old) timestamp with its append time.
func timestampLogAppendTime(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := topicName(env, "ts-append")
	if code := createTopicCode(ctx, adm, topic, 1, map[string]*string{"message.timestamp.type": str("LogAppendTime")}); code != 0 {
		return Observation{"create_error_code": code}, nil
	}
	pr, err := producer(env, topic)
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	old := time.Now().Add(-48 * time.Hour).Truncate(time.Millisecond)
	// The broker stamps with its own clock (the engine VM's, within the 60 s preflight bound);
	// the producer timestamp is 48 h away, so a 90 s window cannot confuse the two.
	start := time.Now().Add(-90 * time.Second)
	for i := 0; i < 10; i++ {
		if err := pr.ProduceSync(ctx, &kgo.Record{Value: []byte(fmt.Sprint(i)), Timestamp: old}).FirstErr(); err != nil {
			return Observation{"produce_error_code": errCode(err)}, nil
		}
	}
	end := time.Now().Add(90 * time.Second)
	recs, err := consumeAll(ctx, env, topic, 10)
	inWindow, kept, types := 0, 0, map[string]bool{}
	for _, r := range recs {
		if !r.Timestamp.Before(start) && !r.Timestamp.After(end) {
			inWindow++
		}
		if r.Timestamp.Equal(old) {
			kept++
		}
		types[tsType(r)] = true
	}
	obs := Observation{"create_error_code": 0, "consumed": len(recs), "timestamps_at_append_time": inWindow, "producer_timestamps_kept": kept, "timestamp_types": sortedKeys(types)}
	if err != nil {
		obs["consume_error"] = "incomplete"
	}
	return obs, nil
}

// timestampValidation: message.timestamp.before.max.ms and after.max.ms (KIP-937) refuse
// records too far in the past or future with INVALID_TIMESTAMP (32); the default topic
// configuration is observed too.
func timestampValidation(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	strict, deflt := topicName(env, "ts-strict"), topicName(env, "ts-default")
	if err := createTopic(ctx, adm, strict, 1, map[string]*string{
		"message.timestamp.before.max.ms": str("3600000"), "message.timestamp.after.max.ms": str("3600000")}); err != nil {
		return nil, err
	}
	if err := createTopic(ctx, adm, deflt, 1, nil); err != nil {
		return nil, err
	}
	pr, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.RecordRetries(1))
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	code := func(topic string, offset time.Duration) int {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return errCode(pr.ProduceSync(c, &kgo.Record{Topic: topic, Value: []byte("x"), Timestamp: time.Now().Add(offset)}).FirstErr())
	}
	return Observation{
		"strict_now_error_code":        code(strict, 0),
		"strict_2h_past_error_code":    code(strict, -2*time.Hour),
		"strict_2h_future_error_code":  code(strict, 2*time.Hour),
		"default_30d_past_error_code":  code(deflt, -30*24*time.Hour),
		"default_2h_future_error_code": code(deflt, 2*time.Hour),
	}, nil
}

// listOffsetsSpecs: earliest, latest, by timestamp and max-timestamp (-3) on a fresh topic of
// ten records whose timestamps are t0, t0+1s, ... and one out-of-order record carrying the
// largest timestamp in the middle.
func listOffsetsSpecs(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := topicName(env, "listoffsets")
	if err := createTopic(ctx, adm, topic, 1, nil); err != nil {
		return nil, err
	}
	pr, err := producer(env, topic)
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	t0 := time.Now().Add(-time.Hour).Truncate(time.Second)
	for i := 0; i < 10; i++ {
		ts := t0.Add(time.Duration(i) * time.Second)
		if i == 4 {
			ts = t0.Add(time.Minute) // the largest timestamp, at offset 4
		}
		if err := pr.ProduceSync(ctx, &kgo.Record{Value: []byte(fmt.Sprint(i)), Timestamp: ts}).FirstErr(); err != nil {
			return Observation{"produce_error_code": errCode(err)}, nil
		}
	}
	obs := Observation{}
	record := func(key string, lo kadm.ListedOffsets, err error) {
		if err != nil {
			obs[key] = "request error"
			return
		}
		o, _ := lo.Lookup(topic, 0)
		obs[key] = o.Offset
	}
	s, err := adm.ListStartOffsets(ctx, topic)
	record("earliest", s, err)
	e, err := adm.ListEndOffsets(ctx, topic)
	record("latest", e, err)
	a, err := adm.ListOffsetsAfterMilli(ctx, t0.Add(5500*time.Millisecond).UnixMilli(), topic)
	record("by_timestamp_t0_plus_5_5s", a, err)
	a, err = adm.ListOffsetsAfterMilli(ctx, t0.Add(2*time.Hour).UnixMilli(), topic)
	record("by_timestamp_after_all", a, err)
	// Max-timestamp (-3) is not in kadm; ask with a raw ListOffsets request.
	req := kmsg.NewPtrListOffsetsRequest()
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition, rp.Timestamp = 0, -3
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	if resp, err := req.RequestWith(ctx, cl); err == nil && len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 {
		p := resp.Topics[0].Partitions[0]
		obs["max_timestamp_error_code"] = int(p.ErrorCode)
		obs["max_timestamp_offset"] = p.Offset
		obs["max_timestamp_is_largest"] = p.Timestamp == t0.Add(time.Minute).UnixMilli()
	} else {
		obs["max_timestamp_error_code"] = "request error"
	}
	return obs, nil
}

// readAll reads a topic from the start until the end offset known now, returning every record.
func readAll(ctx context.Context, env Env, adm *kadm.Client, topic string) ([]*kgo.Record, error) {
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	e, _ := ends.Lookup(topic, 0)
	s, _ := starts.Lookup(topic, 0)
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
		kgo.FetchMaxWait(300*time.Millisecond))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var out []*kgo.Record
	if e.Offset <= s.Offset {
		return out, nil
	}
	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		fs := cl.PollFetches(c)
		if c.Err() != nil {
			return out, nil
		}
		last := int64(-1)
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r); last = r.Offset })
		if last >= e.Offset-1 {
			return out, nil
		}
	}
}

// compactionProbe: cleanup.policy=compact keeps only the latest value per key and, after
// delete.retention.ms, removes tombstones. Segments roll every second (a small record is
// produced every 2 s to keep rolling); the probe waits up to 120 s for each effect.
// Unbatched: one uncompressed record per produce request. Batched: the same records sent
// asynchronously in lz4 batches, the way Kafka Connect and Kafka Streams write.
func compactionProbe(batched bool) Probe {
	return func(ctx context.Context, env Env) (Observation, error) { return compaction(ctx, env, batched) }
}

func compaction(ctx context.Context, env Env, batched bool) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := topicName(env, "compact")
	if err := createTopic(ctx, adm, topic, 1, map[string]*string{
		"cleanup.policy": str("compact"), "segment.ms": str("1000"), "min.cleanable.dirty.ratio": str("0.01"),
		"delete.retention.ms": str("5000"), "min.compaction.lag.ms": str("0")}); err != nil {
		return nil, err
	}
	codec := kgo.ProducerBatchCompression(kgo.NoCompression())
	if batched {
		codec = kgo.ProducerBatchCompression(kgo.Lz4Compression())
	}
	pr, err := producer(env, topic, codec, kgo.ProducerLinger(50*time.Millisecond))
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	for v := 1; v <= 5; v++ {
		for k := 0; k < 10; k++ {
			r := &kgo.Record{Key: []byte(fmt.Sprintf("k%d", k)), Value: []byte(fmt.Sprintf("v%d-%s", v, "payload-payload-payload-payload"))}
			if batched {
				pr.Produce(ctx, r, nil)
				continue
			}
			if err := pr.ProduceSync(ctx, r).FirstErr(); err != nil {
				return Observation{"produce_error_code": errCode(err)}, nil
			}
		}
	}
	if err := pr.Flush(ctx); err != nil {
		return nil, err
	}
	if err := pr.ProduceSync(ctx, &kgo.Record{Key: []byte("k0"), Value: nil}).FirstErr(); err != nil {
		return Observation{"tombstone_error_code": errCode(err)}, nil
	}
	compacted, latestOK, tombstoneGone := waitCompacted(ctx, env, adm, pr, topic)
	return Observation{"compacted_within_120s": compacted, "latest_value_per_key_kept": latestOK, "tombstone_removed_within_120s": tombstoneGone}, nil
}

// waitCompacted produces a small "roll" record every 4 s (so segments keep rolling) and reads the
// topic back until keys k1..k9 hold one value each and the k0 tombstone is gone, for up to 120 s.
// It reports whether the log was compacted, whether the kept values are the latest (v5-), and
// whether the tombstone was removed.
func waitCompacted(ctx context.Context, env Env, adm *kadm.Client, pr *kgo.Client, topic string) (compacted, latestOK, tombstoneGone bool) {
	deadline := time.Now().Add(120 * time.Second)
	roll := 0
	for time.Now().Before(deadline) && !(compacted && tombstoneGone) {
		time.Sleep(4 * time.Second)
		roll++
		_ = pr.ProduceSync(ctx, &kgo.Record{Key: []byte("roll"), Value: []byte(fmt.Sprint(roll))}).FirstErr()
		recs, err := readAll(ctx, env, adm, topic)
		if err != nil {
			continue
		}
		perKey := map[string][]string{}
		for _, r := range recs {
			if string(r.Key) == "roll" {
				continue
			}
			v := "<tombstone>"
			if r.Value != nil {
				v = string(r.Value)
			}
			perKey[string(r.Key)] = append(perKey[string(r.Key)], v)
		}
		single := true
		latest := true
		for k := 1; k < 10; k++ {
			vs := perKey[fmt.Sprintf("k%d", k)]
			if len(vs) != 1 {
				single = false
			}
			if len(vs) == 0 || !strings.HasPrefix(vs[len(vs)-1], "v5-") {
				latest = false
			}
		}
		k0 := perKey["k0"]
		if single && (len(k0) == 0 || (len(k0) == 1 && k0[0] == "<tombstone>")) {
			compacted = true
			latestOK = latest
		}
		if compacted && len(k0) == 0 {
			tombstoneGone = true
		}
	}
	return compacted, latestOK, tombstoneGone
}

// retentionMs: retention.ms=2000 with segments rolling every second; the log start offset must
// advance past the first records within 120 s.
func retentionMs(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := topicName(env, "retention-ms")
	if err := createTopic(ctx, adm, topic, 1, map[string]*string{"retention.ms": str("2000"), "segment.ms": str("1000")}); err != nil {
		return nil, err
	}
	pr, err := producer(env, topic)
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	for i := 0; i < 20; i++ {
		if err := pr.ProduceSync(ctx, &kgo.Record{Value: []byte(fmt.Sprint(i))}).FirstErr(); err != nil {
			return Observation{"produce_error_code": errCode(err)}, nil
		}
	}
	return Observation{"log_start_advanced_past_first_records_within_120s": waitLogStart(ctx, adm, pr, topic, 20, 120*time.Second)}, nil
}

// retentionMsZero: retention.ms=0 makes every closed segment eligible for deletion at once;
// Kafka 4.x deletes them on the next retention check and keeps only the active segment. Segments
// roll every second (a record is produced every 4 s). Observed: whether the create is accepted,
// how the topic describes retention.ms, whether the log start passes the first 20 records within
// 120 s, and whether the newest record is still in the log afterwards (the active segment kept).
func retentionMsZero(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := topicName(env, "retention-ms-zero")
	obs := Observation{"create_error_code": createTopicCode(ctx, adm, topic, 1, map[string]*string{"retention.ms": str("0"), "segment.ms": str("1000")})}
	if obs["create_error_code"] != 0 {
		return obs, nil
	}
	obs["retention_ms_described"] = describeTopicConfig(ctx, adm, topic, "retention.ms")
	pr, err := producer(env, topic)
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	for i := 0; i < 20; i++ {
		if err := pr.ProduceSync(ctx, &kgo.Record{Value: []byte(fmt.Sprint(i))}).FirstErr(); err != nil {
			obs["produce_error_code"] = errCode(err)
			return obs, nil
		}
	}
	obs["log_start_advanced_past_first_records_within_120s"] = waitLogStart(ctx, adm, pr, topic, 20, 120*time.Second)
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	s, _ := starts.Lookup(topic, 0)
	e, _ := ends.Lookup(topic, 0)
	obs["newest_record_still_in_log"] = s.Offset < e.Offset
	return obs, nil
}

// describeTopicConfig: "value=<v> source=<s>" for one topic config, "absent", or "error <code>".
func describeTopicConfig(ctx context.Context, adm *kadm.Client, topic, key string) string {
	rc, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return fmt.Sprintf("error %d", errCode(err))
	}
	r, err := rc.On(topic, nil)
	if err != nil {
		return fmt.Sprintf("error %d", errCode(err))
	}
	for _, c := range r.Configs {
		if c.Key == key {
			return fmt.Sprintf("value=%s source=%s", c.MaybeValue(), c.Source)
		}
	}
	return "absent"
}

// retentionBytesProbe: retention.bytes=1048576 with 1 MiB segments; 3 MiB produced; the log start
// must advance within 120 s. Refusing the configuration is observed as the create error code.
// With compactDelete the topic is cleanup.policy=compact,delete, where both apply: every record
// has its own key, so compaction removes nothing and only the size bound can trim the log.
func retentionBytesProbe(compactDelete bool) Probe {
	return func(ctx context.Context, env Env) (Observation, error) {
		return retentionBytes(ctx, env, compactDelete)
	}
}

func retentionBytes(ctx context.Context, env Env, compactDelete bool) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := topicName(env, "retention-bytes")
	configs := map[string]*string{"retention.bytes": str("1048576"), "segment.bytes": str("1048576")}
	if compactDelete {
		topic = topicName(env, "retention-bytes-cd")
		configs["cleanup.policy"] = str("compact,delete")
	}
	code := createTopicCode(ctx, adm, topic, 1, configs)
	obs := Observation{"create_error_code": code}
	if code != 0 {
		return obs, nil
	}
	pr, err := producer(env, topic, kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	payload := make([]byte, 10*1024)
	for i := 0; i < 300; i++ {
		r := &kgo.Record{Value: payload}
		if compactDelete {
			r.Key = []byte(fmt.Sprintf("k%03d", i))
		}
		if err := pr.ProduceSync(ctx, r).FirstErr(); err != nil {
			obs["produce_error_code"] = errCode(err)
			return obs, nil
		}
	}
	obs["log_start_advanced_within_120s"] = waitLogStart(ctx, adm, pr, topic, 1, 120*time.Second)
	return obs, nil
}

// waitLogStart polls the earliest offset until it reaches past, producing a small record every
// 2 s so time-based segments keep rolling.
func waitLogStart(ctx context.Context, adm *kadm.Client, pr *kgo.Client, topic string, past int64, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if s, err := adm.ListStartOffsets(ctx, topic); err == nil {
			if o, ok := s.Lookup(topic, 0); ok && o.Offset >= past {
				return true
			}
		}
		time.Sleep(4 * time.Second)
		_ = pr.ProduceSync(ctx, &kgo.Record{Value: []byte("tick")}).FirstErr()
	}
	return false
}

// compactionKnob tests min.cleanable.dirty.ratio and max.compaction.lag.ms. The topic starts
// compacted with min.cleanable.dirty.ratio=0.01 over a large base (1000 keys); a first
// duplicate shows when the first cleaning pass has happened. The ratio is then raised to 0.9
// (and, for the lag row, max.compaction.lag.ms set to 10000) with an incremental config alter,
// and a second duplicate is a tiny dirty share, far below 0.9:
//   - without a lag bound it must stay uncompacted for 45 s (the ratio is honoured);
//   - with max.compaction.lag.ms=10000 it must be compacted within 60 s (the lag forces it).
func compactionKnob(withLag bool) Probe {
	return func(ctx context.Context, env Env) (Observation, error) {
		cl, adm, err := adminClient(env)
		if err != nil {
			return nil, err
		}
		defer cl.Close()
		cfg := map[string]*string{"cleanup.policy": str("compact"), "segment.ms": str("1000"),
			"min.cleanable.dirty.ratio": str("0.01"), "min.compaction.lag.ms": str("0"), "delete.retention.ms": str("86400000")}
		name := "dirty-ratio"
		if withLag {
			name = "max-lag"
		}
		topic := topicName(env, name)
		if err := createTopic(ctx, adm, topic, 1, cfg); err != nil {
			return nil, err
		}
		pr, err := producer(env, topic, kgo.ProducerBatchCompression(kgo.NoCompression()))
		if err != nil {
			return nil, err
		}
		defer pr.Close()
		put := func(k, v string) error {
			return pr.ProduceSync(ctx, &kgo.Record{Key: []byte(k), Value: []byte(v)}).FirstErr()
		}
		pad := strings.Repeat("p", 100)
		for i := 0; i < 1000; i++ {
			if err := put(fmt.Sprintf("base-%04d", i), pad); err != nil {
				return Observation{"produce_error_code": errCode(err)}, nil
			}
		}
		if err := put("first", "1"); err != nil {
			return Observation{"produce_error_code": errCode(err)}, nil
		}
		if err := put("first", "2"); err != nil {
			return Observation{"produce_error_code": errCode(err)}, nil
		}
		versions := func(key string) int {
			recs, err := readAll(ctx, env, adm, topic)
			if err != nil {
				return -1
			}
			n := 0
			for _, r := range recs {
				if string(r.Key) == key {
					n++
				}
			}
			return n
		}
		roll := 0
		tick := func() { roll++; _ = put("roll", fmt.Sprint(roll)) }
		firstClean := false
		for deadline := time.Now().Add(120 * time.Second); time.Now().Before(deadline); {
			time.Sleep(4 * time.Second)
			tick()
			if versions("first") == 1 {
				firstClean = true
				break
			}
		}
		obs := Observation{"first_cleaning_pass_within_120s": firstClean}
		if !firstClean {
			return obs, nil
		}
		raise := []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "min.cleanable.dirty.ratio", Value: str("0.9")}}
		if withLag {
			raise = append(raise, kadm.AlterConfig{Op: kadm.SetConfig, Name: "max.compaction.lag.ms", Value: str("10000")})
		}
		ar, err := adm.AlterTopicConfigs(ctx, raise, topic)
		code := errCode(err)
		if err == nil && len(ar) > 0 {
			code = errCode(ar[0].Err)
		}
		obs["raise_ratio_error_code"] = code
		if code != 0 {
			return obs, nil
		}
		_ = put("second", "1")
		_ = put("second", "2")
		wait := 45 * time.Second
		if withLag {
			wait = 60 * time.Second
		}
		compacted := false
		for deadline := time.Now().Add(wait); time.Now().Before(deadline); {
			time.Sleep(4 * time.Second)
			tick()
			if versions("second") == 1 {
				compacted = true
				if withLag {
					break
				}
			}
		}
		if withLag {
			obs["small_dirty_share_compacted_by_lag_within_60s"] = compacted
		} else {
			obs["small_dirty_share_left_uncompacted_for_45s"] = !compacted
		}
		return obs, nil
	}
}
