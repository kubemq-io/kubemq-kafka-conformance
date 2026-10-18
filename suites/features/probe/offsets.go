package probe

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	register("offsets-commit-fetch", offsetsCommitFetch)
	register("offsets-commit-generation-checks", offsetsCommitGenerationChecks)
	register("offsets-multi-group-fetch", offsetsMultiGroupFetch)
	register("offsets-delete-records", offsetsDeleteRecords)
	register("offsets-delete-group-offsets", offsetsDeleteGroupOffsets)
}

func offsetTopic(ctx context.Context, env Env, name string, partitions int32, records int) (*kgo.Client, *kadm.Client, string, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, nil, "", err
	}
	topic := topicName(env, name)
	if err := createTopic(ctx, adm, topic, partitions, nil); err != nil {
		cl.Close()
		return nil, nil, "", err
	}
	pr, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		cl.Close()
		return nil, nil, "", err
	}
	defer pr.Close()
	for i := 0; i < records; i++ {
		r := &kgo.Record{Topic: topic, Partition: int32(i) % partitions, Value: []byte(fmt.Sprint(i))}
		if err := pr.ProduceSync(ctx, r).FirstErr(); err != nil {
			cl.Close()
			return nil, nil, "", fmt.Errorf("produce: %w", err)
		}
	}
	return cl, adm, topic, nil
}

// offsetsCommitFetch: an admin (simple, memberless) commit of offsets with metadata on two
// partitions is returned exactly by OffsetFetch, including the metadata string; a partition
// never committed reads -1.
func offsetsCommitFetch(ctx context.Context, env Env) (Observation, error) {
	cl, adm, topic, err := offsetTopic(ctx, env, "offsets", 3, 9)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	group := topic + "-g"
	var os kadm.Offsets
	os.Add(kadm.Offset{Topic: topic, Partition: 0, At: 2, LeaderEpoch: -1, Metadata: "meta-p0"})
	os.Add(kadm.Offset{Topic: topic, Partition: 1, At: 3, LeaderEpoch: -1, Metadata: ""})
	cr, err := adm.CommitOffsets(ctx, group, os)
	if err != nil {
		return Observation{"commit_error_code": errCode(err)}, nil
	}
	obs := Observation{"commit_error_code": errCode(cr.Error())}
	// Ask for the topic's partitions explicitly: Kafka then answers -1 for one never committed.
	fr, err := adm.FetchOffsetsForTopics(ctx, group, topic)
	if err != nil {
		obs["fetch_error_code"] = errCode(err)
		return obs, nil
	}
	for p := int32(0); p < 3; p++ {
		o, ok := fr.Lookup(topic, p)
		key := fmt.Sprintf("p%d", p)
		if !ok {
			obs[key] = "absent"
			continue
		}
		obs[key] = fmt.Sprintf("offset=%d metadata=%q error=%d", o.At, o.Metadata, errCode(o.Err))
	}
	return obs, nil
}

// offsetsCommitGenerationChecks: in a group with a live member, a commit carrying a stale
// generation is refused with ILLEGAL_GENERATION (22) and one from an unknown member with
// UNKNOWN_MEMBER_ID (25); the member's own commit succeeds.
func offsetsCommitGenerationChecks(ctx context.Context, env Env) (Observation, error) {
	g, err := newRawGroup(ctx, env, "commit-gen")
	if err != nil {
		return nil, err
	}
	defer g.close()
	jr, err := g.join(ctx, g.cl, "", nil, 30000, 10000)
	if err != nil {
		return nil, err
	}
	if _, err := g.sync(ctx, g.cl, jr, nil); err != nil {
		return nil, err
	}
	// OffsetCommit v10+ names topics by id (KIP-848): set both name and id so the request is
	// right at whichever version the client negotiates.
	md, err := kadm.NewClient(g.cl).Metadata(ctx, g.topic)
	if err != nil {
		return nil, err
	}
	topicID := md.Topics[g.topic].ID
	commit := func(gen int32, member string) (int, error) {
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group, req.Generation, req.MemberID = g.group, gen, member
		t := kmsg.NewOffsetCommitRequestTopic()
		t.Topic, t.TopicID = g.topic, topicID
		p := kmsg.NewOffsetCommitRequestTopicPartition()
		p.Partition, p.Offset, p.LeaderEpoch = 0, 1, -1
		t.Partitions = append(t.Partitions, p)
		req.Topics = append(req.Topics, t)
		resp, err := req.RequestWith(ctx, g.cl)
		if err != nil {
			return 0, err
		}
		if len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 {
			return int(resp.Topics[0].Partitions[0].ErrorCode), nil
		}
		return -1, nil
	}
	own, err := commit(jr.Generation, jr.MemberID)
	if err != nil {
		return nil, err
	}
	stale, err := commit(jr.Generation-1, jr.MemberID)
	if err != nil {
		return nil, err
	}
	unknown, err := commit(jr.Generation, "not-a-member")
	if err != nil {
		return nil, err
	}
	obs := Observation{"member_commit_error_code": own, "stale_generation_error_code": stale, "unknown_member_error_code": unknown}
	if env.Target == "real-kafka" && own != 0 {
		return obs, fmt.Errorf("probe precondition: the oracle refused the member's own commit with %d", own)
	}
	return obs, nil
}

// offsetsMultiGroupFetch: an admin fetching the committed offsets of three groups at once gets
// each group's own offsets (franz-go batches them in one OffsetFetch v8 request where the
// broker supports it and falls back to one request per group otherwise).
func offsetsMultiGroupFetch(ctx context.Context, env Env) (Observation, error) {
	cl, adm, topic, err := offsetTopic(ctx, env, "multigroup", 1, 5)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	groups := []string{topic + "-g1", topic + "-g2", topic + "-g3"}
	for i, g := range groups {
		var os kadm.Offsets
		os.Add(kadm.Offset{Topic: topic, Partition: 0, At: int64(i + 1), LeaderEpoch: -1})
		if _, err := adm.CommitOffsets(ctx, g, os); err != nil {
			return Observation{"commit_error_code": errCode(err)}, nil
		}
	}
	res := adm.FetchManyOffsets(ctx, groups...)
	obs := Observation{}
	for i, g := range groups {
		r, ok := res[g]
		if !ok {
			obs[fmt.Sprintf("group%d", i+1)] = "absent"
			continue
		}
		o, _ := r.Fetched.Lookup(topic, 0)
		obs[fmt.Sprintf("group%d", i+1)] = fmt.Sprintf("offset=%d error=%d", o.At, errCode(r.Err))
	}
	return obs, nil
}

// offsetsDeleteRecords: DeleteRecords to offset 5 moves the log start to 5; earliest is 5; a
// fetch from offset 0 answers OFFSET_OUT_OF_RANGE (1).
func offsetsDeleteRecords(ctx context.Context, env Env) (Observation, error) {
	cl, adm, topic, err := offsetTopic(ctx, env, "deleterecords", 1, 10)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var os kadm.Offsets
	os.Add(kadm.Offset{Topic: topic, Partition: 0, At: 5, LeaderEpoch: -1})
	dr, err := adm.DeleteRecords(ctx, os)
	if err != nil {
		return Observation{"delete_records_error_code": errCode(err)}, nil
	}
	d, _ := dr.Lookup(topic, 0)
	obs := Observation{"delete_records_error_code": errCode(d.Err), "low_watermark": d.LowWatermark}
	if s, err := adm.ListStartOffsets(ctx, topic); err == nil {
		o, _ := s.Lookup(topic, 0)
		obs["earliest"] = o.Offset
	}
	code, err := fetchFromOffsetCode(ctx, cl, adm, topic, 0)
	if err != nil {
		return nil, err
	}
	obs["fetch_from_0_error_code"] = code
	return obs, nil
}

// fetchFromOffsetCode sends one sessionless Fetch for partition 0 of topic at offset to its
// leader and returns the partition error code.
func fetchFromOffsetCode(ctx context.Context, cl *kgo.Client, adm *kadm.Client, topic string, offset int64) (int, error) {
	md, err := adm.Metadata(ctx, topic)
	if err != nil {
		return 0, err
	}
	t, ok := md.Topics[topic]
	if !ok || len(t.Partitions) == 0 {
		return 0, fmt.Errorf("metadata for %s lists no partition", topic)
	}
	req := kmsg.NewPtrFetchRequest()
	req.ReplicaID, req.MaxWaitMillis, req.MinBytes, req.MaxBytes, req.SessionEpoch = -1, 500, 1, 1<<20, -1
	rt := kmsg.NewFetchRequestTopic()
	rt.Topic, rt.TopicID = topic, t.ID
	rp := kmsg.NewFetchRequestTopicPartition()
	rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes, rp.CurrentLeaderEpoch, rp.LogStartOffset = 0, offset, 1<<20, -1, -1
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl.Broker(int(t.Partitions[0].Leader)))
	if err != nil {
		return 0, err
	}
	if len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 {
		return int(resp.Topics[0].Partitions[0].ErrorCode), nil
	}
	return -1, nil
}

// offsetsDeleteGroupOffsets (OffsetDelete, key 47): deleting a committed offset of an empty
// group removes it; OffsetFetch then reads -1.
func offsetsDeleteGroupOffsets(ctx context.Context, env Env) (Observation, error) {
	cl, adm, topic, err := offsetTopic(ctx, env, "offsetdelete", 1, 3)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	group := topic + "-g"
	var os kadm.Offsets
	os.Add(kadm.Offset{Topic: topic, Partition: 0, At: 2, LeaderEpoch: -1})
	if _, err := adm.CommitOffsets(ctx, group, os); err != nil {
		return Observation{"commit_error_code": errCode(err)}, nil
	}
	time.Sleep(200 * time.Millisecond)
	dr, err := adm.DeleteOffsets(ctx, group, kadm.TopicsSet{topic: {0: {}}})
	obs := Observation{"delete_error_code": errCode(err)}
	if err == nil {
		obs["delete_partition_error_code"] = errCode(dr.Error())
	}
	fr, err := adm.FetchOffsets(ctx, group)
	if err == nil {
		o, ok := fr.Lookup(topic, 0)
		obs["offset_after_delete"] = map[bool]int64{true: o.At, false: -1}[ok]
	}
	return obs, nil
}

func init() { register("offsets-retention", offsetsRetention) }

// offsetsRetention: with offsets.retention.minutes=1 on both brokers, the committed offset of
// an empty group expires (OffsetFetch reads -1 again), while the offsets of a group whose
// member is still consuming the topic are kept (KIP-211).
func offsetsRetention(ctx context.Context, env Env) (Observation, error) {
	cl, adm, topic, err := offsetTopic(ctx, env, "offsets-retention", 1, 5)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	empty, active := topic+"-empty", topic+"-active"
	var os kadm.Offsets
	os.Add(kadm.Offset{Topic: topic, Partition: 0, At: 3, LeaderEpoch: -1})
	if _, err := adm.CommitOffsets(ctx, empty, os); err != nil {
		return Observation{"commit_error_code": errCode(err)}, nil
	}
	m, err := newMember(env, active, topic, kgo.RangeBalancer(), kgo.AutoCommitInterval(2*time.Second))
	if err != nil {
		return nil, err
	}
	defer m.close()
	offsetOf := func(group string) int64 {
		r, err := adm.FetchOffsetsForTopics(ctx, group, topic)
		if err != nil {
			return -2
		}
		o, ok := r.Lookup(topic, 0)
		if !ok {
			return -2
		}
		return o.At
	}
	expired := false
	for deadline := time.Now().Add(180 * time.Second); time.Now().Before(deadline); {
		time.Sleep(5 * time.Second)
		if offsetOf(empty) == -1 {
			expired = true
			break
		}
	}
	return Observation{"empty_group_offset_expired_within_180s": expired, "active_group_offset_kept": offsetOf(active) >= 0}, nil
}
