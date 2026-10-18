package probe

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	register("wire-topic-ids", wireTopicIDs)
	register("wire-error-codes", wireErrorCodes)
}

func metadataTopicID(ctx context.Context, cl *kgo.Client, topic string) ([16]byte, int, error) {
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return [16]byte{}, 0, err
	}
	if len(resp.Topics) != 1 {
		return [16]byte{}, -1, nil
	}
	return resp.Topics[0].TopicID, int(resp.Topics[0].ErrorCode), nil
}

// wireTopicIDs: Metadata (v10+) names every topic with a non-zero topic id that stays the same
// across calls; deleting and re-creating the topic gives a new id; DeleteTopics naming only the
// topic id (v6+) deletes it.
func wireTopicIDs(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	t := topicName(env, "topic-ids")
	if err := createTopic(ctx, adm, t, 1, nil); err != nil {
		return nil, err
	}
	id1, _, err := metadataTopicID(ctx, cl, t)
	if err != nil {
		return nil, err
	}
	id2, _, _ := metadataTopicID(ctx, cl, t)
	obs := Observation{"topic_id_present": id1 != [16]byte{}, "topic_id_stable": id1 == id2}
	if _, err := adm.DeleteTopics(ctx, t); err != nil {
		return nil, err
	}
	recreated := false
	for i := 0; i < 50 && !recreated; i++ {
		if r, err := adm.CreateTopics(ctx, 1, 1, nil, t); err == nil && r[t].Err == nil {
			recreated = true
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}
	obs["recreated"] = recreated
	if recreated {
		id3, _, _ := metadataTopicID(ctx, cl, t)
		obs["recreated_topic_has_new_id"] = id3 != id1 && id3 != [16]byte{}
		// Delete by id only.
		req := kmsg.NewPtrDeleteTopicsRequest()
		req.TimeoutMillis = 15000
		dt := kmsg.NewDeleteTopicsRequestTopic()
		dt.TopicID = id3
		req.Topics = append(req.Topics, dt)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			obs["delete_by_id"] = "request error"
		} else if len(resp.Topics) == 1 {
			obs["delete_by_id_error_code"] = int(resp.Topics[0].ErrorCode)
			gone := false
			for i := 0; i < 50 && !gone; i++ {
				_, code, _ := metadataTopicID(ctx, cl, t)
				gone = code == 3
				if !gone {
					time.Sleep(200 * time.Millisecond)
				}
			}
			obs["deleted_by_id_gone_within_10s"] = gone
		}
	}
	return obs, nil
}

// rawFrame sends one request frame on a fresh connection and reports whether the broker
// answered or closed the connection.
func rawFrame(ctx context.Context, addr string, key, version int16) string {
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return "dial error"
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	var p []byte
	p = binary.BigEndian.AppendUint16(p, uint16(key))
	p = binary.BigEndian.AppendUint16(p, uint16(version))
	p = binary.BigEndian.AppendUint32(p, 7)
	p = binary.BigEndian.AppendUint16(p, 5)
	p = append(p, "kcert"...)
	frame := append(binary.BigEndian.AppendUint32(nil, uint32(len(p))), p...)
	if _, err := conn.Write(frame); err != nil {
		return "closed"
	}
	var sz [4]byte
	if _, err := io.ReadFull(conn, sz[:]); err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return "no answer within 5s"
		}
		return "closed"
	}
	return "answered"
}

// wireErrorCodes: error-code parity for bad inputs.
func wireErrorCodes(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	t := topicName(env, "errcodes")
	if err := createTopic(ctx, adm, t, 1, nil); err != nil {
		return nil, err
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: t, Value: []byte("x")}).FirstErr(); err != nil {
		return nil, err
	}
	obs := Observation{}
	// Fetch at a negative offset and from a partition that does not exist.
	codeOrFailed := func(code int, err error) any {
		if err != nil {
			return "request error"
		}
		return code
	}
	obs["fetch_negative_offset_error_code"] = codeOrFailed(fetchFromOffsetCode(ctx, cl, adm, t, -5))
	obs["fetch_past_end_error_code"] = codeOrFailed(fetchFromOffsetCode(ctx, cl, adm, t, 1000))
	md, err := adm.Metadata(ctx, t)
	if err != nil || len(md.Topics[t].Partitions) == 0 {
		return nil, fmt.Errorf("metadata for %s lists no partition", t)
	}
	leader := md.Topics[t].Partitions[0].Leader
	{
		req := kmsg.NewPtrFetchRequest()
		req.ReplicaID, req.MaxWaitMillis, req.MinBytes, req.MaxBytes, req.SessionEpoch = -1, 200, 1, 1<<20, -1
		rt := kmsg.NewFetchRequestTopic()
		rt.Topic, rt.TopicID = t, md.Topics[t].ID
		rp := kmsg.NewFetchRequestTopicPartition()
		rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes, rp.CurrentLeaderEpoch, rp.LogStartOffset = 99, 0, 1<<20, -1, -1
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		obs["fetch_unknown_partition_error_code"] = "request error"
		if resp, err := req.RequestWith(ctx, cl.Broker(int(leader))); err == nil && len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 {
			obs["fetch_unknown_partition_error_code"] = int(resp.Topics[0].Partitions[0].ErrorCode)
		}
	}
	{
		req := kmsg.NewPtrListOffsetsRequest()
		req.ReplicaID = -1
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = t
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition, rp.Timestamp, rp.CurrentLeaderEpoch = 99, -1, -1
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		obs["listoffsets_unknown_partition_error_code"] = "request error"
		if resp, err := req.RequestWith(ctx, cl.Broker(int(leader))); err == nil && len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 {
			obs["listoffsets_unknown_partition_error_code"] = int(resp.Topics[0].Partitions[0].ErrorCode)
		}
	}
	// Invalid topic names.
	obs["create_invalid_name_error_code"] = createCode(ctx, adm, "bad name!", 1, 1, false)
	obs["create_dot_name_error_code"] = createCode(ctx, adm, "..", 1, 1, false)
	// Empty group id.
	{
		req := kmsg.NewPtrJoinGroupRequest()
		req.Group, req.SessionTimeoutMillis, req.RebalanceTimeoutMillis, req.ProtocolType = "", 10000, 10000, "consumer"
		p := kmsg.NewJoinGroupRequestProtocol()
		p.Name = "range"
		req.Protocols = append(req.Protocols, p)
		if resp, err := req.RequestWith(ctx, cl); err == nil {
			obs["join_empty_group_id_error_code"] = int(resp.ErrorCode)
		} else {
			obs["join_empty_group_id_error_code"] = errCode(err)
		}
	}
	// Unknown API key and a Produce version far above the broker's range.
	obs["unknown_api_key"] = rawFrame(ctx, env.Bootstrap, 999, 0)
	obs["produce_version_99"] = rawFrame(ctx, env.Bootstrap, 0, 99)
	return obs, nil
}
