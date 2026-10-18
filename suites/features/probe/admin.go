package probe

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	register("admin-topics", adminTopics)
	register("admin-partitions", adminPartitions)
	register("admin-partition-limit", adminPartitionLimit)
	register("admin-configs", adminConfigs)
	register("admin-groups", adminGroups)
	register("admin-long-topic-names", adminLongTopicNames)
}

func createCode(ctx context.Context, adm *kadm.Client, topic string, partitions int32, rf int16, validate bool) int {
	var resp kadm.CreateTopicResponses
	var err error
	if validate {
		resp, err = adm.ValidateCreateTopics(ctx, partitions, rf, nil, topic)
	} else {
		resp, err = adm.CreateTopics(ctx, partitions, rf, nil, topic)
	}
	if err != nil {
		return errCode(err)
	}
	r, ok := resp[topic]
	if !ok {
		return -1
	}
	return errCode(r.Err)
}

func topicExists(ctx context.Context, adm *kadm.Client, topic string) bool {
	md, err := adm.Metadata(ctx, topic)
	if err != nil {
		return false
	}
	t, ok := md.Topics[topic]
	return ok && t.Err == nil
}

// adminTopics: create, duplicate create, validate-only create, invalid partition count,
// replication factor above the broker count, delete, delete of an unknown topic.
func adminTopics(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	t := topicName(env, "admin-topic")
	obs := Observation{"create_error_code": createCode(ctx, adm, t, 2, 1, false)}
	obs["duplicate_create_error_code"] = createCode(ctx, adm, t, 2, 1, false)
	v := topicName(env, "admin-validate")
	obs["validate_only_error_code"] = createCode(ctx, adm, v, 1, 1, true)
	obs["validate_only_created_topic"] = topicExists(ctx, adm, v)
	obs["zero_partitions_error_code"] = createCode(ctx, adm, topicName(env, "admin-zero"), 0, 1, false)
	obs["rf_above_broker_count_error_code"] = createCode(ctx, adm, topicName(env, "admin-rf"), 1, 3, false)
	dr, err := adm.DeleteTopics(ctx, t)
	if err != nil {
		obs["delete_error_code"] = errCode(err)
	} else {
		obs["delete_error_code"] = errCode(dr[t].Err)
	}
	gone := false
	for i := 0; i < 50 && !gone; i++ {
		gone = !topicExists(ctx, adm, t)
		if !gone {
			time.Sleep(200 * time.Millisecond)
		}
	}
	obs["deleted_topic_gone_within_10s"] = gone
	unknown := topicName(env, "admin-unknown")
	dr, err = adm.DeleteTopics(ctx, unknown)
	if err != nil {
		obs["delete_unknown_error_code"] = errCode(err)
	} else {
		obs["delete_unknown_error_code"] = errCode(dr[unknown].Err)
	}
	return obs, nil
}

// adminPartitions: increase from 2 to 4 (new partitions writable), set the same count, and
// try to decrease.
func adminPartitions(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	t := topicName(env, "admin-partitions")
	if err := createTopic(ctx, adm, t, 2, nil); err != nil {
		return nil, err
	}
	code := func(set int) int {
		r, err := adm.UpdatePartitions(ctx, set, t)
		if err != nil {
			return errCode(err)
		}
		return errCode(r[t].Err)
	}
	obs := Observation{"increase_to_4_error_code": code(4)}
	count := 0
	for i := 0; i < 50; i++ {
		if md, err := adm.Metadata(ctx, t); err == nil {
			count = len(md.Topics[t].Partitions)
			if count == 4 {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	obs["partitions_after_increase"] = count
	pr, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	obs["produce_to_new_partition_error_code"] = errCode(pr.ProduceSync(ctx, &kgo.Record{Topic: t, Partition: 3, Value: []byte("x")}).FirstErr())
	obs["same_count_error_code"] = code(4)
	obs["decrease_error_code"] = code(3)
	return obs, nil
}

// adminPartitionLimit: Kafka accepts a topic with 300 partitions; KubeMQ documents a limit of
// 256 per topic.
func adminPartitionLimit(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	return Observation{
		"create_256_partitions_error_code": createCode(ctx, adm, topicName(env, "admin-256"), 256, 1, false),
		"create_300_partitions_error_code": createCode(ctx, adm, topicName(env, "admin-300"), 300, 1, false),
	}, nil
}

// adminConfigs: a config set at creation is described back as a topic-level value; an
// incremental alter sets and then deletes (reverts to default) a config.
func adminConfigs(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	t := topicName(env, "admin-configs")
	if err := createTopic(ctx, adm, t, 1, map[string]*string{"retention.ms": str("3600000")}); err != nil {
		return nil, err
	}
	describe := func(key string) string {
		rc, err := adm.DescribeTopicConfigs(ctx, t)
		if err != nil {
			return fmt.Sprintf("error %d", errCode(err))
		}
		r, err := rc.On(t, nil)
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
	obs := Observation{"retention_ms_after_create": describe("retention.ms")}
	alter := func(op kadm.IncrementalOp, key, value string) int {
		ac := kadm.AlterConfig{Op: op, Name: key}
		if op == kadm.SetConfig {
			ac.Value = &value
		}
		r, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{ac}, t)
		if err != nil {
			return errCode(err)
		}
		if len(r) == 0 {
			return -1
		}
		return errCode(r[0].Err)
	}
	obs["set_cleanup_policy_error_code"] = alter(kadm.SetConfig, "cleanup.policy", "compact")
	obs["cleanup_policy_after_set"] = describe("cleanup.policy")
	obs["delete_retention_ms_error_code"] = alter(kadm.DeleteConfig, "retention.ms", "")
	obs["retention_ms_after_delete"] = describe("retention.ms")
	return obs, nil
}

// adminGroups: list, describe (state, protocol, members) and delete of consumer groups; a
// group with a live member cannot be deleted (NON_EMPTY_GROUP, 68), an empty one can.
func adminGroups(ctx context.Context, env Env) (Observation, error) {
	topic, err := groupTopic(ctx, env, "admin-groups", 2)
	if err != nil {
		return nil, err
	}
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	group := topic + "-g"
	m, err := newMember(env, group, topic, kgo.RangeBalancer())
	if err != nil {
		return nil, err
	}
	obs := Observation{"member_assigned": waitAssigned(ctx, []*member{m}, 2, 2, 30*time.Second)}
	obs["list_contains_group"] = "request error"
	if lg, err := adm.ListGroups(ctx); err == nil {
		_, obs["list_contains_group"] = lg[group]
	}
	obs["describe_state"] = "request error"
	if dg, err := adm.DescribeGroups(ctx, group); err == nil {
		g := dg[group]
		obs["describe_state"] = g.State
		obs["describe_protocol_type"] = g.ProtocolType
		obs["describe_protocol"] = g.Protocol
		obs["describe_members"] = len(g.Members)
	}
	del := func() int {
		r, err := adm.DeleteGroups(ctx, group)
		if err != nil {
			return errCode(err)
		}
		return errCode(r[group].Err)
	}
	obs["delete_non_empty_error_code"] = del()
	m.close()
	state := ""
	for i := 0; i < 50; i++ {
		if dg, err := adm.DescribeGroups(ctx, group); err == nil {
			state = dg[group].State
			if state == "Empty" {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	obs["state_after_member_left"] = state
	obs["delete_empty_error_code"] = del()
	obs["list_contains_group_after_delete"] = "request error"
	if lg, err := adm.ListGroups(ctx); err == nil {
		_, still := lg[group]
		obs["list_contains_group_after_delete"] = still
	}
	return obs, nil
}

// adminLongTopicNames: Kafka accepts topic names up to 249 characters and refuses 250 with
// INVALID_TOPIC_EXCEPTION (17). After each request the broker must still answer. This row runs
// last in the track: on KubeMQ v3.6.16 a name of 96 or more characters crashes the broker.
func adminLongTopicNames(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	obs := Observation{}
	// A fresh client after a 3 s settle: the crash happens when the request is applied, a
	// moment after the answer, and an existing connection may still look alive.
	alive := func() bool {
		time.Sleep(3 * time.Second)
		fresh, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap))
		if err != nil {
			return false
		}
		defer fresh.Close()
		c, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return fresh.Ping(c) == nil
	}
	suffix := fmt.Sprint(time.Now().UnixNano())
	for _, n := range []int{96, 249, 250} {
		name := suffix + strings.Repeat("t", n-len(suffix))
		obs[fmt.Sprintf("create_%d_chars_error_code", n)] = createCode(ctx, adm, name, 1, 1, false)
		up := alive()
		obs[fmt.Sprintf("broker_answers_after_%d_chars", n)] = up
		if !up {
			break
		}
	}
	return obs, nil
}
