package probe

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Rows of the acl posture. alice's grants (suites/security/posture, Specs["acl"]): topics
// kcert-acl-ok-* (Read, Write, Describe, Create), groups kcert-acl-g-* (Read), transactional ids
// kcert-acl-tx-* (Write, Describe), and a DENY of Write on the topic kcert-acl-ok-blocked.
// Every topic is created by admin first unless the row is about creating it.
func init() {
	register("acl-produce", aclProduce)
	register("acl-consume", aclConsume)
	register("acl-create-topic", aclCreateTopic)
	register("acl-idempotent-producer", aclIdempotent)
	register("acl-transactional-producer", aclTransactional)
	register("acl-deny-rule", aclDenyRule)
	register("acl-metadata-visibility", aclMetadataVisibility)
	register("acl-admin-apis", aclAdminAPIs)
	register("acl-write-implies-describe", aclWriteImpliesDescribe)
}

const blockedTopic = "kcert-acl-ok-blocked"

func aclAdmin(env Env) (*kgo.Client, error) {
	return saslClient(env, "plain", mechanism("PLAIN", adminUser, adminPass))
}

// aliceClient keeps franz-go's retries for retriable errors (a coordinator still loading is
// one); authorization errors are not retriable, so they surface at once.
func aliceClient(env Env, opts ...kgo.Opt) (*kgo.Client, error) {
	opts = append([]kgo.Opt{kgo.ProduceRequestTimeout(10 * time.Second), kgo.RecordDeliveryTimeout(20 * time.Second)}, opts...)
	return saslClient(env, "plain", mechanism("PLAIN", saslUser, saslPassword), opts...)
}

func aclTopic(env Env, prefix string) string {
	return fmt.Sprintf("%s%s-%d", prefix, env.Target, time.Now().UnixNano())
}

// adminTopics creates topics as admin.
func adminTopics(ctx context.Context, env Env, topics ...string) error {
	cl, err := aclAdmin(env)
	if err != nil {
		return err
	}
	defer cl.Close()
	for _, t := range topics {
		if err := createTopic(ctx, kadm.NewClient(cl), t, 1); err != nil {
			return err
		}
	}
	return nil
}

func produceOne(ctx context.Context, cl *kgo.Client, topic string) string {
	return errName(cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("x")}).FirstErr())
}

// aclProduce: alice produces (not idempotent) to an allowed and to a not-granted topic.
func aclProduce(ctx context.Context, env Env) (Observation, error) {
	ok, no := aclTopic(env, "kcert-acl-ok-"), aclTopic(env, "kcert-acl-no-")
	if err := adminTopics(ctx, env, ok, no); err != nil {
		return nil, err
	}
	cl, err := aliceClient(env, kgo.DisableIdempotentWrite())
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	return Observation{"granted_topic": produceOne(ctx, cl, ok), "not_granted_topic": produceOne(ctx, cl, no)}, nil
}

// consumeGroup consumes topic in group as alice with manual commits; it reports the first error
// of the poll and of a commit.
func consumeGroup(ctx context.Context, env Env, topic, group string) (string, string) {
	cl, err := aliceClient(env, kgo.ConsumeTopics(topic), kgo.ConsumerGroup(group), kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		return "client-" + Outcome(err), ""
	}
	defer cl.Close()
	pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		fs := cl.PollFetches(pctx)
		if pctx.Err() != nil {
			return "no records before the deadline", ""
		}
		if err := fs.Err0(); err != nil {
			return errName(err), ""
		}
		if fs.NumRecords() > 0 {
			return "NONE", errName(cl.CommitUncommittedOffsets(pctx))
		}
	}
}

// aclConsume: alice consumes a granted topic in a granted group (Read on the group is all Kafka
// asks for consuming and committing), a not-granted topic, and a granted topic in a not-granted
// group.
func aclConsume(ctx context.Context, env Env) (Observation, error) {
	ok, no := aclTopic(env, "kcert-acl-ok-"), aclTopic(env, "kcert-acl-no-")
	if err := adminTopics(ctx, env, ok, no); err != nil {
		return nil, err
	}
	admin, err := aclAdmin(env)
	if err != nil {
		return nil, err
	}
	for _, t := range []string{ok, no} {
		if err := admin.ProduceSync(ctx, &kgo.Record{Topic: t, Partition: 0, Value: []byte("x")}).FirstErr(); err != nil {
			admin.Close()
			return nil, fmt.Errorf("admin produce: %w", err)
		}
	}
	admin.Close()
	obs := Observation{}
	obs["granted_topic_granted_group_poll"], obs["granted_topic_granted_group_commit"] = consumeGroup(ctx, env, ok, aclTopic(env, "kcert-acl-g-"))
	obs["not_granted_topic_poll"], _ = consumeGroup(ctx, env, no, aclTopic(env, "kcert-acl-g-"))
	obs["not_granted_group_poll"], _ = consumeGroup(ctx, env, ok, aclTopic(env, "kcert-acl-nog-"))
	return obs, nil
}

// aclCreateTopic: alice creates a topic under her prefix and one outside it.
func aclCreateTopic(ctx context.Context, env Env) (Observation, error) {
	cl, err := aliceClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	obs := Observation{}
	for field, prefix := range map[string]string{"granted_prefix": "kcert-acl-ok-", "not_granted_prefix": "kcert-acl-no-"} {
		res, err := adm.CreateTopic(ctx, 1, 1, nil, aclTopic(env, prefix))
		if err == nil {
			err = res.Err
		}
		obs[field] = errName(err)
	}
	return obs, nil
}

// aclIdempotent: alice's idempotent producer (the default of every current client) on a topic she
// may write. Kafka 4.x allows it with Write on any topic; a broker that wants a cluster-level
// grant refuses InitProducerId.
func aclIdempotent(ctx context.Context, env Env) (Observation, error) {
	ok := aclTopic(env, "kcert-acl-ok-")
	if err := adminTopics(ctx, env, ok); err != nil {
		return nil, err
	}
	cl, err := aliceClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	return Observation{"idempotent_produce": produceOne(ctx, cl, ok)}, nil
}

// aclTransactional: alice runs a transaction with a granted and with a not-granted
// transactional id.
func aclTransactional(ctx context.Context, env Env) (Observation, error) {
	ok := aclTopic(env, "kcert-acl-ok-")
	if err := adminTopics(ctx, env, ok); err != nil {
		return nil, err
	}
	obs := Observation{}
	for field, prefix := range map[string]string{"granted_transactional_id": "kcert-acl-tx-", "not_granted_transactional_id": "kcert-acl-notx-"} {
		cl, err := aliceClient(env, kgo.TransactionalID(aclTopic(env, prefix)), kgo.TransactionTimeout(30*time.Second))
		if err != nil {
			return nil, err
		}
		res := "NONE"
		if err := cl.BeginTransaction(); err != nil {
			res = "begin: " + errName(err)
		} else if r := produceOne(ctx, cl, ok); r != "NONE" {
			res = "produce: " + r
			_ = cl.EndTransaction(ctx, kgo.TryAbort)
		} else if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
			res = "commit: " + errName(err)
		}
		cl.Close()
		obs[field] = res
	}
	return obs, nil
}

// aclDenyRule: the posture DENYs Write on kcert-acl-ok-blocked although alice's prefix allows
// it. Both brokers get the DENY through CreateAcls after boot (posture.ApplyKafkaACLs,
// posture.ApplyKubeMQACLs).
func aclDenyRule(ctx context.Context, env Env) (Observation, error) {
	// The topic name is fixed by the deny rule; a second leg on the same broker reuses it.
	cl, err := aclAdmin(env)
	if err != nil {
		return nil, err
	}
	res, err := kadm.NewClient(cl).CreateTopic(ctx, 1, 1, nil, blockedTopic)
	cl.Close()
	if err == nil && res.Err != nil && errName(res.Err) != "TOPIC_ALREADY_EXISTS" {
		err = res.Err
	}
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", blockedTopic, err)
	}
	alice, err := aliceClient(env, kgo.DisableIdempotentWrite())
	if err != nil {
		return nil, err
	}
	defer alice.Close()
	return Observation{"denied_topic_inside_granted_prefix": produceOne(ctx, alice, blockedTopic)}, nil
}

// aclMetadataVisibility: alice lists all topics; Kafka shows only topics she may Describe.
func aclMetadataVisibility(ctx context.Context, env Env) (Observation, error) {
	ok, no := aclTopic(env, "kcert-acl-ok-"), aclTopic(env, "kcert-acl-no-")
	if err := adminTopics(ctx, env, ok, no); err != nil {
		return nil, err
	}
	cl, err := aliceClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	md, err := kadm.NewClient(cl).Metadata(ctx)
	if err != nil {
		return Observation{"metadata": errName(err)}, nil
	}
	_, seesOK := md.Topics[ok]
	_, seesNo := md.Topics[no]
	return Observation{"lists_granted_topic": seesOK, "lists_not_granted_topic": seesNo}, nil
}

// aclAdminAPIs: as admin, DescribeAcls (any), CreateAcls (allow alice Read on a new prefix),
// DeleteAcls (that rule).
func aclAdminAPIs(ctx context.Context, env Env) (Observation, error) {
	cl, err := aclAdmin(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	obs := Observation{}
	desc, err := adm.DescribeACLs(ctx, kadm.NewACLs().AnyResource().Allow().Deny().AllowHosts().DenyHosts().
		ResourcePatternType(kadm.ACLPatternAny).Operations(kadm.OpAny))
	if err != nil {
		obs["describe"] = errName(err)
	} else {
		n, derr := 0, error(nil)
		for _, d := range desc {
			if d.Err != nil {
				derr = d.Err
			}
			n += len(d.Described)
		}
		obs["describe"] = errName(derr)
		obs["describe_lists_alices_rules"] = n > 0
	}
	rule := kadm.NewACLs().Allow("User:alice").AllowHosts("*").Topics("kcert-acl-new-").
		ResourcePatternType(kadm.ACLPatternPrefixed).Operations(kadm.OpRead)
	if res, err := adm.CreateACLs(ctx, rule); err != nil {
		obs["create"] = errName(err)
	} else {
		obs["create"] = firstACLErr(len(res), func(i int) error { return res[i].Err })
	}
	if res, err := adm.DeleteACLs(ctx, rule); err != nil {
		obs["delete"] = errName(err)
	} else {
		obs["delete"] = firstACLErr(len(res), func(i int) error { return res[i].Err })
	}
	return obs, nil
}

// firstACLErr is the first per-rule error's name, or "NONE" when every rule succeeded.
func firstACLErr(n int, at func(int) error) string {
	if n == 0 {
		return "no result"
	}
	for i := 0; i < n; i++ {
		if name := errName(at(i)); name != "NONE" {
			return name
		}
	}
	return "NONE"
}

// aclWriteImpliesDescribe: alice has only Write on topics kcert-acl-w-*. In Kafka, Write implies
// Describe, so Metadata for such a topic answers it (a producer needs that to find the leader);
// then she produces to it.
func aclWriteImpliesDescribe(ctx context.Context, env Env) (Observation, error) {
	topic := aclTopic(env, "kcert-acl-w-")
	if err := adminTopics(ctx, env, topic); err != nil {
		return nil, err
	}
	cl, err := aliceClient(env, kgo.DisableIdempotentWrite())
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	obs := Observation{}
	md, err := kadm.NewClient(cl).Metadata(ctx, topic)
	switch {
	case err != nil:
		obs["metadata"] = errName(err)
	case md.Topics[topic].Topic == "":
		obs["metadata"] = "topic missing from the answer"
	default:
		obs["metadata"] = errName(md.Topics[topic].Err)
	}
	obs["produce"] = produceOne(ctx, cl, topic)
	return obs, nil
}
