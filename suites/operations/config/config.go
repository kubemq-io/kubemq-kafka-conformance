// Package config holds the operations track's configuration-mapping probes (docs/operations.md
// section 3): what an operator who knows Kafka's settings sees when the same admin action meets
// KubeMQ. Each probe runs against one broker and returns deterministic facts (values, error
// codes, counts); the track runs it on the Kafka oracle, then on KubeMQ, and compares field by
// field, exactly like the features track.
package config

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Observation is what a probe saw.
type Observation map[string]any

// Probe runs one scenario against one broker. An error means the probe could not complete.
type Probe func(ctx context.Context, bootstrap, target string) (Observation, error)

// Expected is each probe's oracle precondition: what Apache Kafka itself must show for the row to
// be a valid reference. A probe whose Kafka leg does not show it is VOID (a broken probe or an
// unexpected Kafka), never a PASS on two equally wrong brokers. topic-defaults compares values
// and needs only that Kafka reported every key.
var Expected = map[string]func(Observation) bool{
	"topic-defaults": func(o Observation) bool {
		for _, k := range DefaultKeys {
			if o[k] == "absent" {
				return false
			}
		}
		return true
	},
	"segment-bytes":       func(o Observation) bool { v, ok := o["segment.bytes"].(string); return ok && v != "absent" },
	"create-defaults":     func(o Observation) bool { return o["create_error_code"] == 0 && o["partitions"] == 1 },
	"unknown-topic-key":   func(o Observation) bool { return o["create_error_code"] == 40 },
	"min-insync-zero":     func(o Observation) bool { return o["create_error_code"] == 40 },
	"auto-create":         func(o Observation) bool { return o["produce_error_code"] == 0 && o["partitions"] == 1 },
	"txn-timeout-too-big": func(o Observation) bool { return o["init_error_code"] == 50 },
}

// Registry maps the probe names used in matrix/operations.yaml to probes.
var Registry = map[string]Probe{
	"topic-defaults":      topicDefaults,
	"segment-bytes":       segmentBytes,
	"create-defaults":     createDefaults,
	"unknown-topic-key":   unknownTopicKey,
	"min-insync-zero":     minInsyncZero,
	"auto-create":         autoCreate,
	"txn-timeout-too-big": txnTimeoutTooBig,
}

// DefaultKeys are the topic configs whose defaults an operator compares first. segment.bytes is
// its own row (operations.config.segment-bytes): KubeMQ reports its real engine segment size.
var DefaultKeys = []string{
	"cleanup.policy", "compression.type", "max.message.bytes", "message.timestamp.type",
	"min.insync.replicas", "retention.bytes", "retention.ms",
}

func name(target, probe string) string {
	return fmt.Sprintf("kcert-cfg-%s-%s-%d", probe, target, time.Now().UnixNano())
}

func admin(bootstrap string) (*kgo.Client, *kadm.Client, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	if err != nil {
		return nil, nil, err
	}
	return cl, kadm.NewClient(cl), nil
}

// code is the Kafka error code of err: 0 for nil, -1 for a non-Kafka error.
func code(err error) int {
	if err == nil {
		return 0
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return int(ke.Code)
	}
	return -1
}

func create(ctx context.Context, adm *kadm.Client, topic string, partitions int32, rf int16, configs map[string]*string) (int, error) {
	resp, err := adm.CreateTopic(ctx, partitions, rf, configs, topic)
	if err == nil {
		err = resp.Err
	}
	// A refusal is the observation, not a probe failure: only a non-Kafka error fails the probe.
	if c := code(err); c >= 0 {
		return c, nil
	}
	return 0, err
}

// waitTopic waits until topic is listed with every partition led; returns its partition count
// and replica count of partition 0, or -1, -1 when it never appears within limit.
func waitTopic(ctx context.Context, adm *kadm.Client, topic string, limit time.Duration) (int, int) {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		md, err := adm.Metadata(ctx, topic)
		if err == nil {
			if t, ok := md.Topics[topic]; ok && t.Err == nil && len(t.Partitions) > 0 {
				ready := true
				for _, p := range t.Partitions {
					if p.Leader < 0 {
						ready = false
					}
				}
				if ready {
					return len(t.Partitions), len(t.Partitions[0].Replicas)
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return -1, -1
}

func describe(ctx context.Context, adm *kadm.Client, topic string) (map[string]string, error) {
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, err
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		return nil, err
	}
	if rc.Err != nil {
		return nil, rc.Err
	}
	out := map[string]string{}
	for _, c := range rc.Configs {
		v := "null"
		if c.Value != nil {
			v = *c.Value
		}
		out[c.Key] = v
	}
	return out, nil
}

// topicDefaults: a topic created with no configs; the default value DescribeConfigs reports for
// each key an operator checks first ("absent" when the broker does not report the key).
func topicDefaults(ctx context.Context, bootstrap, target string) (Observation, error) {
	cl, adm, err := admin(bootstrap)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := name(target, "defaults")
	if c, err := create(ctx, adm, topic, 1, -1, nil); err != nil || c != 0 {
		return nil, fmt.Errorf("create %s: code %d %v", topic, c, err)
	}
	waitTopic(ctx, adm, topic, 30*time.Second)
	cfg, err := describe(ctx, adm, topic)
	if err != nil {
		return nil, fmt.Errorf("describe %s: %w", topic, err)
	}
	obs := Observation{}
	for _, k := range DefaultKeys {
		v, ok := cfg[k]
		if !ok {
			v = "absent"
		}
		obs[k] = v
	}
	return obs, nil
}

// segmentBytes: the segment.bytes a fresh topic reports ("absent" when not reported).
func segmentBytes(ctx context.Context, bootstrap, target string) (Observation, error) {
	cl, adm, err := admin(bootstrap)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := name(target, "segment")
	if c, err := create(ctx, adm, topic, 1, -1, nil); err != nil || c != 0 {
		return nil, fmt.Errorf("create %s: code %d %v", topic, c, err)
	}
	waitTopic(ctx, adm, topic, 30*time.Second)
	cfg, err := describe(ctx, adm, topic)
	if err != nil {
		return nil, fmt.Errorf("describe %s: %w", topic, err)
	}
	v, ok := cfg["segment.bytes"]
	if !ok {
		v = "absent"
	}
	return Observation{"segment.bytes": v}, nil
}

// createDefaults: CreateTopics with partitions -1 and replication factor -1 (use the broker's
// defaults, CreateTopics v4+); what is created on a single node.
func createDefaults(ctx context.Context, bootstrap, target string) (Observation, error) {
	cl, adm, err := admin(bootstrap)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := name(target, "brokerdefaults")
	c, err := create(ctx, adm, topic, -1, -1, nil)
	if err != nil {
		return nil, err
	}
	obs := Observation{"create_error_code": c}
	if c == 0 {
		p, r := waitTopic(ctx, adm, topic, 30*time.Second)
		obs["partitions"], obs["replicas"] = p, r
	}
	return obs, nil
}

// unknownTopicKey: CreateTopics with a config key Kafka does not define.
func unknownTopicKey(ctx context.Context, bootstrap, target string) (Observation, error) {
	cl, adm, err := admin(bootstrap)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := name(target, "unknownkey")
	v := "1"
	c, err := create(ctx, adm, topic, 1, -1, map[string]*string{"kcert.unknown.key": &v})
	if err != nil {
		return nil, err
	}
	obs := Observation{"create_error_code": c}
	if c == 0 {
		waitTopic(ctx, adm, topic, 30*time.Second)
		cfg, err := describe(ctx, adm, topic)
		if err != nil {
			return nil, err
		}
		_, echoed := cfg["kcert.unknown.key"]
		obs["key_echoed_by_describe"] = echoed
	}
	return obs, nil
}

// minInsyncZero: CreateTopics with min.insync.replicas=0, which Kafka's validator refuses.
func minInsyncZero(ctx context.Context, bootstrap, target string) (Observation, error) {
	cl, adm, err := admin(bootstrap)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	v := "0"
	c, err := create(ctx, adm, name(target, "minisr0"), 1, -1, map[string]*string{"min.insync.replicas": &v})
	if err != nil {
		return nil, err
	}
	return Observation{"create_error_code": c}, nil
}

// autoCreate: a producer that allows topic auto-creation (as the Java producer does by default)
// writes one record to a topic that does not exist.
func autoCreate(ctx context.Context, bootstrap, target string) (Observation, error) {
	topic := name(target, "autocreate")
	pr, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.AllowAutoTopicCreation(), kgo.RecordRetries(20))
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	perr := pr.ProduceSync(pctx, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr()
	if code(perr) < 0 {
		return nil, fmt.Errorf("produce: %w", perr)
	}
	obs := Observation{"produce_error_code": code(perr)}
	cl, adm, err := admin(bootstrap)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	p, r := waitTopic(ctx, adm, topic, 10*time.Second)
	obs["partitions"], obs["replicas"] = p, r
	return obs, nil
}

// txnTimeoutTooBig: a transactional producer asks for a transaction timeout one millisecond above
// Kafka's default transaction.max.timeout.ms (900000).
func txnTimeoutTooBig(ctx context.Context, bootstrap, target string) (Observation, error) {
	topic := name(target, "txntimeout")
	cl, adm, err := admin(bootstrap)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	if c, err := create(ctx, adm, topic, 1, -1, nil); err != nil || c != 0 {
		return nil, fmt.Errorf("create %s: code %d %v", topic, c, err)
	}
	waitTopic(ctx, adm, topic, 30*time.Second)
	pr, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.TransactionalID(topic+"-txn"),
		kgo.TransactionTimeout(900001*time.Millisecond), kgo.RecordRetries(3))
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var perr error
	if perr = pr.BeginTransaction(); perr == nil {
		perr = pr.ProduceSync(pctx, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr()
	}
	if code(perr) < 0 {
		return nil, fmt.Errorf("transactional produce: %w", perr)
	}
	return Observation{"init_error_code": code(perr)}, nil
}
