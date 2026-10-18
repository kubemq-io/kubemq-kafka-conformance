// Package franz implements the transaction scenarios (SCENARIOS.md) with the franz-go client.
// It only reports facts; package verify judges them.
package franz

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/transactions/verify"
)

// Env is one broker.
type Env struct {
	Bootstrap string
	Target    string // "real-kafka" or "kubemq", for names only
}

// Scenario runs against one broker and returns its facts.
type Scenario func(ctx context.Context, env Env) (any, error)

// Scenarios maps scenario names to their franz-go implementation.
var Scenarios = map[string]Scenario{}

// Client names the franz-go version linked into this binary.
func Client() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if d.Path == "github.com/twmb/franz-go" {
				return "franz-go " + d.Version
			}
		}
	}
	return "franz-go unknown"
}

func name(env Env, kind string) string {
	return fmt.Sprintf("kcert-txn-%s-%s-%d", kind, env.Target, time.Now().UnixNano())
}

// ErrName is the Kafka error name of err: "NONE" for nil, the protocol name for a Kafka error,
// otherwise "CLIENT:<message>" so a client-side failure is still visible in the observation.
func ErrName(err error) string {
	if err == nil {
		return "NONE"
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return ke.Message
	}
	return "CLIENT:" + err.Error()
}

func admin(env Env) (*kgo.Client, *kadm.Client, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap))
	if err != nil {
		return nil, nil, err
	}
	return cl, kadm.NewClient(cl), nil
}

// createTopic creates a topic with one copy and waits until every partition has a leader.
func createTopic(ctx context.Context, adm *kadm.Client, topic string, partitions int32) error {
	return createTopicWith(ctx, adm, topic, partitions, nil)
}

// createTopicWith is createTopic with topic configs.
func createTopicWith(ctx context.Context, adm *kadm.Client, topic string, partitions int32, configs map[string]*string) error {
	resp, err := adm.CreateTopic(ctx, partitions, 1, configs, topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	for {
		md, err := adm.Metadata(ctx, topic)
		if err == nil {
			if t, ok := md.Topics[topic]; ok && t.Err == nil && int32(len(t.Partitions)) == partitions {
				ready := true
				for _, p := range t.Partitions {
					if p.Leader < 0 || p.Err != nil {
						ready = false
					}
				}
				if ready {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("topic %s never became ready: %w", topic, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// listOffsets returns partition 0's last stable offset (read_committed latest) and high
// watermark (read_uncommitted latest).
func listOffsets(ctx context.Context, adm *kadm.Client, topic string) (lso, hwm int64, err error) {
	c, err := adm.ListCommittedOffsets(ctx, topic)
	if err == nil {
		err = c.Error()
	}
	if err != nil {
		return 0, 0, fmt.Errorf("list last stable offset: %w", err)
	}
	e, err := adm.ListEndOffsets(ctx, topic)
	if err == nil {
		err = e.Error()
	}
	if err != nil {
		return 0, 0, fmt.Errorf("list high watermark: %w", err)
	}
	lo, _ := c.Lookup(topic, 0)
	hi, _ := e.Lookup(topic, 0)
	return lo.Offset, hi.Offset, nil
}

// readAll reads a topic's partitions from the start with a fresh, group-less consumer until no
// record arrived for quiet, or ctx ends. Markers are never records, so they are never returned.
func readAll(ctx context.Context, env Env, topic string, partitions int32, level kgo.IsolationLevel, quiet time.Duration) ([]verify.Rec, error) {
	ps := map[int32]kgo.Offset{}
	for p := int32(0); p < partitions; p++ {
		ps[p] = kgo.NewOffset().AtStart()
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: ps}),
		kgo.FetchIsolationLevel(level), kgo.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var out []verify.Rec
	for {
		pctx, cancel := context.WithTimeout(ctx, quiet)
		fs := cl.PollFetches(pctx)
		cancel()
		if ctx.Err() != nil {
			return out, nil
		}
		n := 0
		var ferr error
		fs.EachError(func(_ string, _ int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				ferr = err
			}
		})
		if ferr != nil {
			return out, fmt.Errorf("fetch: %w", ferr)
		}
		fs.EachRecord(func(r *kgo.Record) {
			n++
			out = append(out, verify.Rec{Partition: r.Partition, Offset: r.Offset, Key: string(r.Key), Value: string(r.Value)})
		})
		if n == 0 {
			return out, nil
		}
	}
}

func records(topic string, keys []string) []*kgo.Record {
	out := make([]*kgo.Record, len(keys))
	for i, k := range keys {
		out[i] = &kgo.Record{Topic: topic, Partition: 0, Key: []byte(k), Value: []byte(verify.ValueFor(k))}
	}
	return out
}
