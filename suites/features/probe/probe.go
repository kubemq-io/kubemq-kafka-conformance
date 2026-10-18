// Package probe holds the broker-feature probes of the features track (Track B).
//
// A probe runs one scenario against one broker and returns an Observation: a small set of
// deterministic facts (counts, error codes, codecs, flags), never timings or broker-assigned
// identifiers. The track runs every probe on the Kafka oracle first, then on KubeMQ with only
// the bootstrap address changed, and compares the two observations field by field.
package probe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Observation is what a probe saw. Values must be JSON-encodable and deterministic.
type Observation map[string]any

// Env is what a probe gets: one broker.
type Env struct {
	Bootstrap string
	Target    string // "real-kafka" or "kubemq", for topic names only
	// Container is the broker's container. A probe that needs a client which only exists in a
	// container (the Java consumer) runs it in this container's network namespace, where the
	// broker's advertised address Bootstrap reaches the broker itself.
	Container string
}

// Probe runs one scenario against one broker. An error means the probe could not complete
// (on the oracle that makes the row VOID; on KubeMQ, FAIL); a behaviour difference must be an
// Observation field, not an error.
type Probe func(ctx context.Context, env Env) (Observation, error)

// Registry maps the probe names used in matrix/features.yaml to probes.
var Registry = map[string]Probe{}

func register(name string, p Probe) { Registry[name] = p }

// Diff compares two observations. It returns the differing fields as canonical text (sorted
// keys, JSON values), which is also the exact deviation a gap registration must carry, and
// whether they are identical.
func Diff(oracle, kubemq Observation) (string, bool) {
	keys := map[string]bool{}
	for k := range oracle {
		keys[k] = true
	}
	for k := range kubemq {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	var parts []string
	for _, k := range sorted {
		o, ok1 := oracle[k]
		q, ok2 := kubemq[k]
		oj, qj := enc(o, ok1), enc(q, ok2)
		if oj != qj {
			parts = append(parts, fmt.Sprintf("%s: Kafka %s, KubeMQ %s", k, oj, qj))
		}
	}
	return strings.Join(parts, "; "), len(parts) == 0
}

func enc(v any, present bool) string {
	if !present {
		return "absent"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

// --- helpers shared by probes ------------------------------------------------------------

func topicName(env Env, probe string) string {
	return fmt.Sprintf("kcert-%s-%s-%d", probe, env.Target, time.Now().UnixNano())
}

func adminClient(env Env) (*kgo.Client, *kadm.Client, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap))
	if err != nil {
		return nil, nil, err
	}
	return cl, kadm.NewClient(cl), nil
}

// createTopic creates a topic and waits until its metadata lists every partition with a leader.
func createTopic(ctx context.Context, adm *kadm.Client, topic string, partitions int32, configs map[string]*string) error {
	resp, err := adm.CreateTopic(ctx, partitions, 1, configs, topic)
	// A transport failure (not a Kafka error code) is retried once: published ports on a
	// container engine VM can refuse a connection for a moment.
	if err != nil && errCode(err) == -1 {
		time.Sleep(time.Second)
		resp, err = adm.CreateTopic(ctx, partitions, 1, configs, topic)
	}
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

// errCode is the Kafka error code of err, 0 for nil, -1 for a non-Kafka error.
func errCode(err error) int {
	if err == nil {
		return 0
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return int(ke.Code)
	}
	return -1
}

// consumeAll reads exactly n records from the start of every partition of topic, or fails
// when ctx ends first. Records come back in the order the client delivered them.
func consumeAll(ctx context.Context, env Env, topic string, n int, opts ...kgo.Opt) ([]*kgo.Record, error) {
	base := []kgo.Opt{kgo.SeedBrokers(env.Bootstrap), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxWait(500 * time.Millisecond)}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var out []*kgo.Record
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return out, fmt.Errorf("consumed %d of %d records before the deadline", len(out), n)
		}
		if err := fs.Err0(); err != nil {
			return out, fmt.Errorf("consume: %w", err)
		}
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	// Drain briefly so a broker that delivers extra (duplicate) records is seen doing it.
	drain, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	for drain.Err() == nil {
		fs := cl.PollFetches(drain)
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out, nil
}
