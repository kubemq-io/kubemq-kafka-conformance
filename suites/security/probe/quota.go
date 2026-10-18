package probe

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Rows of the quota posture: every user has a produce and a fetch quota of QuotaRate bytes per
// second (suites/security/posture, Specs["quota"]).
func init() {
	register("quota-produce-honouring-client", quotaProduceHonouring)
	register("quota-produce-ignoring-client", quotaProduceIgnoring)
	register("quota-fetch-honouring-client", quotaFetchHonouring)
	register("quota-describe", quotaDescribe)
	register("quota-alter", quotaAlter)
}

const (
	// QuotaRate is the posture's quota in bytes per second.
	QuotaRate = 256 * 1024
	// quotaPayload is what each throughput row moves: 24 seconds' worth at the quota. Kafka
	// measures a client's rate over its whole quota window (11 one-second samples by default),
	// so a payload smaller than about ten seconds' worth is never throttled; this one is well
	// past that on both brokers.
	quotaPayload = 24 * QuotaRate
	quotaRecord  = 16 * 1024
)

// RateHeld says whether moving n bytes in elapsed stayed within four times the quota: a broker
// that enforces the quota keeps a client near it, one that does not lets it run at link speed
// (hundreds of times the quota here). Pure, for the tests.
func RateHeld(n int, elapsed time.Duration) bool {
	if elapsed <= 0 {
		return false
	}
	return float64(n)/elapsed.Seconds() <= 4*QuotaRate
}

// throttleHook records whether any broker response carried a throttle time, and the total.
type throttleHook struct {
	seen  atomic.Bool
	total atomic.Int64
}

func (h *throttleHook) OnBrokerThrottle(_ kgo.BrokerMetadata, d time.Duration, _ bool) {
	if d > 0 {
		h.seen.Store(true)
		h.total.Add(int64(d))
	}
}

func quotaLog(env Env, row string, elapsed time.Duration, h *throttleHook) {
	var th time.Duration
	if h != nil {
		th = time.Duration(h.total.Load())
	}
	fmt.Fprintf(os.Stderr, "[security] %s %s: %d bytes in %s (%.0f KiB/s), throttle total %s\n",
		env.Target, row, quotaPayload, elapsed.Round(time.Millisecond), float64(quotaPayload)/1024/elapsed.Seconds(), th)
}

// randomRecord is an incompressible record value, so every client sends the bytes it is charged
// for (franz-go compresses by default; zeros would shrink to almost nothing).
func randomRecord() []byte {
	b := make([]byte, quotaRecord)
	_, _ = rand.Read(b)
	return b
}

func quotaTopic(ctx context.Context, env Env, label string) (string, error) {
	topic := topicName(env, label)
	return topic, adminTopics(ctx, env, topic)
}

// quotaProduceHonouring: franz-go (which waits out a throttle time before its next request to
// that broker, KIP-219) produces 24 seconds' worth of data at the quota as alice.
func quotaProduceHonouring(ctx context.Context, env Env) (Observation, error) {
	topic, err := quotaTopic(ctx, env, "quota-p")
	if err != nil {
		return nil, err
	}
	h := &throttleHook{}
	cl, err := aliceClient(env, kgo.WithHooks(h), kgo.ProducerBatchMaxBytes(quotaRecord+1024), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	start := time.Now()
	for i := 0; i < quotaPayload/quotaRecord; i++ {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: randomRecord()}).FirstErr(); err != nil {
			return nil, fmt.Errorf("produce: %w", err)
		}
	}
	el := time.Since(start)
	quotaLog(env, "produce franz-go", el, h)
	return Observation{"throttle_signalled": h.seen.Load(), "rate_held": RateHeld(quotaPayload, el)}, nil
}

// quotaProduceIgnoring: a second client library, sarama's synchronous producer, produces the same
// data as alice. (The probe name predates the finding that sarama 1.61 also waits out throttle
// times; the row is a second-client check, not a test of server-side enforcement.)
func quotaProduceIgnoring(ctx context.Context, env Env) (Observation, error) {
	topic, err := quotaTopic(ctx, env, "quota-s")
	if err != nil {
		return nil, err
	}
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_0_0_0
	cfg.Net.SASL.Enable, cfg.Net.SASL.Mechanism = true, sarama.SASLTypePlaintext
	cfg.Net.SASL.User, cfg.Net.SASL.Password = saslUser, saslPassword
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Timeout = 30 * time.Second
	cfg.Net.ReadTimeout = 60 * time.Second
	p, err := sarama.NewSyncProducer(Seeds(env.Plain), cfg)
	if err != nil {
		return nil, fmt.Errorf("sarama producer: %w", err)
	}
	defer p.Close()
	start := time.Now()
	for i := 0; i < quotaPayload/quotaRecord; i++ {
		if _, _, err := p.SendMessage(&sarama.ProducerMessage{Topic: topic, Partition: 0, Value: sarama.ByteEncoder(randomRecord())}); err != nil {
			return nil, fmt.Errorf("sarama produce: %w", err)
		}
	}
	el := time.Since(start)
	quotaLog(env, "produce sarama", el, nil)
	return Observation{"rate_held": RateHeld(quotaPayload, el)}, nil
}

// quotaFetchHonouring: admin writes 24 seconds' worth of data, then alice's franz-go consumer
// reads it all.
func quotaFetchHonouring(ctx context.Context, env Env) (Observation, error) {
	topic, err := quotaTopic(ctx, env, "quota-f")
	if err != nil {
		return nil, err
	}
	admin, err := aclAdmin(env)
	if err != nil {
		return nil, err
	}
	adminStart := time.Now()
	for i := 0; i < quotaPayload/quotaRecord; i++ {
		if err := admin.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: randomRecord()}).FirstErr(); err != nil {
			admin.Close()
			return nil, fmt.Errorf("admin produce: %w", err)
		}
	}
	admin.Close()
	quotaLog(env, "fetch setup: admin produce", time.Since(adminStart), nil)
	h := &throttleHook{}
	cl, err := aliceClient(env, kgo.WithHooks(h), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxBytes(quotaRecord*2), kgo.FetchMaxPartitionBytes(quotaRecord*2))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	start, got := time.Now(), 0
	for got < quotaPayload {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil, fmt.Errorf("consumed %d of %d bytes before the deadline", got, quotaPayload)
		}
		if err := fs.Err0(); err != nil {
			return nil, fmt.Errorf("consume: %w", err)
		}
		fs.EachRecord(func(r *kgo.Record) { got += len(r.Value) })
	}
	el := time.Since(start)
	quotaLog(env, "fetch franz-go", el, h)
	return Observation{"throttle_signalled": h.seen.Load(), "rate_held": RateHeld(quotaPayload, el)}, nil
}

// quotaDescribe: as admin, DescribeClientQuotas for every user and client-id entity.
func quotaDescribe(ctx context.Context, env Env) (Observation, error) {
	cl, err := aclAdmin(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	res, err := kadm.NewClient(cl).DescribeClientQuotas(ctx, false, []kadm.DescribeClientQuotaComponent{{Type: "user", MatchType: 2}})
	if err != nil {
		return Observation{"describe": errName(err)}, nil
	}
	var entries []string
	for _, q := range res {
		var vals []string
		for _, v := range q.Values {
			vals = append(vals, fmt.Sprintf("%s=%.0f", v.Key, v.Value))
		}
		sort.Strings(vals)
		entries = append(entries, entityString(q.Entity)+" "+strings.Join(vals, ","))
	}
	sort.Strings(entries)
	return Observation{"describe": "NONE", "user_entities": entries}, nil
}

func entityString(e kadm.ClientQuotaEntity) string {
	var parts []string
	for _, c := range e {
		name := "<default>"
		if c.Name != nil {
			name = *c.Name
		}
		parts = append(parts, c.Type+"="+name)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// quotaAlter: as admin, AlterClientQuotas raises alice's produce quota.
func quotaAlter(ctx context.Context, env Env) (Observation, error) {
	cl, err := aclAdmin(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	name := saslUser
	res, err := kadm.NewClient(cl).AlterClientQuotas(ctx, []kadm.AlterClientQuotaEntry{{
		Entity: kadm.ClientQuotaEntity{{Type: "user", Name: &name}},
		Ops:    []kadm.AlterClientQuotaOp{{Key: "producer_byte_rate", Value: 2 * QuotaRate}},
	}})
	if err != nil {
		return Observation{"alter": errName(err)}, nil
	}
	out := "no result"
	for _, r := range res {
		out = errName(r.Err)
	}
	return Observation{"alter": out}, nil
}
