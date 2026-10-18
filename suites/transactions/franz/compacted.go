package franz

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/transactions/verify"
)

func init() { Scenarios["compacted-read"] = compactedRead }

// compactedRead: see verify/compacted.go and SCENARIOS.md.
func compactedRead(ctx context.Context, env Env) (any, error) {
	acl, adm, err := admin(env)
	if err != nil {
		return nil, err
	}
	defer acl.Close()
	topic := name(env, "compacted")
	str := func(s string) *string { return &s }
	if err := createTopicWith(ctx, adm, topic, 1, map[string]*string{
		"cleanup.policy": str("compact"), "segment.ms": str("1000"), "min.cleanable.dirty.ratio": str("0.01"),
		"delete.retention.ms": str("5000"), "min.compaction.lag.ms": str("0")}); err != nil {
		return nil, err
	}
	p, err := txnProducer(env, name(env, "compacted-tid"), 60*time.Second)
	if err != nil {
		return nil, err
	}
	defer p.Close()
	txn := func(prefix string, keys []string, end kgo.TransactionEndTry) error {
		if err := p.BeginTransaction(); err != nil {
			return fmt.Errorf("begin %s: %w", prefix, err)
		}
		var rs []*kgo.Record
		for _, k := range keys {
			rs = append(rs, &kgo.Record{Topic: topic, Partition: 0, Key: []byte(k), Value: []byte(prefix + ":" + k)})
		}
		if err := p.ProduceSync(ctx, rs...).FirstErr(); err != nil {
			return fmt.Errorf("produce %s: %w", prefix, err)
		}
		if err := p.EndTransaction(ctx, end); err != nil {
			return fmt.Errorf("end %s: %w", prefix, err)
		}
		return nil
	}
	if err := txn("1", verify.Keys("k", 0, 5), kgo.TryCommit); err != nil {
		return nil, err
	}
	if err := txn("aborted", verify.Keys("k", 0, 5), kgo.TryAbort); err != nil {
		return nil, err
	}
	if err := txn("3", verify.Keys("k", 0, 3), kgo.TryCommit); err != nil {
		return nil, err
	}
	f := verify.CompactedFacts{}
	deadline := time.Now().Add(120 * time.Second)
	for roll := 1; time.Now().Before(deadline); roll++ {
		time.Sleep(4 * time.Second)
		if err := txn(fmt.Sprint(roll), []string{"roll"}, kgo.TryCommit); err != nil {
			return nil, err
		}
		if f.ReadCommitted, err = readAll(ctx, env, topic, 1, kgo.ReadCommitted(), 3*time.Second); err != nil {
			return nil, fmt.Errorf("read_committed: %w", err)
		}
		if f.ReadUncommitted, err = readAll(ctx, env, topic, 1, kgo.ReadUncommitted(), 3*time.Second); err != nil {
			return nil, fmt.Errorf("read_uncommitted: %w", err)
		}
		if cleaned(f) {
			f.Settled = true
			break
		}
	}
	return f, nil
}

// cleaned reports whether the cleaner has visibly run: one committed record per k-* key and no
// aborted record left in the log. It only decides when to stop waiting; verify judges the reads.
func cleaned(f verify.CompactedFacts) bool {
	n := map[string]int{}
	for _, r := range f.ReadCommitted {
		if r.Key != "roll" {
			n[r.Key]++
		}
	}
	for _, c := range n {
		if c != 1 {
			return false
		}
	}
	for _, r := range f.ReadUncommitted {
		if strings.HasPrefix(r.Value, "aborted:") {
			return false
		}
	}
	return len(n) > 0
}
