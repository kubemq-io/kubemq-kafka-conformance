package franz

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/transactions/verify"
)

func init() { Scenarios["isolation"] = isolation }

// isolation: see verify/isolation.go and SCENARIOS.md.
func isolation(ctx context.Context, env Env) (any, error) {
	acl, adm, err := admin(env)
	if err != nil {
		return nil, err
	}
	defer acl.Close()
	topic := name(env, "isolation")
	if err := createTopic(ctx, adm, topic, 1); err != nil {
		return nil, err
	}
	p, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.TransactionalID(name(env, "isolation-tid")),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.TransactionTimeout(60*time.Second))
	if err != nil {
		return nil, err
	}
	defer p.Close()
	f := verify.IsolationFacts{Outcomes: map[string]string{}}

	// txn 1, with the offsets listed while it is open
	if err := p.BeginTransaction(); err != nil {
		return nil, fmt.Errorf("begin 1: %w", err)
	}
	if err := p.ProduceSync(ctx, records(topic, verify.Keys("c", 0, verify.IsolationCommitted1))...).FirstErr(); err != nil {
		return nil, fmt.Errorf("produce txn 1: %w", err)
	}
	if f.OpenLSO, f.OpenHWM, err = listOffsets(ctx, adm, topic); err != nil {
		return nil, err
	}
	f.Outcomes["commit1"] = ErrName(p.EndTransaction(ctx, kgo.TryCommit))

	// txn 2, aborted
	if err := p.BeginTransaction(); err != nil {
		return nil, fmt.Errorf("begin 2: %w", err)
	}
	if err := p.ProduceSync(ctx, records(topic, verify.Keys("a", 0, verify.IsolationAborted))...).FirstErr(); err != nil {
		return nil, fmt.Errorf("produce txn 2: %w", err)
	}
	f.Outcomes["abort"] = ErrName(p.EndTransaction(ctx, kgo.TryAbort))

	// txn 3
	if err := p.BeginTransaction(); err != nil {
		return nil, fmt.Errorf("begin 3: %w", err)
	}
	if err := p.ProduceSync(ctx, records(topic, verify.Keys("c", verify.IsolationCommitted1, verify.IsolationCommitted2))...).FirstErr(); err != nil {
		return nil, fmt.Errorf("produce txn 3: %w", err)
	}
	f.Outcomes["commit2"] = ErrName(p.EndTransaction(ctx, kgo.TryCommit))

	// Markers are written asynchronously after EndTxn; give the broker a moment to settle so the
	// listed offsets are final on both brokers.
	time.Sleep(time.Second)
	if f.FinalLSO, f.FinalHWM, err = listOffsets(ctx, adm, topic); err != nil {
		return nil, err
	}
	if f.ReadCommitted, err = readAll(ctx, env, topic, 1, kgo.ReadCommitted(), 4*time.Second); err != nil {
		return nil, fmt.Errorf("read_committed: %w", err)
	}
	if f.ReadUncommitted, err = readAll(ctx, env, topic, 1, kgo.ReadUncommitted(), 4*time.Second); err != nil {
		return nil, fmt.Errorf("read_uncommitted: %w", err)
	}
	return f, nil
}
