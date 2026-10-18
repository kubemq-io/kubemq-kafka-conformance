package franz

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/transactions/verify"
)

func init() {
	Scenarios["fencing"] = fencing
	Scenarios["timeout-abort"] = timeoutAbort
	Scenarios["timeout-limit"] = timeoutLimit
}

func txnProducer(env Env, tid string, timeout time.Duration) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.TransactionalID(tid),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.TransactionTimeout(timeout),
		kgo.RecordDeliveryTimeout(20*time.Second))
}

// fencing: see verify/fencing.go and SCENARIOS.md.
func fencing(ctx context.Context, env Env) (any, error) {
	acl, adm, err := admin(env)
	if err != nil {
		return nil, err
	}
	defer acl.Close()
	topic, tid := name(env, "fence"), name(env, "fence-tid")
	if err := createTopic(ctx, adm, topic, 1); err != nil {
		return nil, err
	}
	f := verify.FencingFacts{Outcomes: map[string]string{}}
	a, err := txnProducer(env, tid, 60*time.Second)
	if err != nil {
		return nil, err
	}
	defer a.Close()
	if err := a.BeginTransaction(); err != nil {
		return nil, fmt.Errorf("A begin: %w", err)
	}
	if err := a.ProduceSync(ctx, records(topic, verify.Keys("z", 0, 3))...).FirstErr(); err != nil {
		return nil, fmt.Errorf("A produce: %w", err)
	}

	b, err := txnProducer(env, tid, 60*time.Second)
	if err != nil {
		return nil, err
	}
	defer b.Close()
	_, _, berr := b.ProducerID(ctx) // InitProducerId: bumps the epoch, aborts A's transaction
	f.Outcomes["b_init"] = ErrName(berr)

	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	f.Outcomes["a_send_after_fence"] = ErrName(a.ProduceSync(sctx, records(topic, verify.Keys("z", 3, 1))...).FirstErr())
	cancel()
	ectx, cancel := context.WithTimeout(ctx, 30*time.Second)
	f.Outcomes["a_commit_after_fence"] = ErrName(a.EndTransaction(ectx, kgo.TryCommit))
	cancel()

	if berr == nil {
		if err := b.BeginTransaction(); err != nil {
			return nil, fmt.Errorf("B begin: %w", err)
		}
		if err := b.ProduceSync(ctx, records(topic, verify.Keys("b", 0, 2))...).FirstErr(); err != nil {
			return nil, fmt.Errorf("B produce: %w", err)
		}
		f.Outcomes["b_commit"] = ErrName(b.EndTransaction(ctx, kgo.TryCommit))
	}
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

// timeoutAbort: see verify/fencing.go and SCENARIOS.md.
func timeoutAbort(ctx context.Context, env Env) (any, error) {
	acl, adm, err := admin(env)
	if err != nil {
		return nil, err
	}
	defer acl.Close()
	topic, tid := name(env, "timeout"), name(env, "timeout-tid")
	if err := createTopic(ctx, adm, topic, 1); err != nil {
		return nil, err
	}
	f := verify.TimeoutFacts{Outcomes: map[string]string{}}
	p, err := txnProducer(env, tid, 3*time.Second)
	if err != nil {
		return nil, err
	}
	defer p.Close()
	if err := p.BeginTransaction(); err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	if err := p.ProduceSync(ctx, records(topic, verify.Keys("t", 0, 3))...).FirstErr(); err != nil {
		return nil, fmt.Errorf("produce: %w", err)
	}
	for i := 0; i < 45; i++ {
		time.Sleep(time.Second)
		lso, hwm, err := listOffsets(ctx, adm, topic)
		if err != nil {
			return nil, err
		}
		if lso == hwm {
			f.AbortedByBroker = true
			break
		}
	}
	ectx, cancel := context.WithTimeout(ctx, 30*time.Second)
	f.Outcomes["late_commit"] = ErrName(p.EndTransaction(ectx, kgo.TryCommit))
	cancel()
	p.Close()

	q, err := txnProducer(env, tid, 60*time.Second)
	if err != nil {
		return nil, err
	}
	defer q.Close()
	if err := q.BeginTransaction(); err != nil {
		return nil, fmt.Errorf("new producer begin: %w", err)
	}
	if err := q.ProduceSync(ctx, records(topic, verify.Keys("u", 0, 1))...).FirstErr(); err != nil {
		f.Outcomes["new_producer_send"] = ErrName(err)
	}
	f.Outcomes["new_producer_commit"] = ErrName(q.EndTransaction(ctx, kgo.TryCommit))
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

// timeoutLimit: see verify/fencing.go and SCENARIOS.md. Raw InitProducerId, because the client
// would only surface the refusal on its first send.
func timeoutLimit(ctx context.Context, env Env) (any, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	f := verify.TimeoutLimitFacts{Outcomes: map[string]string{}}
	for label, ms := range map[string]int32{"at_max_900000": 900000, "above_max_900001": 900001} {
		tid := name(env, "limit-"+label)
		// Wait for the transaction coordinator: the first request on a fresh broker may answer
		// COORDINATOR_NOT_AVAILABLE while the transaction state topic is created.
		var code int16
		for i := 0; i < 20; i++ {
			req := kmsg.NewPtrInitProducerIDRequest()
			tidCopy := tid
			req.TransactionalID, req.TransactionTimeoutMillis = &tidCopy, ms
			resp, err := req.RequestWith(ctx, cl)
			if err != nil {
				return nil, fmt.Errorf("InitProducerId %s: %w", label, err)
			}
			code = resp.ErrorCode
			if code != kerr.CoordinatorNotAvailable.Code && code != kerr.CoordinatorLoadInProgress.Code && code != kerr.NotCoordinator.Code {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		f.Outcomes[label] = ErrName(kerr.ErrorForCode(code))
	}
	return f, nil
}
