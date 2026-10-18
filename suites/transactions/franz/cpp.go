package franz

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/transactions/verify"
)

func init() {
	Scenarios["consume-transform-produce"] = cpp
	Scenarios["transaction-version"] = transactionVersion
}

// cpp: see verify/cpp.go and SCENARIOS.md. franz-go's GroupTransactSession resets the session
// to the group's committed offsets when a transaction aborts.
func cpp(ctx context.Context, env Env) (any, error) {
	acl, adm, err := admin(env)
	if err != nil {
		return nil, err
	}
	defer acl.Close()
	in, out, group := name(env, "cpp-in"), name(env, "cpp-out"), name(env, "cpp-group")
	for _, t := range []string{in, out} {
		if err := createTopic(ctx, adm, t, verify.CPPPartitions); err != nil {
			return nil, err
		}
	}
	var inputs []*kgo.Record
	for n := 0; n < verify.CPPInputs; n++ {
		k := fmt.Sprintf("i-%02d", n)
		inputs = append(inputs, &kgo.Record{Topic: in, Partition: int32(n % verify.CPPPartitions), Key: []byte(k), Value: []byte(verify.ValueFor(k))})
	}
	pin, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return nil, err
	}
	err = pin.ProduceSync(ctx, inputs...).FirstErr()
	pin.Close()
	if err != nil {
		return nil, fmt.Errorf("produce input: %w", err)
	}

	sess, err := kgo.NewGroupTransactSession(kgo.SeedBrokers(env.Bootstrap), kgo.TransactionalID(name(env, "cpp-tid")),
		kgo.ConsumerGroup(group), kgo.ConsumeTopics(in), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.RequireStableFetchOffsets(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.TransactionTimeout(60*time.Second),
		kgo.SessionTimeout(10*time.Second), kgo.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		return nil, err
	}
	f := verify.CPPFacts{FirstError: "NONE", GroupOffsets: map[string]int64{}}
	fail := func(what string, err error) {
		if f.FirstError == "NONE" {
			f.FirstError = what + ":" + ErrName(err)
		}
	}
	committedOut := 0
	txn := 0
	deadline := time.Now().Add(120 * time.Second)
	for committedOut < verify.CPPInputs && time.Now().Before(deadline) && ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		fs := sess.PollRecords(pctx, 5)
		cancel()
		var recs []*kgo.Record
		fs.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
		if len(recs) == 0 {
			continue
		}
		if err := sess.Begin(); err != nil {
			fail("begin", err)
			break
		}
		var outs []*kgo.Record
		for _, r := range recs {
			k := "o-" + strings.TrimPrefix(string(r.Key), "i-")
			outs = append(outs, &kgo.Record{Topic: out, Partition: r.Partition, Key: []byte(k), Value: []byte(verify.ValueFor(k))})
		}
		if err := sess.ProduceSync(ctx, outs...).FirstErr(); err != nil {
			fail("produce", err)
		}
		txn++
		abort := txn%3 == 0
		end := kgo.TryCommit
		if abort {
			end = kgo.TryAbort
		}
		committed, err := sess.End(ctx, end)
		if err != nil {
			fail("end", err)
		}
		switch {
		case committed:
			f.Commits++
			committedOut += len(outs)
		case abort:
			f.Aborts++
		}
	}
	sess.Close()

	// The group's committed offsets, as any admin tool sees them.
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic = in
	for p := int32(0); p < verify.CPPPartitions; p++ {
		rt.Partitions = append(rt.Partitions, p)
	}
	req.Topics = append(req.Topics, rt)
	if resp, err := req.RequestWith(ctx, acl); err == nil {
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				f.GroupOffsets[fmt.Sprint(p.Partition)] = p.Offset
			}
		}
	} else {
		fail("offset fetch", err)
	}
	if f.ReadCommitted, err = readAll(ctx, env, out, verify.CPPPartitions, kgo.ReadCommitted(), 4*time.Second); err != nil {
		return nil, fmt.Errorf("read_committed: %w", err)
	}
	if f.ReadUncommitted, err = readAll(ctx, env, out, verify.CPPPartitions, kgo.ReadUncommitted(), 4*time.Second); err != nil {
		return nil, fmt.Errorf("read_uncommitted: %w", err)
	}
	return f, nil
}

// transactionVersion: see verify/cpp.go and SCENARIOS.md.
func transactionVersion(ctx context.Context, env Env) (any, error) {
	acl, adm, err := admin(env)
	if err != nil {
		return nil, err
	}
	defer acl.Close()
	topic := name(env, "tv")
	if err := createTopic(ctx, adm, topic, 1); err != nil {
		return nil, err
	}
	f := verify.VersionFacts{Finalized: -1, SupportedMax: -1, Outcomes: map[string]string{}}
	av, err := kmsg.NewPtrApiVersionsRequest().RequestWith(ctx, acl)
	if err != nil {
		return nil, fmt.Errorf("ApiVersions: %w", err)
	}
	for _, ff := range av.FinalizedFeatures {
		if ff.Name == "transaction.version" {
			f.Finalized = int(ff.MaxVersionLevel)
		}
	}
	for _, sf := range av.SupportedFeatures {
		if sf.Name == "transaction.version" {
			f.SupportedMax = int(sf.MaxVersion)
		}
	}
	p, err := txnProducer(env, name(env, "tv-tid"), 60*time.Second)
	if err != nil {
		return nil, err
	}
	defer p.Close()
	epoch := func() error {
		_, e, err := p.ProducerID(ctx)
		if err == nil {
			f.Epochs = append(f.Epochs, int(e))
		}
		return err
	}
	if err := epoch(); err != nil {
		return nil, fmt.Errorf("init producer id: %w", err)
	}
	for i := 0; i < 2; i++ {
		if err := p.BeginTransaction(); err != nil {
			return nil, fmt.Errorf("begin %d: %w", i+1, err)
		}
		if err := p.ProduceSync(ctx, records(topic, verify.Keys("v", 2*i, 2))...).FirstErr(); err != nil {
			return nil, fmt.Errorf("produce %d: %w", i+1, err)
		}
		f.Outcomes[fmt.Sprintf("commit%d", i+1)] = ErrName(p.EndTransaction(ctx, kgo.TryCommit))
		if err := epoch(); err != nil {
			return nil, fmt.Errorf("producer id after commit %d: %w", i+1, err)
		}
	}
	if f.ReadCommitted, err = readAll(ctx, env, topic, 1, kgo.ReadCommitted(), 4*time.Second); err != nil {
		return nil, fmt.Errorf("read_committed: %w", err)
	}
	return f, nil
}
