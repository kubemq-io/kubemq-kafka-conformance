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

func init() { Scenarios["send-offsets"] = sendOffsets }

// sendOffsets: see verify/sendoffsets.go and SCENARIOS.md. franz-go sends a consumer group's
// offsets inside a transaction only through a GroupTransactSession (a joined group); this
// scenario uses no group membership, so it sends AddOffsetsToTxn and TxnOffsetCommit itself with
// the transactional client's own producer id and epoch, exactly as Java's
// sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(group)) does.
func sendOffsets(ctx context.Context, env Env) (any, error) {
	acl, adm, err := admin(env)
	if err != nil {
		return nil, err
	}
	defer acl.Close()
	in, out := name(env, "so-in"), name(env, "so-out")
	group := name(env, "so-group")
	tid := name(env, "so-tid")
	for _, t := range []string{in, out} {
		if err := createTopic(ctx, adm, t, 1); err != nil {
			return nil, err
		}
	}
	if err := acl.ProduceSync(ctx, records(in, verify.Keys("i", 0, verify.SendOffsetsInputs))...).FirstErr(); err != nil {
		return nil, fmt.Errorf("produce input: %w", err)
	}
	p, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.TransactionalID(tid),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.TransactionTimeout(60*time.Second))
	if err != nil {
		return nil, err
	}
	defer p.Close()
	f := verify.SendOffsetsFacts{Outcomes: map[string]string{}}

	txn := func(label string, keys []string, offset int64, commit bool, whileOpen func()) error {
		if err := p.BeginTransaction(); err != nil {
			return fmt.Errorf("begin %s: %w", label, err)
		}
		if err := p.ProduceSync(ctx, records(out, keys)...).FirstErr(); err != nil {
			return fmt.Errorf("produce %s: %w", label, err)
		}
		f.Outcomes[label+"_send_offsets"] = ErrName(txnOffsets(ctx, p, tid, group, in, offset))
		if whileOpen != nil {
			whileOpen()
		}
		end := kgo.TryAbort
		if commit {
			end = kgo.TryCommit
		}
		f.Outcomes[label+"_end"] = ErrName(p.EndTransaction(ctx, end))
		return nil
	}
	half := verify.SendOffsetsInputs / 2
	if err := txn("txn1", verify.Keys("o", 0, half), int64(half), true, nil); err != nil {
		return nil, err
	}
	f.AfterCommit1 = settledOffset(ctx, acl, group, in)
	if err := txn("txn2", verify.Keys("o", half, half), int64(verify.SendOffsetsInputs), false, func() {
		f.StableFetchWhileOpen = groupOffset(ctx, acl, group, in, true)
	}); err != nil {
		return nil, err
	}
	f.AfterAbort = settledOffset(ctx, acl, group, in)
	if err := txn("txn3", verify.Keys("o", half, half), int64(verify.SendOffsetsInputs), true, nil); err != nil {
		return nil, err
	}
	f.AfterCommit2 = settledOffset(ctx, acl, group, in)
	if f.ReadCommitted, err = readAll(ctx, env, out, 1, kgo.ReadCommitted(), 4*time.Second); err != nil {
		return nil, fmt.Errorf("read_committed: %w", err)
	}
	return f, nil
}

// txnOffsets adds group to the open transaction and commits offset for partition 0 of topic in
// it, with no group membership (generation -1, no member id).
func txnOffsets(ctx context.Context, p *kgo.Client, tid, group, topic string, offset int64) error {
	pid, epoch, err := p.ProducerID(ctx)
	if err != nil {
		return err
	}
	add := kmsg.NewPtrAddOffsetsToTxnRequest()
	add.TransactionalID, add.ProducerID, add.ProducerEpoch, add.Group = tid, pid, epoch, group
	ar, err := add.RequestWith(ctx, p)
	if err != nil {
		return err
	}
	if err := kerr.ErrorForCode(ar.ErrorCode); err != nil {
		return err
	}
	req := kmsg.NewPtrTxnOffsetCommitRequest()
	req.TransactionalID, req.Group, req.ProducerID, req.ProducerEpoch = tid, group, pid, epoch
	req.Generation, req.MemberID = -1, ""
	rt := kmsg.NewTxnOffsetCommitRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
	rp.Partition, rp.Offset, rp.LeaderEpoch = 0, offset, -1
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, p)
	if err != nil {
		return err
	}
	for _, t := range resp.Topics {
		for _, pr := range t.Partitions {
			if err := kerr.ErrorForCode(pr.ErrorCode); err != nil {
				return err
			}
		}
	}
	return nil
}

// settledOffset is the group's offset once no transaction holds pending offsets for it: the
// broker applies a transaction's offsets when it writes the commit marker, shortly after EndTxn
// returns, so the fetch requires stable offsets and is retried while the broker answers
// UNSTABLE_OFFSET_COMMIT, for up to 15 seconds (Java's admin client retries the same way).
func settledOffset(ctx context.Context, cl *kgo.Client, group, topic string) string {
	deadline := time.Now().Add(15 * time.Second)
	for {
		v := groupOffset(ctx, cl, group, topic, true)
		if v != kerr.UnstableOffsetCommit.Message || time.Now().After(deadline) {
			return v
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// groupOffset fetches the group's committed offset for partition 0 of topic: the offset, or the
// error name when the fetch fails (UNSTABLE_OFFSET_COMMIT under requireStable while a
// transaction holds pending offsets), or "none" when nothing is committed.
func groupOffset(ctx context.Context, cl *kgo.Client, group, topic string, requireStable bool) string {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group, req.RequireStable = group, requireStable
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic, rt.Partitions = topic, []int32{0}
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return ErrName(err)
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return ErrName(err)
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return ErrName(err)
			}
			if p.Offset < 0 {
				return "none"
			}
			return fmt.Sprint(p.Offset)
		}
	}
	return "absent"
}
