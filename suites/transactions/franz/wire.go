package franz

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/transactions/verify"
)

func init() {
	Scenarios["idempotent-sequences"] = idempotentSequences
	Scenarios["epoch-errors"] = epochErrors
}

var crc32c = crc32.MakeTable(crc32.Castagnoli)

// wireClient is a client whose Produce requests carry topic names: Produce v13+ (Kafka 4.x)
// addresses topics by id only, which hand-built requests here do not fill in. v12 is the last
// version with names and the same batch semantics.
func wireClient(env Env) (*kgo.Client, error) {
	v := kversion.Stable()
	v.SetMaxKeyVersion(0, 12)
	return kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.MaxVersions(v))
}

// batch encodes one uncompressed v2 record batch for partition 0: the producer id, epoch and
// first sequence are the caller's, so the broker's sequence and epoch checks can be driven
// exactly. Each record's value is verify.ValueFor(key).
func batch(pid int64, epoch int16, seq int32, transactional bool, keys []string) []byte {
	now := time.Now().UnixMilli()
	var recs []byte
	for i, k := range keys {
		var body []byte
		body = append(body, 0)                          // attributes
		body = binary.AppendVarint(body, 0)             // timestamp delta
		body = binary.AppendVarint(body, int64(i))      // offset delta
		body = binary.AppendVarint(body, int64(len(k))) // key
		body = append(body, k...)
		v := verify.ValueFor(k)
		body = binary.AppendVarint(body, int64(len(v)))
		body = append(body, v...)
		body = binary.AppendVarint(body, 0) // no headers
		recs = binary.AppendVarint(recs, int64(len(body)))
		recs = append(recs, body...)
	}
	var attrs int16
	if transactional {
		attrs |= 0x10
	}
	// From attributes to the end: the part the CRC covers.
	var tail []byte
	tail = binary.BigEndian.AppendUint16(tail, uint16(attrs))
	tail = binary.BigEndian.AppendUint32(tail, uint32(len(keys)-1)) // last offset delta
	tail = binary.BigEndian.AppendUint64(tail, uint64(now))         // first timestamp
	tail = binary.BigEndian.AppendUint64(tail, uint64(now))         // max timestamp
	tail = binary.BigEndian.AppendUint64(tail, uint64(pid))
	tail = binary.BigEndian.AppendUint16(tail, uint16(epoch))
	tail = binary.BigEndian.AppendUint32(tail, uint32(seq))
	tail = binary.BigEndian.AppendUint32(tail, uint32(len(keys)))
	tail = append(tail, recs...)
	var b []byte
	b = binary.BigEndian.AppendUint64(b, 0)                       // base offset
	b = binary.BigEndian.AppendUint32(b, uint32(4+1+4+len(tail))) // length after this field
	b = binary.BigEndian.AppendUint32(b, 0xffffffff)              // partition leader epoch -1
	b = append(b, 2)                                              // magic
	b = binary.BigEndian.AppendUint32(b, crc32.Checksum(tail, crc32c))
	return append(b, tail...)
}

// produceRaw sends one batch to partition 0 of topic with acks=all and returns "<ERROR>@<base
// offset>" for success ("NONE@3") or the error name.
func produceRaw(ctx context.Context, cl *kgo.Client, topic string, tid *string, b []byte) string {
	req := kmsg.NewPtrProduceRequest()
	req.Acks, req.TimeoutMillis, req.TransactionID = -1, 10000, tid
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition, rp.Records = 0, b
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return ErrName(err)
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return ErrName(err)
			}
			return fmt.Sprintf("NONE@%d", p.BaseOffset)
		}
	}
	return "NO_PARTITION_IN_RESPONSE"
}

// initPID sends InitProducerId (retrying while the coordinator loads) and returns the id, the
// epoch and the error name.
func initPID(ctx context.Context, cl *kgo.Client, tid *string, pid int64, epoch int16) (int64, int16, string) {
	for i := 0; ; i++ {
		req := kmsg.NewPtrInitProducerIDRequest()
		req.TransactionalID, req.TransactionTimeoutMillis, req.ProducerID, req.ProducerEpoch = tid, 60000, pid, epoch
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			return 0, 0, ErrName(err)
		}
		code := resp.ErrorCode
		retry := code == kerr.CoordinatorNotAvailable.Code || code == kerr.CoordinatorLoadInProgress.Code ||
			code == kerr.NotCoordinator.Code || code == kerr.ConcurrentTransactions.Code
		if !retry || i >= 40 {
			return resp.ProducerID, resp.ProducerEpoch, ErrName(kerr.ErrorForCode(code))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// idempotentSequences: see verify/idempotent.go and SCENARIOS.md.
func idempotentSequences(ctx context.Context, env Env) (any, error) {
	cl, err := wireClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	topic := name(env, "idem")
	if err := createTopic(ctx, adm, topic, 1); err != nil {
		return nil, err
	}
	f := verify.IdempotentFacts{Outcomes: map[string]string{}}
	pid, epoch, init := initPID(ctx, cl, nil, -1, -1)
	f.Outcomes["01_init"] = init
	if init != "NONE" {
		return f, nil
	}
	k := func(from, n int) []string { return verify.Keys("k", from, n) }
	first := batch(pid, epoch, 0, false, k(0, 3))
	f.Outcomes["02_seq0"] = produceRaw(ctx, cl, topic, nil, first)
	f.Outcomes["03_seq0_resent"] = produceRaw(ctx, cl, topic, nil, first)
	f.Outcomes["04_seq5_gap"] = produceRaw(ctx, cl, topic, nil, batch(pid, epoch, 5, false, []string{"x-00"}))
	f.Outcomes["05_seq3"] = produceRaw(ctx, cl, topic, nil, batch(pid, epoch, 3, false, k(3, 2)))
	f.Outcomes["06_next_epoch_seq0"] = produceRaw(ctx, cl, topic, nil, batch(pid, epoch+1, 0, false, k(5, 1)))
	f.Outcomes["07_old_epoch_seq5"] = produceRaw(ctx, cl, topic, nil, batch(pid, epoch, 5, false, []string{"x-01"}))
	f.Outcomes["08_next_epoch_seq1"] = produceRaw(ctx, cl, topic, nil, batch(pid, epoch+1, 1, false, k(6, 1)))
	if f.Read, err = readAll(ctx, env, topic, 1, kgo.ReadUncommitted(), 4*time.Second); err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	return f, nil
}

// epochErrors: see verify/idempotent.go and SCENARIOS.md.
func epochErrors(ctx context.Context, env Env) (any, error) {
	cl, err := wireClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	topic := name(env, "epoch")
	if err := createTopic(ctx, adm, topic, 1); err != nil {
		return nil, err
	}
	tid := name(env, "epoch-tid")
	f := verify.EpochFacts{Outcomes: map[string]string{}}
	pid, e1, init1 := initPID(ctx, cl, &tid, -1, -1)
	f.Outcomes["01_init"] = init1
	if init1 != "NONE" {
		return f, nil
	}
	pid2, e2, init2 := initPID(ctx, cl, &tid, -1, -1)
	f.Outcomes["02_second_init"] = init2
	f.SameProducerID = pid2 == pid
	f.EpochDelta = int(e2) - int(e1)

	add := func(epoch int16) string {
		req := kmsg.NewPtrAddPartitionsToTxnRequest()
		req.TransactionalID, req.ProducerID, req.ProducerEpoch = tid, pid, epoch
		rt := kmsg.NewAddPartitionsToTxnRequestTopic()
		rt.Topic, rt.Partitions = topic, []int32{0}
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			return ErrName(err)
		}
		if resp.ErrorCode != 0 {
			return ErrName(kerr.ErrorForCode(resp.ErrorCode))
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				return ErrName(kerr.ErrorForCode(p.ErrorCode))
			}
		}
		return "NO_PARTITION_IN_RESPONSE"
	}
	end := func(epoch int16, commit bool) string {
		req := kmsg.NewPtrEndTxnRequest()
		req.TransactionalID, req.ProducerID, req.ProducerEpoch, req.Commit = tid, pid, epoch, commit
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			return ErrName(err)
		}
		return ErrName(kerr.ErrorForCode(resp.ErrorCode))
	}
	f.Outcomes["03_add_partitions_old_epoch"] = add(e1)
	f.Outcomes["04_add_partitions_current_epoch"] = add(e2)
	f.Outcomes["05_produce_old_epoch"] = produceRaw(ctx, cl, topic, &tid, batch(pid, e1, 0, true, []string{"x-00"}))
	f.Outcomes["06_produce_current_epoch"] = produceRaw(ctx, cl, topic, &tid, batch(pid, e2, 0, true, verify.Keys("e", 0, 2)))
	f.Outcomes["07_end_txn_old_epoch"] = end(e1, true)
	f.Outcomes["08_end_txn_current_epoch"] = end(e2, true)
	_, _, f.Outcomes["09_init_with_old_epoch"] = initPID(ctx, cl, &tid, pid, e1)
	time.Sleep(time.Second)
	if f.ReadCommitted, err = readAll(ctx, env, topic, 1, kgo.ReadCommitted(), 4*time.Second); err != nil {
		return nil, fmt.Errorf("read_committed: %w", err)
	}
	return f, nil
}
