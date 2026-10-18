// Package workload drives the scripted traffic of the operations track between the two metric
// scrapes. A workload runs against one broker, alone, and records the quantities the metric
// rows' expectations are written in (metric.Workloads lists them per workload).
package workload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/operations/metric"
)

// Sizes of the produce-consume workload: 500 records of 1 KiB (16-byte key, 1008-byte value)
// over 3 partitions.
const (
	Records    = 500
	KeyBytes   = 16
	ValueBytes = 1008
	Partitions = 3
	// group-lag workloads: 300 records (100 per partition); the group commits 30, 40 and 50, so the
	// lag is 70, 60 and 50 (180 in total); caught up, it commits 100 on every partition.
	LagRecords = 300
	// produce-rejected: 3 records of 1200 KiB, above both brokers' default 1 MiB record limit.
	Rejected      = 3
	RejectedBytes = 1200 * 1024
	// KubeMQ's group coordinator refreshes its lag gauge every 5 seconds; two refresh cycles plus
	// margin, so a busy machine does not read a stale gauge.
	LagSettle = 11 * time.Second
	// Counters are read at scrape time; a short pause lets in-flight responses land.
	Settle = 1 * time.Second
)

// Run is one finished workload on one broker.
type Run struct {
	Vars       map[string]string  // names bound into selectors: topic, group
	Quantities map[string]float64 // what the expectations are written in
	Extra      metric.Scrape      // series the track computes from the admin API ("admin:" names)
	close      func()
}

// Close releases anything the workload held open across the second scrape.
func (r *Run) Close() {
	if r != nil && r.close != nil {
		r.close()
	}
}

// Execute runs the named workload. target is "real-kafka" or "kubemq", used in names only.
func Execute(ctx context.Context, name, bootstrap, target string) (*Run, error) {
	switch name {
	case "produce-consume":
		return produceConsume(ctx, bootstrap, target)
	case "group-lag":
		return groupLag(ctx, bootstrap, target, lagCommits, false)
	case "group-lag-zero":
		return groupLag(ctx, bootstrap, target, [Partitions]int64{100, 100, 100}, false)
	case "group-lag-active":
		return groupLag(ctx, bootstrap, target, lagCommits, true)
	case "produce-rejected":
		return produceRejected(ctx, bootstrap, target)
	case "idle":
		return idle(ctx, bootstrap, target)
	}
	return nil, fmt.Errorf("workload %q is not implemented", name)
}

func names(target, workload string) (topic, group string) {
	n := time.Now().UnixNano()
	topic = fmt.Sprintf("kcert-ops-%s-%s-%d", workload, target, n)
	return topic, topic + "-group"
}

func admin(bootstrap string) (*kgo.Client, *kadm.Client, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	if err != nil {
		return nil, nil, err
	}
	return cl, kadm.NewClient(cl), nil
}

func createTopic(ctx context.Context, adm *kadm.Client, topic string, partitions int32) error {
	resp, err := adm.CreateTopic(ctx, partitions, 1, nil, topic)
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

// produce writes n records round-robin over the partitions, acks=all, idempotent, and fails on
// any refused record. It returns the key + value bytes written.
func produce(ctx context.Context, bootstrap, topic string, n int) (int, error) {
	// Uncompressed: byte metrics are compared with the payload size, and franz-go compresses
	// with snappy by default.
	cl, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	payload := 0
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%012d", i))
		val := bytes.Repeat([]byte{byte('a' + i%26)}, ValueBytes)
		payload += len(key) + len(val)
		cl.Produce(ctx, &kgo.Record{Topic: topic, Partition: int32(i % Partitions), Key: key, Value: val}, nil)
	}
	if err := cl.Flush(ctx); err != nil {
		return 0, err
	}
	// Produce with a nil promise reports nothing; confirm every record landed.
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("list end offsets: %w", err)
	}
	var total int64
	ends.Each(func(o kadm.ListedOffset) { total += o.Offset })
	if total != int64(n) {
		return 0, fmt.Errorf("produced %d records, log end offsets sum to %d", n, total)
	}
	return payload, nil
}

func produceConsume(ctx context.Context, bootstrap, target string) (*Run, error) {
	topic, group := names(target, "pc")
	cl, adm, err := admin(bootstrap)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	if err := createTopic(ctx, adm, topic, Partitions); err != nil {
		return nil, err
	}
	payload, err := produce(ctx, bootstrap, topic, Records)
	if err != nil {
		return nil, err
	}
	cons, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		return nil, err
	}
	got := 0
	for got < Records {
		fs := cons.PollFetches(ctx)
		if ctx.Err() != nil {
			cons.Close()
			return nil, fmt.Errorf("consumed %d of %d records before the deadline", got, Records)
		}
		if err := fs.Err0(); err != nil {
			cons.Close()
			return nil, fmt.Errorf("consume: %w", err)
		}
		got += fs.NumRecords()
	}
	if err := cons.CommitUncommittedOffsets(ctx); err != nil {
		cons.Close()
		return nil, fmt.Errorf("commit: %w", err)
	}
	cons.Close() // leaves the group
	sleep(ctx, Settle)
	return &Run{
		Vars:       map[string]string{"topic": topic, "group": group},
		Quantities: map[string]float64{"records": Records, "payload_bytes": float64(payload), "partitions": Partitions},
	}, nil
}

// lagCommits are the offsets the group commits per partition in the lag workloads.
var lagCommits = [Partitions]int64{30, 40, 50}

// groupLag produces LagRecords records over the partitions and commits commits for a group. With
// active=false the group has no members (an operator's offset reset, or a stopped application);
// with active=true one consumer joins, receives every record, commits commits and stays in the
// group until the second scrape (a running application that is behind).
func groupLag(ctx context.Context, bootstrap, target string, commits [Partitions]int64, active bool) (*Run, error) {
	topic, group := names(target, "lag")
	cl, adm, err := admin(bootstrap)
	if err != nil {
		return nil, err
	}
	if err := createTopic(ctx, adm, topic, Partitions); err != nil {
		cl.Close()
		return nil, err
	}
	if _, err := produce(ctx, bootstrap, topic, LagRecords); err != nil {
		cl.Close()
		return nil, err
	}
	offs := kadm.Offsets{}
	for p := int32(0); p < Partitions; p++ {
		offs.Add(kadm.Offset{Topic: topic, Partition: p, At: commits[p], LeaderEpoch: -1})
	}
	closeAll := cl.Close
	if active {
		member, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			cl.Close()
			return nil, err
		}
		got := 0
		for got < LagRecords && ctx.Err() == nil {
			fs := member.PollFetches(ctx)
			if err := fs.Err0(); err != nil {
				member.Close()
				cl.Close()
				return nil, fmt.Errorf("member consume: %w", err)
			}
			got += fs.NumRecords()
		}
		eo := map[string]map[int32]kgo.EpochOffset{topic: {}}
		for p := int32(0); p < Partitions; p++ {
			eo[topic][p] = kgo.EpochOffset{Epoch: -1, Offset: commits[p]}
		}
		var cerr error
		member.CommitOffsetsSync(ctx, eo, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				cerr = err
				return
			}
			for _, t := range resp.Topics {
				for _, pr := range t.Partitions {
					if e := kerr.ErrorForCode(pr.ErrorCode); e != nil {
						cerr = e
					}
				}
			}
		})
		if cerr != nil {
			member.Close()
			cl.Close()
			return nil, fmt.Errorf("member commit: %w", cerr)
		}
		closeAll = func() { member.Close(); cl.Close() }
	} else {
		resp, err := adm.CommitOffsets(ctx, group, offs)
		if err == nil {
			err = resp.Error()
		}
		if err != nil {
			cl.Close()
			return nil, fmt.Errorf("commit offsets for empty group: %w", err)
		}
	}
	sleep(ctx, LagSettle)
	extra, err := AdminLag(ctx, adm, group, topic)
	if err != nil {
		closeAll()
		return nil, err
	}
	var consumed float64
	q := map[string]float64{"records": LagRecords, "partitions": Partitions}
	for p := 0; p < Partitions; p++ {
		consumed += float64(commits[p])
		q[fmt.Sprintf("lag_p%d", p)] = float64(LagRecords/Partitions) - float64(commits[p])
	}
	q["consumed"], q["lag"] = consumed, LagRecords-consumed
	return &Run{Vars: map[string]string{"topic": topic, "group": group}, Quantities: q, Extra: extra, close: closeAll}, nil
}

// AdminLag computes consumer lag the way kafka-consumer-groups.sh does: log end offset minus
// committed offset per partition (a partition without a commit counts its whole log). The result
// is the "admin:consumer_group_lag" series.
func AdminLag(ctx context.Context, adm *kadm.Client, group, topic string) (metric.Scrape, error) {
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	committed, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("fetch offsets: %w", err)
	}
	var out metric.Scrape
	var lerr error
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			lerr = o.Err
			return
		}
		at := int64(0)
		if c, ok := committed.Lookup(o.Topic, o.Partition); ok && c.Err == nil && c.At >= 0 {
			at = c.At
		}
		out = append(out, metric.Series{Name: "admin:consumer_group_lag",
			Labels: map[string]string{"group": group, "topic": o.Topic, "partition": fmt.Sprint(o.Partition)},
			Value:  float64(o.Offset - at)})
	})
	return out, lerr
}

func produceRejected(ctx context.Context, bootstrap, target string) (*Run, error) {
	topic, _ := names(target, "rej")
	cl, adm, err := admin(bootstrap)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	if err := createTopic(ctx, adm, topic, 1); err != nil {
		return nil, err
	}
	// The client limit is raised so the broker, not the client, refuses the records. Uncompressed,
	// or the broker sees a batch far below its limit (brokers check the batch as sent).
	pr, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.ProducerBatchMaxBytes(4<<20), kgo.BrokerMaxWriteBytes(8<<20),
		kgo.DisableIdempotentWrite(), kgo.RecordRetries(0), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	refused := 0
	for i := 0; i < Rejected; i++ {
		rec := &kgo.Record{Topic: topic, Value: bytes.Repeat([]byte{'x'}, RejectedBytes)}
		err := pr.ProduceSync(ctx, rec).FirstErr()
		var ke *kerr.Error
		switch {
		case errors.As(err, &ke) && ke.Code == kerr.MessageTooLarge.Code:
			refused++
		case err == nil:
			return nil, fmt.Errorf("the broker accepted a %d-byte record; the workload needs it refused", RejectedBytes)
		default:
			return nil, fmt.Errorf("oversized record: want MESSAGE_TOO_LARGE (10), got %w", err)
		}
	}
	sleep(ctx, Settle)
	return &Run{Vars: map[string]string{"topic": topic}, Quantities: map[string]float64{"rejected": float64(refused)}}, nil
}

func idle(ctx context.Context, bootstrap, target string) (*Run, error) {
	topic, _ := names(target, "idle")
	cl, adm, err := admin(bootstrap)
	if err != nil {
		return nil, err
	}
	if err := createTopic(ctx, adm, topic, Partitions); err != nil {
		cl.Close()
		return nil, err
	}
	// The admin client's connection stays open across the second scrape.
	if _, err := adm.Metadata(ctx, topic); err != nil {
		cl.Close()
		return nil, err
	}
	sleep(ctx, Settle)
	return &Run{Vars: map[string]string{"topic": topic}, Quantities: map[string]float64{"partitions": Partitions}, close: cl.Close}, nil
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
