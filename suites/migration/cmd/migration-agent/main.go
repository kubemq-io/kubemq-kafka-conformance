// Command migration-agent is the migration track's client. It runs inside a client container on
// the run's private network, so it reaches the source Kafka by its container name and the broker
// under test through the host-gateway alias. Each subcommand does one step and prints one JSON
// object on stdout; errors go to stderr with exit 1.
//
//	produce   --bootstrap B --topic T --partitions P --records N [--from S] [--compacted]
//	          create the topic when absent, write records S..S+N-1 round-robin, acks=all,
//	          uncompressed; key "key-<seq mod 20>", value "value-<seq>", header seq=<seq>;
//	          --config k=v,... adds topic configs at creation
//	tombstone --bootstrap B --topic T --partitions P --keys k1,k2
//	          write a null-value record for each key, on the key's partition
//	txn       --bootstrap B --topic T --partitions P --committed N --aborted M
//	          one transaction of N records committed, then one of M records aborted
//	commit    --bootstrap B --group G --topic T --offsets 0=60,1=100
//	          commit offsets for a group with no members
//	offsets   --bootstrap B --group G --topic T
//	          committed offsets of the group and the log end offsets of the topic
//	consume   --bootstrap B --group G --topic T [--idle 5s]
//	          join the group and receive from its committed offsets (earliest when none) until
//	          nothing arrives for --idle; does not commit
//	dump      --bootstrap B --topic T [--read-committed]
//	          every record of the topic, without a group
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Rec is one delivered or dumped record.
type Rec struct {
	Partition int32             `json:"p"`
	Offset    int64             `json:"o"`
	Key       *string           `json:"k"`
	Value     *string           `json:"v"`
	Headers   map[string]string `json:"h,omitempty"`
	Timestamp int64             `json:"ts"`
}

// Offsets is the answer of the offsets subcommand. Committed is -1 for a partition without a
// commit.
type Offsets struct {
	Committed map[int32]int64 `json:"committed"`
	End       map[int32]int64 `json:"end"`
}

func main() {
	if len(os.Args) < 2 {
		die("usage: migration-agent <produce|tombstone|txn|commit|offsets|consume|dump> [flags]")
	}
	sub := os.Args[1]
	fs := flag.NewFlagSet(sub, flag.ExitOnError)
	bootstrap := fs.String("bootstrap", "", "bootstrap address")
	topic := fs.String("topic", "", "topic")
	group := fs.String("group", "", "consumer group")
	partitions := fs.Int("partitions", 3, "partitions when creating the topic")
	records := fs.Int("records", 0, "records to produce")
	from := fs.Int("from", 0, "first sequence number")
	compacted := fs.Bool("compacted", false, "create the topic with cleanup.policy=compact")
	config := fs.String("config", "", "extra topic configs when creating the topic, k=v,k=v")
	keys := fs.String("keys", "", "comma-separated keys")
	committed := fs.Int("committed", 0, "records in the committed transaction")
	aborted := fs.Int("aborted", 0, "records in the aborted transaction")
	offsets := fs.String("offsets", "", "partition=offset,...")
	idle := fs.Duration("idle", 5*time.Second, "stop consuming after this long without a record")
	readCommitted := fs.Bool("read-committed", false, "dump with isolation.level=read_committed")
	timeout := fs.Duration("timeout", 2*time.Minute, "bound on the whole subcommand")
	_ = fs.Parse(os.Args[2:])
	if *bootstrap == "" {
		die("--bootstrap is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var out any
	var err error
	switch sub {
	case "produce":
		out, err = produce(ctx, *bootstrap, *topic, int32(*partitions), *from, *records, *compacted, *config)
	case "tombstone":
		out, err = tombstone(ctx, *bootstrap, *topic, int32(*partitions), strings.Split(*keys, ","))
	case "txn":
		out, err = txn(ctx, *bootstrap, *topic, int32(*partitions), *committed, *aborted)
	case "commit":
		out, err = commit(ctx, *bootstrap, *group, *topic, *offsets)
	case "offsets":
		out, err = groupOffsets(ctx, *bootstrap, *group, *topic)
	case "consume":
		out, err = consume(ctx, *bootstrap, *group, *topic, *idle)
	case "dump":
		out, err = dump(ctx, *bootstrap, *topic, *readCommitted)
	default:
		die("unknown subcommand " + sub)
	}
	if err != nil {
		die(sub + ": " + err.Error())
	}
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		die(err.Error())
	}
}

// seeds splits a comma-separated bootstrap list (a cluster's brokers).
func seeds(bootstrap string) []string {
	var out []string
	for _, s := range strings.Split(bootstrap, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, "migration-agent:", msg)
	os.Exit(1)
}

func key(seq int) string { return fmt.Sprintf("key-%02d", seq%20) }

// partitionOf places a key the same way on every run: keys hash to a fixed partition here so a
// compacted topic keeps each key on one partition (the default partitioner would too).
func partitionOf(k string, n int32) int32 {
	var h uint32
	for i := 0; i < len(k); i++ {
		h = h*31 + uint32(k[i])
	}
	return int32(h % uint32(n))
}

func ensureTopic(ctx context.Context, adm *kadm.Client, topic string, partitions int32, compacted bool, config string) error {
	cfg := map[string]*string{}
	if compacted {
		c := "compact"
		cfg["cleanup.policy"] = &c
	}
	for _, kv := range strings.Split(config, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok {
			v := v
			cfg[k] = &v
		}
	}
	resp, err := adm.CreateTopic(ctx, partitions, -1, cfg, topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil && !errorsIs(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	for ctx.Err() == nil {
		md, err := adm.Metadata(ctx, topic)
		if err == nil {
			if t, ok := md.Topics[topic]; ok && t.Err == nil && len(t.Partitions) > 0 {
				ready := true
				for _, p := range t.Partitions {
					if p.Leader < 0 {
						ready = false
					}
				}
				if ready {
					return nil
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("topic %s never became ready: %w", topic, ctx.Err())
}

func errorsIs(err error, target *kerr.Error) bool {
	var ke *kerr.Error
	return errors.As(err, &ke) && ke.Code == target.Code
}

func producer(bootstrap string, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{kgo.SeedBrokers(seeds(bootstrap)...), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerBatchCompression(kgo.NoCompression())}
	return kgo.NewClient(append(base, opts...)...)
}

func endOffsets(ctx context.Context, adm *kadm.Client, topic string) (map[int32]int64, error) {
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	out := map[int32]int64{}
	var lerr error
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			lerr = o.Err
		}
		out[o.Partition] = o.Offset
	})
	return out, lerr
}

func produce(ctx context.Context, bootstrap, topic string, partitions int32, from, n int, compacted bool, config string) (any, error) {
	cl, err := producer(bootstrap)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if err := ensureTopic(ctx, adm, topic, partitions, compacted, config); err != nil {
		return nil, err
	}
	for seq := from; seq < from+n; seq++ {
		k, v := key(seq), fmt.Sprintf("value-%06d", seq)
		p := int32(seq % int(partitions))
		if compacted {
			p = partitionOf(k, partitions)
		}
		r := &kgo.Record{Topic: topic, Partition: p, Key: []byte(k), Value: []byte(v),
			Headers: []kgo.RecordHeader{{Key: "seq", Value: []byte(strconv.Itoa(seq))}}}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			return nil, fmt.Errorf("record %d: %w", seq, err)
		}
	}
	ends, err := endOffsets(ctx, adm, topic)
	return map[string]any{"end": ends}, err
}

func tombstone(ctx context.Context, bootstrap, topic string, partitions int32, keys []string) (any, error) {
	cl, err := producer(bootstrap)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	for _, k := range keys {
		r := &kgo.Record{Topic: topic, Partition: partitionOf(k, partitions), Key: []byte(k), Value: nil}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			return nil, fmt.Errorf("tombstone %s: %w", k, err)
		}
	}
	ends, err := endOffsets(ctx, kadm.NewClient(cl), topic)
	return map[string]any{"end": ends}, err
}

func txn(ctx context.Context, bootstrap, topic string, partitions int32, committed, aborted int) (any, error) {
	adminCl, err := kgo.NewClient(kgo.SeedBrokers(seeds(bootstrap)...))
	if err != nil {
		return nil, err
	}
	defer adminCl.Close()
	if err := ensureTopic(ctx, kadm.NewClient(adminCl), topic, partitions, false, ""); err != nil {
		return nil, err
	}
	cl, err := producer(bootstrap, kgo.TransactionalID(topic+"-txn"))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	write := func(from, n int, end kgo.TransactionEndTry) error {
		if err := cl.BeginTransaction(); err != nil {
			return err
		}
		for seq := from; seq < from+n; seq++ {
			r := &kgo.Record{Topic: topic, Partition: int32(seq % int(partitions)), Key: []byte(key(seq)),
				Value: []byte(fmt.Sprintf("value-%06d", seq)), Headers: []kgo.RecordHeader{{Key: "seq", Value: []byte(strconv.Itoa(seq))}}}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				return err
			}
		}
		return cl.EndTransaction(ctx, end)
	}
	if err := write(0, committed, kgo.TryCommit); err != nil {
		return nil, fmt.Errorf("committed transaction: %w", err)
	}
	if err := write(committed, aborted, kgo.TryAbort); err != nil {
		return nil, fmt.Errorf("aborted transaction: %w", err)
	}
	ends, err := endOffsets(ctx, kadm.NewClient(adminCl), topic)
	return map[string]any{"end": ends}, err
}

func commit(ctx context.Context, bootstrap, group, topic, spec string) (any, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds(bootstrap)...))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	offs := kadm.Offsets{}
	for _, kv := range strings.Split(spec, ",") {
		p, o, ok := strings.Cut(kv, "=")
		pi, err1 := strconv.Atoi(p)
		oi, err2 := strconv.ParseInt(o, 10, 64)
		if !ok || err1 != nil || err2 != nil {
			return nil, fmt.Errorf("bad --offsets entry %q", kv)
		}
		offs.Add(kadm.Offset{Topic: topic, Partition: int32(pi), At: oi, LeaderEpoch: -1})
	}
	resp, err := kadm.NewClient(cl).CommitOffsets(ctx, group, offs)
	if err == nil {
		err = resp.Error()
	}
	if err != nil {
		return nil, err
	}
	return groupOffsets(ctx, bootstrap, group, topic)
}

func groupOffsets(ctx context.Context, bootstrap, group, topic string) (any, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds(bootstrap)...))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ends, err := endOffsets(ctx, adm, topic)
	if err != nil {
		return nil, fmt.Errorf("end offsets: %w", err)
	}
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("fetch offsets: %w", err)
	}
	out := Offsets{Committed: map[int32]int64{}, End: ends}
	for p := range ends {
		out.Committed[p] = -1
		if o, ok := fetched.Lookup(topic, p); ok && o.Err == nil && o.At >= 0 {
			out.Committed[p] = o.At
		}
	}
	return out, nil
}

func consume(ctx context.Context, bootstrap, group, topic string, idle time.Duration) (any, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds(bootstrap)...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	recs, err := poll(ctx, cl, idle)
	return map[string]any{"records": recs}, err
}

func dump(ctx context.Context, bootstrap, topic string, readCommitted bool) (any, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(seeds(bootstrap)...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxWait(500 * time.Millisecond)}
	if readCommitted {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	recs, err := poll(ctx, cl, 4*time.Second)
	return map[string]any{"records": recs}, err
}

// poll receives until nothing arrives for idle (the first record may take longer: a group join).
func poll(ctx context.Context, cl *kgo.Client, idle time.Duration) ([]Rec, error) {
	var out []Rec
	last := time.Now()
	first := true
	for {
		limit := idle
		if first {
			limit = idle + 20*time.Second
		}
		if time.Since(last) > limit {
			break
		}
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		fs := cl.PollFetches(pctx)
		cancel()
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		for _, e := range fs.Errors() {
			if e.Err != context.DeadlineExceeded && e.Err != context.Canceled {
				return out, fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}
		fs.EachRecord(func(r *kgo.Record) {
			rec := Rec{Partition: r.Partition, Offset: r.Offset, Timestamp: r.Timestamp.UnixMilli()}
			if r.Key != nil {
				k := string(r.Key)
				rec.Key = &k
			}
			if r.Value != nil {
				v := string(r.Value)
				rec.Value = &v
			}
			for _, h := range r.Headers {
				if rec.Headers == nil {
					rec.Headers = map[string]string{}
				}
				rec.Headers[h.Key] = string(h.Value)
			}
			out = append(out, rec)
			last = time.Now()
			first = false
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out, nil
}
