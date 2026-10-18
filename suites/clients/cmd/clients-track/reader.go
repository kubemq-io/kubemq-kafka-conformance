package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/clients/verify"
)

// hostDial makes the track's own clients (fresh reader, topic pre-creation) reach a broker that
// advertises the host-gateway alias: every broker address is dialed on the host loopback with
// its port kept. The drivers, in containers, use the advertised names unchanged.
func hostDial(ctx context.Context, network, host string) (net.Conn, error) {
	_, port, err := net.SplitHostPort(host)
	if err != nil {
		return nil, err
	}
	var d net.Dialer
	return d.DialContext(ctx, network, net.JoinHostPort("127.0.0.1", port))
}

func hostClient(bootstrap string, opts ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(bootstrap), kgo.Dialer(hostDial)}, opts...)...)
}

// precreate creates the topic for a client without an admin API (KCERT_ADMIN=0): the scenario's
// partition count, or the broker defaults when partitions is 0.
func precreate(ctx context.Context, bootstrap, topic string, partitions int32) error {
	cl, err := hostClient(bootstrap)
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	p, rf := partitions, int16(1)
	if p == 0 {
		p, rf = -1, -1
	}
	resp, err := adm.CreateTopic(ctx, p, rf, nil, topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	for ctx.Err() == nil {
		md, err := adm.Metadata(ctx, topic)
		if t, ok := md.Topics[topic]; err == nil && ok && t.Err == nil && len(t.Partitions) > 0 {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("topic %s never became ready: %w", topic, ctx.Err())
}

// freshRead reads every partition of topic from the start to its high watermark, as a client that
// is a party to nothing else, then fetches the group's committed offsets. A read that fails to
// connect is retried (3 attempts, 2 seconds apart): a momentary local connection failure on the
// track's host is not the broker's behaviour, while a broker that is down still fails every attempt.
func freshRead(ctx context.Context, bootstrap, topic, group string) verify.View {
	var v verify.View
	for attempt := 1; attempt <= 3; attempt++ {
		v = freshReadOnce(ctx, bootstrap, topic, group)
		if v.ReadError == "" || !strings.Contains(v.ReadError, "unable to dial") || ctx.Err() != nil {
			break
		}
		fmt.Fprintf(os.Stderr, "[clients] fresh reader attempt %d could not connect (%s); retrying\n", attempt, v.ReadError)
		time.Sleep(2 * time.Second)
	}
	return v
}

func freshReadOnce(ctx context.Context, bootstrap, topic, group string) verify.View {
	v := verify.View{HighWatermarks: map[int32]int64{}, Committed: map[int32]int64{}}
	fail := func(format string, a ...any) verify.View {
		v.ReadError = fmt.Sprintf(format, a...)
		return v
	}
	cl, err := hostClient(bootstrap)
	if err != nil {
		return fail("%v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	md, err := adm.Metadata(ctx, topic)
	if err != nil {
		return fail("metadata: %v", err)
	}
	t, ok := md.Topics[topic]
	if !ok || t.Err != nil {
		return fail("metadata: topic %s: %v", topic, t.Err)
	}
	v.Partitions = int32(len(t.Partitions))
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return fail("list end offsets: %v", err)
	}
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return fail("list start offsets: %v", err)
	}
	from := map[int32]kgo.Offset{}
	want := 0
	var bad error
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			bad = o.Err
			return
		}
		v.HighWatermarks[o.Partition] = o.Offset
		s, _ := starts.Lookup(topic, o.Partition)
		if s.Offset != 0 {
			bad = fmt.Errorf("partition %d starts at offset %d, not 0", o.Partition, s.Offset)
		}
		if o.Offset > 0 {
			from[o.Partition] = kgo.NewOffset().At(0)
			want += int(o.Offset)
		}
	})
	if bad != nil {
		return fail("offsets: %v", bad)
	}
	if want > 0 {
		rd, err := hostClient(bootstrap, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: from}),
			kgo.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			return fail("%v", err)
		}
		defer rd.Close()
		next := map[int32]int64{}
		for {
			done := true
			for p, hw := range v.HighWatermarks {
				if next[p] < hw {
					done = false
				}
			}
			if done {
				break
			}
			fs := rd.PollFetches(ctx)
			if ctx.Err() != nil {
				return fail("read %d records before the deadline, high watermarks %v", len(v.Records), v.HighWatermarks)
			}
			if err := fs.Err0(); err != nil {
				return fail("fetch: %v", err)
			}
			fs.EachRecord(func(r *kgo.Record) {
				if r.Offset >= v.HighWatermarks[r.Partition] {
					return
				}
				next[r.Partition] = r.Offset + 1
				lr := verify.LogRecord{Partition: r.Partition, Offset: r.Offset}
				if r.Key != nil {
					k := string(r.Key)
					lr.Key = &k
				}
				if r.Value != nil {
					val := string(r.Value)
					lr.Value = &val
				}
				for _, h := range r.Headers {
					lr.Headers = append(lr.Headers, [2]string{h.Key, string(h.Value)})
				}
				lr.Timestamp, lr.CreateTS = r.Timestamp.UnixMilli(), r.Attrs.TimestampType() == 0
				v.Records = append(v.Records, lr)
			})
		}
	}
	offs, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return fail("fetch committed offsets of group %s: %v", group, err)
	}
	offs.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic && o.Err == nil && o.At >= 0 {
			v.Committed[o.Partition] = o.At
		}
	})
	return v
}
