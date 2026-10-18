package probe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	fprobe "github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// createTopic creates a single-copy topic and waits until every partition has a leader.
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

// produce sends n records to partition 0 of topic, synchronously, and returns what was sent.
func produce(ctx context.Context, cl *kgo.Client, topic string, n int) ([]fprobe.Sent, error) {
	var sent []fprobe.Sent
	for i := 0; i < n; i++ {
		r := &kgo.Record{Topic: topic, Partition: 0, Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte(fmt.Sprintf("v%d", i))}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			return sent, fmt.Errorf("produce record %d: %w", i, err)
		}
		sent = append(sent, fprobe.Sent{Partition: 0, Key: r.Key, Value: r.Value})
	}
	return sent, nil
}

// consume reads n records from the start of topic with a fresh client, then drains briefly so
// extra (duplicate) records are seen.
func consume(ctx context.Context, newClient func(...kgo.Opt) (*kgo.Client, error), topic string, n int) ([]fprobe.Got, error) {
	cl, err := newClient(kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var got []fprobe.Got
	add := func(r *kgo.Record) {
		g := fprobe.Got{Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value}
		for _, h := range r.Headers {
			g.Headers = append(g.Headers, [2][]byte{[]byte(h.Key), h.Value})
		}
		got = append(got, g)
	}
	for len(got) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return got, fmt.Errorf("consumed %d of %d records before the deadline", len(got), n)
		}
		if err := fs.Err0(); err != nil {
			return got, fmt.Errorf("consume: %w", err)
		}
		fs.EachRecord(add)
	}
	drain, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	for drain.Err() == nil {
		cl.PollFetches(drain).EachRecord(add)
	}
	return got, nil
}

// errName is a Kafka error's name ("NONE" for nil), or the Outcome class of a non-Kafka error.
func errName(err error) string {
	if err == nil {
		return "NONE"
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return ke.Message
	}
	return "client-" + Outcome(err)
}

func ioReadFull(r io.Reader, buf []byte) (int, error) { return io.ReadFull(r, buf) }
