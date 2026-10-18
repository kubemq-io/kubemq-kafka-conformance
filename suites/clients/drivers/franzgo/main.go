// Command franzgo is the franz-go client driver of the clients track (SCENARIO.md).
package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/clients/drivers/golib"
)

const client = "franz-go"

var (
	led = golib.NewLedger()
	in  golib.Inputs
)

func main() {
	in = golib.ReadInputs(led)
	led.Emit("start", map[string]any{"client": client, "version": golib.ModuleVersion("github.com/twmb/franz-go"),
		"mode": in.Mode, "group_protocol": groupProtocol()})
	if in.Mode == "scenario" {
		scenario()
	} else {
		stock()
	}
	led.Emit("done", nil)
}

// groupProtocol reads the effective setting from a consumer configured as both modes configure
// theirs: franz-go runs the KIP-848 protocol only when kgo.ServerSideBalancer is set.
func groupProtocol() string {
	cl, err := kgo.NewClient(kgo.SeedBrokers(in.Bootstrap), kgo.ConsumerGroup(in.Group), kgo.ConsumeTopics(in.Topic))
	if err != nil {
		led.Fail("start", -1, err)
	}
	defer cl.Close()
	if on, _ := cl.OptValue(kgo.ServerSideBalancer).(bool); on {
		return "consumer"
	}
	return "classic"
}

func scenario() {
	if in.Admin {
		create(in.Partitions, 1)
	}
	prod, err := kgo.NewClient(kgo.SeedBrokers(in.Bootstrap), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		led.Fail("producer", -1, err)
	}
	produce(prod, 1, 0, in.Records, true)
	consume(1, in.Records, false)
	produce(prod, 2, in.Records, in.Records+in.Records2, true)
	prod.Close()
	consume(2, in.Records2, true)
}

func stock() {
	if in.Admin {
		create(-1, -1)
	}
	assigned := make(chan struct{}, 1)
	cons, err := kgo.NewClient(kgo.SeedBrokers(in.Bootstrap), kgo.ConsumerGroup(in.Group), kgo.ConsumeTopics(in.Topic),
		kgo.OnPartitionsAssigned(onAssigned(1, assigned)))
	if err != nil {
		led.Fail("consumer", -1, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), in.Timeout)
	defer cancel()
	got := map[int]bool{}
	// Poll until assigned; nothing has been produced yet, so this delivers nothing.
	waitAssigned(ctx, cons, assigned, 1, got)
	prod, err := kgo.NewClient(kgo.SeedBrokers(in.Bootstrap))
	if err != nil {
		led.Fail("producer", -1, err)
	}
	produce(prod, 1, 0, in.Records, false)
	prod.Close()
	poll(ctx, cons, 1, got, in.Records)
	cons.Close()
}

func create(partitions int32, rf int16) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(in.Bootstrap))
	if err != nil {
		led.Fail("create", -1, err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), in.Timeout)
	defer cancel()
	resp, err := adm.CreateTopic(ctx, partitions, rf, nil, in.Topic)
	if err == nil {
		err = resp.Err
	}
	led.Emit("create", map[string]any{"topic": in.Topic, "partitions": partitions, "error_code": code(err), "error": msg(err)})
	if err != nil {
		led.Fail("create", code(err), err)
	}
	// An application waits until the topic is usable before producing.
	for {
		md, err := adm.Metadata(ctx, in.Topic)
		if t, ok := md.Topics[in.Topic]; err == nil && ok && t.Err == nil && len(t.Partitions) > 0 &&
			(partitions < 0 || int32(len(t.Partitions)) == partitions) && leadersKnown(t) {
			return
		}
		select {
		case <-ctx.Done():
			led.Fail("create", -1, fmt.Errorf("topic never became ready: %w", ctx.Err()))
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func leadersKnown(t kadm.TopicDetail) bool {
	for _, p := range t.Partitions {
		if p.Leader < 0 || p.Err != nil {
			return false
		}
	}
	return true
}

// produce sends records lo..hi-1 asynchronously, records each acknowledgement, and flushes.
func produce(cl *kgo.Client, phase, lo, hi int, keyed bool) {
	var wg sync.WaitGroup
	for seq := lo; seq < hi; seq++ {
		r := &kgo.Record{Topic: in.Topic, Value: []byte(golib.Value(seq))}
		if keyed {
			r.Key = []byte(golib.Key(seq, in.Keys))
			r.Headers = []kgo.RecordHeader{{Key: "seq", Value: []byte(strconv.Itoa(seq))}, {Key: "client", Value: []byte(client)}}
		}
		wg.Add(1)
		s := seq
		cl.Produce(context.Background(), r, func(r *kgo.Record, err error) {
			defer wg.Done()
			ev := map[string]any{"phase": phase, "seq": s, "partition": r.Partition, "offset": r.Offset, "error_code": code(err)}
			if err != nil {
				ev["error"], ev["offset"] = err.Error(), -1
			}
			led.Emit("ack", ev)
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), in.Timeout)
	defer cancel()
	if err := cl.Flush(ctx); err != nil {
		led.Fail("produce", code(err), err)
	}
	wg.Wait()
}

func onAssigned(phase int, signal chan struct{}) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
		ps := append([]int32(nil), m[in.Topic]...)
		sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
		led.Emit("assigned", map[string]any{"phase": phase, "partitions": ps})
		select {
		case signal <- struct{}{}:
		default:
		}
	}
}

// consume runs one scenario consume phase: a new group member reads want records, keeps polling briefly when drain is set, commits synchronously and leaves.
func consume(phase, want int, drain bool) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(in.Bootstrap), kgo.ConsumerGroup(in.Group), kgo.ConsumeTopics(in.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(onAssigned(phase, make(chan struct{}, 1))))
	if err != nil {
		led.Fail("consume", -1, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), in.Timeout)
	defer cancel()
	got := map[int]bool{}
	poll(ctx, cl, phase, got, want)
	if drain {
		d, dcancel := context.WithTimeout(ctx, 2*time.Second)
		for d.Err() == nil {
			fs := cl.PollFetches(d)
			fs.EachError(func(t string, p int32, err error) {
				if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
					led.Fail("consume", code(err), fmt.Errorf("drain fetch %s/%d: %w", t, p, err))
				}
			})
			emitFetches(fs, phase, got)
		}
		dcancel()
	}
	err = cl.CommitUncommittedOffsets(ctx)
	led.Emit("commit", map[string]any{"phase": phase, "error_code": code(err), "error": msg(err)})
	cl.Close()
}

func waitAssigned(ctx context.Context, cl *kgo.Client, assigned chan struct{}, phase int, got map[int]bool) {
	for {
		select {
		case <-assigned:
			return
		default:
		}
		p, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		emitFetches(cl.PollFetches(p), phase, got)
		cancel()
		if ctx.Err() != nil {
			led.Fail("consume", -1, errors.New("consumer was never assigned partitions"))
		}
	}
}

// poll reads until got holds want distinct records.
func poll(ctx context.Context, cl *kgo.Client, phase int, got map[int]bool, want int) {
	for len(got) < want {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			led.Fail("consume", -1, fmt.Errorf("received %d of %d records before the deadline", len(got), want))
		}
		fs.EachError(func(t string, p int32, err error) {
			led.Fail("consume", code(err), fmt.Errorf("fetch %s/%d: %w", t, p, err))
		})
		emitFetches(fs, phase, got)
	}
}

func emitFetches(fs kgo.Fetches, phase int, got map[int]bool) {
	fs.EachRecord(func(r *kgo.Record) {
		seq := golib.SeqOf(r.Value)
		got[seq] = true
		hs := make([][2]string, 0, len(r.Headers))
		for _, h := range r.Headers {
			hs = append(hs, [2]string{h.Key, string(h.Value)})
		}
		var key any
		if r.Key != nil {
			key = string(r.Key)
		}
		led.Emit("recv", map[string]any{"phase": phase, "seq": seq, "partition": r.Partition, "offset": r.Offset,
			"key": key, "value": string(r.Value), "headers": hs})
	})
}

func code(err error) int {
	if err == nil {
		return 0
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return int(ke.Code)
	}
	return -1
}

func msg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
