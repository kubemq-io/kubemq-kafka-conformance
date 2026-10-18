// Command segmentio is the segmentio/kafka-go client driver of the clients track (SCENARIO.md).
package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/clients/drivers/golib"
)

const client = "segmentio/kafka-go"

var (
	led      = golib.NewLedger()
	in       golib.Inputs
	acksZero bool // the writer runs without broker acknowledgement: offsets are unknown
)

func main() {
	in = golib.ReadInputs(led)
	// kafka-go implements only the classic group protocol. Its Reader does not expose the
	// partitions the group assigned to it, so the driver says so and the verifier skips only that
	// check (assignment_hidden).
	// Its Writer's default RequiredAcks is RequireNone (acks=0): in stock defaults the broker does
	// not acknowledge, so no offset is known (ack_offsets_unknown, offsets reported as -1).
	led.Emit("start", map[string]any{"client": client, "version": golib.ModuleVersion("github.com/segmentio/kafka-go"),
		"mode": in.Mode, "group_protocol": "classic", "assignment_hidden": true, "ack_offsets_unknown": in.Mode != "scenario"})
	if in.Mode == "scenario" {
		scenario()
	} else {
		stock()
	}
	led.Emit("done", nil)
}

func scenario() {
	if in.Admin {
		create(int(in.Partitions), 1)
	}
	w := &kafka.Writer{Addr: kafka.TCP(in.Bootstrap), Topic: in.Topic, Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, Completion: completion(1)}
	produce(w, 0, in.Records, true)
	consume(1, in.Records, false)
	w.Completion = completion(2)
	produce(w, in.Records, in.Records+in.Records2, true)
	_ = w.Close()
	consume(2, in.Records2, true)
}

func stock() {
	if in.Admin {
		create(-1, -1)
	}
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{in.Bootstrap}, GroupID: in.Group, Topic: in.Topic})
	w := &kafka.Writer{Addr: kafka.TCP(in.Bootstrap), Topic: in.Topic, Completion: completion(1)}
	acksZero = w.RequiredAcks == kafka.RequireNone
	produce(w, 0, in.Records, false)
	_ = w.Close()
	// The upstream quickstart reads with ReadMessage, which commits each message for a group.
	got := map[int]bool{}
	ctx, cancel := context.WithTimeout(context.Background(), in.Timeout)
	defer cancel()
	for len(got) < in.Records {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			led.Fail("consume", code(err), fmt.Errorf("received %d of %d records: %w", len(got), in.Records, err))
		}
		recv(m, 1, got)
	}
	_ = r.Close()
}

func create(partitions, rf int) {
	cl := &kafka.Client{Addr: kafka.TCP(in.Bootstrap)}
	ctx, cancel := context.WithTimeout(context.Background(), in.Timeout)
	defer cancel()
	resp, err := cl.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{
		{Topic: in.Topic, NumPartitions: partitions, ReplicationFactor: rf}}})
	if err == nil {
		err = resp.Errors[in.Topic]
	}
	led.Emit("create", map[string]any{"topic": in.Topic, "partitions": partitions, "error_code": code(err), "error": msg(err)})
	if err != nil {
		led.Fail("create", code(err), err)
	}
	// An application waits until the topic is usable before producing.
	for ctx.Err() == nil {
		md, err := cl.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{in.Topic}})
		if err == nil && len(md.Topics) == 1 && md.Topics[0].Error == nil && len(md.Topics[0].Partitions) > 0 &&
			(partitions < 0 || len(md.Topics[0].Partitions) == partitions) {
			ok := true
			for _, p := range md.Topics[0].Partitions {
				ok = ok && p.Error == nil && p.Leader.ID >= 0
			}
			if ok {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	led.Fail("create", -1, errors.New("topic never became ready"))
}

// completion records one acknowledgement per message of a written batch.
func completion(phase int) func([]kafka.Message, error) {
	return func(ms []kafka.Message, err error) {
		for _, m := range ms {
			ev := map[string]any{"phase": phase, "seq": golib.SeqOf(m.Value), "partition": m.Partition, "offset": m.Offset, "error_code": code(err)}
			if err != nil {
				ev["error"], ev["partition"], ev["offset"] = err.Error(), -1, -1
			} else if acksZero {
				ev["offset"] = -1
			}
			led.Emit("ack", ev)
		}
	}
}

func produce(w *kafka.Writer, lo, hi int, keyed bool) {
	ms := make([]kafka.Message, 0, hi-lo)
	for seq := lo; seq < hi; seq++ {
		m := kafka.Message{Value: []byte(golib.Value(seq))}
		if keyed {
			m.Key = []byte(golib.Key(seq, in.Keys))
			m.Headers = []kafka.Header{{Key: "seq", Value: []byte(strconv.Itoa(seq))}, {Key: "client", Value: []byte(client)}}
		}
		ms = append(ms, m)
	}
	ctx, cancel := context.WithTimeout(context.Background(), in.Timeout)
	defer cancel()
	if err := w.WriteMessages(ctx, ms...); err != nil {
		led.Fail("produce", code(err), err)
	}
}

// read fetches until got holds want records; it returns the messages fetched, for the commit.
func read(r *kafka.Reader, phase int, got map[int]bool, want int) []kafka.Message {
	ctx, cancel := context.WithTimeout(context.Background(), in.Timeout)
	defer cancel()
	var out []kafka.Message
	for len(got) < want {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			led.Fail("consume", code(err), fmt.Errorf("received %d of %d records: %w", len(got), want, err))
		}
		out = append(out, m)
		recv(m, phase, got)
	}
	return out
}

func recv(m kafka.Message, phase int, got map[int]bool) {
	seq := golib.SeqOf(m.Value)
	got[seq] = true
	hs := make([][2]string, 0, len(m.Headers))
	for _, h := range m.Headers {
		hs = append(hs, [2]string{h.Key, string(h.Value)})
	}
	var key any
	if m.Key != nil {
		key = string(m.Key)
	}
	led.Emit("recv", map[string]any{"phase": phase, "seq": seq, "partition": m.Partition, "offset": m.Offset,
		"key": key, "value": string(m.Value), "headers": hs})
}

// consume runs one scenario consume phase: a new group member reads want records, keeps reading
// briefly when drain is set, commits synchronously and leaves.
func consume(phase, want int, drain bool) {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{in.Bootstrap}, GroupID: in.Group, Topic: in.Topic,
		StartOffset: kafka.FirstOffset, CommitInterval: 0, MaxWait: 500 * time.Millisecond})
	got := map[int]bool{}
	ms := read(r, phase, got, want)
	if drain {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		for {
			m, err := r.FetchMessage(ctx)
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			if err != nil {
				led.Fail("consume", code(err), fmt.Errorf("drain: %w", err))
			}
			ms = append(ms, m)
			recv(m, phase, got)
		}
		cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), in.Timeout)
	err := r.CommitMessages(ctx, ms...)
	cancel()
	led.Emit("commit", map[string]any{"phase": phase, "error_code": code(err), "error": msg(err)})
	_ = r.Close()
}

func code(err error) int {
	if err == nil {
		return 0
	}
	var ke kafka.Error
	if errors.As(err, &ke) {
		return int(ke)
	}
	var we kafka.WriteErrors
	if errors.As(err, &we) {
		for _, e := range we {
			if e != nil {
				return code(e)
			}
		}
	}
	return -1
}

func msg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
