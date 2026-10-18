// Command sarama is the IBM/sarama client driver of the clients track (SCENARIO.md).
package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/clients/drivers/golib"
)

const client = "sarama"

var (
	led = golib.NewLedger()
	in  golib.Inputs
)

func main() {
	in = golib.ReadInputs(led)
	// sarama implements only the classic consumer group protocol.
	led.Emit("start", map[string]any{"client": client, "version": golib.ModuleVersion("github.com/IBM/sarama"),
		"mode": in.Mode, "group_protocol": "classic"})
	if in.Mode == "scenario" {
		scenario()
	} else {
		stock()
	}
	led.Emit("done", nil)
}

func scenario() {
	if in.Admin {
		create()
	}
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1 // required by sarama for an idempotent producer
	cfg.Producer.Return.Successes = true
	prod, err := sarama.NewAsyncProducer([]string{in.Bootstrap}, cfg)
	if err != nil {
		led.Fail("producer", code(err), err)
	}
	produce(prod, 1, 0, in.Records, true)
	consume(1, in.Records, false)
	produce(prod, 2, in.Records, in.Records+in.Records2, true)
	if err := prod.Close(); err != nil {
		led.Fail("producer", code(err), err)
	}
	consume(2, in.Records2, true)
}

func create() {
	adm, err := sarama.NewClusterAdmin([]string{in.Bootstrap}, sarama.NewConfig())
	if err != nil {
		led.Fail("create", code(err), err)
	}
	defer adm.Close()
	err = adm.CreateTopic(in.Topic, &sarama.TopicDetail{NumPartitions: in.Partitions, ReplicationFactor: 1}, false)
	led.Emit("create", map[string]any{"topic": in.Topic, "partitions": in.Partitions, "error_code": code(err), "error": msg(err)})
	if err != nil {
		led.Fail("create", code(err), err)
	}
	// An application waits until the topic is usable before producing.
	deadline := time.Now().Add(in.Timeout)
	for time.Now().Before(deadline) {
		md, err := adm.DescribeTopics([]string{in.Topic})
		if err == nil && len(md) == 1 && md[0].Err == sarama.ErrNoError && int32(len(md[0].Partitions)) == in.Partitions && leaders(md[0]) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	led.Fail("create", -1, errors.New("topic never became ready"))
}

func leaders(t *sarama.TopicMetadata) bool {
	for _, p := range t.Partitions {
		if p.Leader < 0 || p.Err != sarama.ErrNoError {
			return false
		}
	}
	return true
}

// produce sends records lo..hi-1 on an async producer with successes and errors returned, and
// records each acknowledgement.
func produce(p sarama.AsyncProducer, phase, lo, hi int, keyed bool) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := lo; n < hi; n++ {
			select {
			case m := <-p.Successes():
				s := m.Metadata.(int)
				led.Emit("ack", map[string]any{"phase": phase, "seq": s, "partition": m.Partition, "offset": m.Offset, "error_code": 0})
			case e := <-p.Errors():
				s := e.Msg.Metadata.(int)
				led.Emit("ack", map[string]any{"phase": phase, "seq": s, "partition": -1, "offset": -1, "error_code": code(e.Err), "error": e.Err.Error()})
			case <-time.After(in.Timeout):
				led.Fail("produce", -1, fmt.Errorf("no acknowledgement within %s", in.Timeout))
			}
		}
	}()
	for seq := lo; seq < hi; seq++ {
		m := &sarama.ProducerMessage{Topic: in.Topic, Value: sarama.StringEncoder(golib.Value(seq)), Metadata: seq}
		if keyed {
			m.Key = sarama.StringEncoder(golib.Key(seq, in.Keys))
			m.Headers = []sarama.RecordHeader{{Key: []byte("seq"), Value: []byte(strconv.Itoa(seq))}, {Key: []byte("client"), Value: []byte(client)}}
		}
		p.Input() <- m
	}
	wg.Wait()
}

// handler is one consume phase's sarama.ConsumerGroupHandler.
type handler struct {
	phase   int
	want    int
	mu      sync.Mutex
	got     map[int]bool
	reached chan struct{} // closed when want records are held
	ready   chan struct{} // closed when every claim has reported its start offset
	claims  int
	started int
	once    sync.Once
	session sarama.ConsumerGroupSession // latest session, for the explicit commit
}

func newHandler(phase, want int) *handler {
	return &handler{phase: phase, want: want, got: map[int]bool{}, reached: make(chan struct{}), ready: make(chan struct{})}
}

func (h *handler) Setup(s sarama.ConsumerGroupSession) error {
	ps := append([]int32(nil), s.Claims()[in.Topic]...)
	sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
	led.Emit("assigned", map[string]any{"phase": h.phase, "partitions": ps})
	h.mu.Lock()
	h.claims, h.started, h.session = len(ps), 0, s
	h.mu.Unlock()
	return nil
}

func (h *handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *handler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	// sarama resolves the claim's start offset (committed, or the reset policy) before it calls
	// ConsumeClaim, so every claim having started is the stock-defaults barrier. InitialOffset can
	// still hold the policy placeholder (-1 for newest), so it is reported only when resolved.
	if o := c.InitialOffset(); o >= 0 {
		led.Emit("position", map[string]any{"phase": h.phase, "partition": c.Partition(), "offset": o})
	}
	h.mu.Lock()
	h.started++
	if h.started == h.claims {
		select {
		case <-h.ready:
		default:
			close(h.ready)
		}
	}
	h.mu.Unlock()
	for m := range c.Messages() {
		seq := golib.SeqOf(m.Value)
		hs := make([][2]string, 0, len(m.Headers))
		for _, hd := range m.Headers {
			hs = append(hs, [2]string{string(hd.Key), string(hd.Value)})
		}
		var key any
		if m.Key != nil {
			key = string(m.Key)
		}
		led.Emit("recv", map[string]any{"phase": h.phase, "seq": seq, "partition": m.Partition, "offset": m.Offset,
			"key": key, "value": string(m.Value), "headers": hs})
		s.MarkMessage(m, "")
		h.mu.Lock()
		h.got[seq] = true
		if len(h.got) >= h.want {
			h.once.Do(func() { close(h.reached) })
		}
		h.mu.Unlock()
	}
	return nil
}

// runGroup consumes in the background until ctx ends. The returned function waits for the
// group's error channel to close (after Close) and returns what it carried.
func runGroup(ctx context.Context, g sarama.ConsumerGroup, h *handler) (chan struct{}, func() []error) {
	done := make(chan struct{})
	errDone := make(chan struct{})
	var errs []error
	go func() {
		defer close(errDone)
		for e := range g.Errors() {
			errs = append(errs, e)
		}
	}()
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			if err := g.Consume(ctx, []string{in.Topic}, h); err != nil && ctx.Err() == nil {
				led.Fail("consume", code(err), err)
			}
		}
	}()
	return done, func() []error { <-errDone; return errs }
}

// consume runs one scenario consume phase: a new group member reads want records, keeps polling
// briefly when drain is set, commits synchronously and leaves.
func consume(phase, want int, drain bool) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	cfg.Consumer.Return.Errors = true
	g, err := sarama.NewConsumerGroup([]string{in.Bootstrap}, in.Group, cfg)
	if err != nil {
		led.Fail("consume", code(err), err)
	}
	h := newHandler(phase, want)
	ctx, cancel := context.WithCancel(context.Background())
	done, errs := runGroup(ctx, g, h)
	select {
	case <-h.reached:
	case <-time.After(in.Timeout):
		led.Fail("consume", -1, fmt.Errorf("received %d of %d records before the deadline", count(h), want))
	}
	if drain {
		time.Sleep(2 * time.Second)
	}
	h.mu.Lock()
	s := h.session
	h.mu.Unlock()
	s.Commit()
	cancel()
	<-done
	cerr := g.Close()
	var e error
	if es := errs(); len(es) > 0 {
		e = es[0]
	} else if cerr != nil {
		e = cerr
	}
	led.Emit("commit", map[string]any{"phase": phase, "error_code": code(e), "error": msg(e)})
}

func count(h *handler) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.got)
}

func stock() {
	// No admin step: sarama's default protocol version predates CreateTopics with broker-default
	// partitions, so the track creates the topic with the broker defaults (admin: false).
	g, err := sarama.NewConsumerGroup([]string{in.Bootstrap}, in.Group, sarama.NewConfig())
	if err != nil {
		led.Fail("consume", code(err), err)
	}
	h := newHandler(1, in.Records)
	ctx, cancel := context.WithCancel(context.Background())
	done, _ := runGroup(ctx, g, h)
	select {
	case <-h.ready:
	case <-time.After(in.Timeout):
		led.Fail("consume", -1, errors.New("consumer was never assigned partitions"))
	}
	pcfg := sarama.NewConfig()
	pcfg.Producer.Return.Successes = true // required by sarama's SyncProducer
	sp, err := sarama.NewSyncProducer([]string{in.Bootstrap}, pcfg)
	if err != nil {
		led.Fail("producer", code(err), err)
	}
	for seq := 0; seq < in.Records; seq++ {
		p, o, err := sp.SendMessage(&sarama.ProducerMessage{Topic: in.Topic, Value: sarama.StringEncoder(golib.Value(seq))})
		ev := map[string]any{"phase": 1, "seq": seq, "partition": p, "offset": o, "error_code": code(err)}
		if err != nil {
			ev["error"], ev["offset"] = err.Error(), -1
		}
		led.Emit("ack", ev)
	}
	_ = sp.Close()
	select {
	case <-h.reached:
	case <-time.After(in.Timeout):
		led.Fail("consume", -1, fmt.Errorf("received %d of %d records before the deadline", count(h), in.Records))
	}
	cancel()
	<-done
	_ = g.Close()
}

func code(err error) int {
	if err == nil {
		return 0
	}
	var ke sarama.KError
	if errors.As(err, &ke) {
		return int(ke)
	}
	var pe *sarama.ProducerError
	if errors.As(err, &pe) {
		return code(pe.Err)
	}
	return -1
}

func msg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
