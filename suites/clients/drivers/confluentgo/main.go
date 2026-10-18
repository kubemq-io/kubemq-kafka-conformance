// Command confluentgo is the confluent-kafka-go (librdkafka) driver of the clients track
// (suites/clients/SCENARIO.md). It is its own module because it links librdkafka through cgo;
// the ledger writer is a few lines, so it is inlined rather than shared. The ledger goes to
// stdout, one JSON object per line.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const client = "confluent-kafka-go"

var (
	mu   sync.Mutex
	enc  = json.NewEncoder(os.Stdout)
	held []int32 // partitions of the latest non-empty assignment

	bootstrap, mode, topic, group = os.Getenv("KCERT_BOOTSTRAP"), os.Getenv("KCERT_MODE"), os.Getenv("KCERT_TOPIC"), os.Getenv("KCERT_GROUP")
	partitions                    = num("KCERT_PARTITIONS", 0)
	records, records2, keys       = num("KCERT_RECORDS", 0), num("KCERT_RECORDS2", 0), num("KCERT_KEYS", 1)
	timeout                       = time.Duration(num("KCERT_TIMEOUT_SEC", 60)) * time.Second
	admin                         = os.Getenv("KCERT_ADMIN") != "0"
)

func num(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func emit(ev string, f map[string]any) {
	m := map[string]any{"ev": ev}
	for k, v := range f {
		m[k] = v
	}
	mu.Lock()
	defer mu.Unlock()
	_ = enc.Encode(m)
}

func fail(step string, err error) {
	emit("error", map[string]any{"step": step, "error_code": code(err), "error": err.Error()})
	emit("done", nil)
	os.Exit(1)
}

// code is the Kafka protocol error code; librdkafka's local errors (negative) map to -1.
func code(err error) int {
	if err == nil {
		return 0
	}
	var ke kafka.Error
	if errors.As(err, &ke) && ke.Code() >= 0 {
		return int(ke.Code())
	}
	return -1
}

func msg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func value(seq int) string { return fmt.Sprintf("value-%06d", seq) }

func seqOf(v []byte) int {
	var n int
	if _, err := fmt.Sscanf(string(v), "value-%d", &n); err != nil {
		return -1
	}
	return n
}

func version() string {
	v := "unknown"
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if d.Path == "github.com/confluentinc/confluent-kafka-go/v2" {
				v = d.Version
			}
		}
	}
	_, lib := kafka.LibraryVersion()
	return fmt.Sprintf("%s (librdkafka %s)", v, lib)
}

func main() {
	if bootstrap == "" || topic == "" || group == "" || (mode != "scenario" && mode != "stock-defaults") {
		fail("inputs", errors.New("KCERT_BOOTSTRAP, KCERT_TOPIC, KCERT_GROUP and KCERT_MODE are required"))
	}
	// librdkafka's documented default group.protocol is classic; the scenario sets it explicitly
	// (the allowed deviation), stock defaults leave it unset.
	emit("start", map[string]any{"client": client, "version": version(), "mode": mode, "group_protocol": "classic"})
	if mode == "scenario" {
		scenario()
	} else {
		stock()
	}
	emit("done", nil)
}

func create(parts int, rf int) {
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
	if err != nil {
		fail("create", err)
	}
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := a.CreateTopics(ctx, []kafka.TopicSpecification{{Topic: topic, NumPartitions: parts, ReplicationFactor: rf}})
	if err == nil && len(res) == 1 && res[0].Error.Code() != kafka.ErrNoError {
		err = res[0].Error
	}
	emit("create", map[string]any{"topic": topic, "partitions": parts, "error_code": code(err), "error": msg(err)})
	if err != nil {
		fail("create", err)
	}
	// An application waits until the topic is usable before producing.
	for ctx.Err() == nil {
		md, err := a.GetMetadata(&topic, false, 5000)
		if err == nil {
			if t, ok := md.Topics[topic]; ok && t.Error.Code() == kafka.ErrNoError && len(t.Partitions) > 0 && (parts < 0 || len(t.Partitions) == parts) {
				ready := true
				for _, p := range t.Partitions {
					ready = ready && p.Leader >= 0 && p.Error.Code() == kafka.ErrNoError
				}
				if ready {
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	fail("create", errors.New("topic never became ready"))
}

func produce(p *kafka.Producer, phase, lo, hi int, keyed bool) {
	dr := make(chan kafka.Event, hi-lo)
	for seq := lo; seq < hi; seq++ {
		m := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value: []byte(value(seq)), Opaque: seq}
		if keyed {
			m.Key = []byte(fmt.Sprintf("key-%02d", seq%keys))
			m.Headers = []kafka.Header{{Key: "seq", Value: []byte(strconv.Itoa(seq))}, {Key: "client", Value: []byte(client)}}
		}
		for {
			err := p.Produce(m, dr)
			if err == nil {
				break
			}
			if code(err) != -1 || err.(kafka.Error).Code() != kafka.ErrQueueFull {
				fail("produce", err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	for n := lo; n < hi; n++ {
		select {
		case e := <-dr:
			m := e.(*kafka.Message)
			s := m.Opaque.(int)
			if err := m.TopicPartition.Error; err != nil {
				emit("ack", map[string]any{"phase": phase, "seq": s, "partition": -1, "offset": -1, "error_code": code(err), "error": err.Error()})
			} else {
				emit("ack", map[string]any{"phase": phase, "seq": s, "partition": m.TopicPartition.Partition, "offset": int64(m.TopicPartition.Offset), "error_code": 0})
			}
		case <-time.After(timeout):
			fail("produce", fmt.Errorf("no delivery report within %s", timeout))
		}
	}
}

func rebalance(phase int, assigned chan struct{}) kafka.RebalanceCb {
	return func(c *kafka.Consumer, ev kafka.Event) error {
		if a, ok := ev.(kafka.AssignedPartitions); ok {
			ps := make([]int32, 0, len(a.Partitions))
			for _, tp := range a.Partitions {
				ps = append(ps, tp.Partition)
			}
			sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
			emit("assigned", map[string]any{"phase": phase, "partitions": ps})
			if len(ps) == 0 {
				return nil // an empty assignment is not readiness
			}
			mu.Lock()
			held = ps
			mu.Unlock()
			select {
			case assigned <- struct{}{}:
			default:
			}
		}
		return nil
	}
}

func recv(m *kafka.Message, phase int, got map[int]bool) {
	seq := seqOf(m.Value)
	got[seq] = true
	hs := make([][2]string, 0, len(m.Headers))
	for _, h := range m.Headers {
		hs = append(hs, [2]string{h.Key, string(h.Value)})
	}
	var key any
	if m.Key != nil {
		key = string(m.Key)
	}
	emit("recv", map[string]any{"phase": phase, "seq": seq, "partition": m.TopicPartition.Partition,
		"offset": int64(m.TopicPartition.Offset), "key": key, "value": string(m.Value), "headers": hs})
}

// pollFor reads for d, or until got holds want records when want > 0.
func pollFor(c *kafka.Consumer, phase int, got map[int]bool, want int, d time.Duration) {
	end := time.Now().Add(d)
	for time.Now().Before(end) && (want <= 0 || len(got) < want) {
		m, err := c.ReadMessage(200 * time.Millisecond)
		if err != nil {
			var ke kafka.Error
			if errors.As(err, &ke) && ke.Code() == kafka.ErrTimedOut {
				continue
			}
			fail("consume", err)
		}
		recv(m, phase, got)
	}
}

func consume(phase, want int, drain bool) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": bootstrap, "group.id": group,
		"group.protocol":    "classic", // the one allowed deviation
		"auto.offset.reset": "earliest", "enable.auto.commit": false})
	if err != nil {
		fail("consume", err)
	}
	if err := c.SubscribeTopics([]string{topic}, rebalance(phase, make(chan struct{}, 1))); err != nil {
		fail("consume", err)
	}
	got := map[int]bool{}
	pollFor(c, phase, got, want, timeout)
	if len(got) < want {
		fail("consume", fmt.Errorf("received %d of %d records before the deadline", len(got), want))
	}
	if drain {
		pollFor(c, phase, got, 0, 2*time.Second)
	}
	_, err = c.Commit()
	emit("commit", map[string]any{"phase": phase, "error_code": code(err), "error": msg(err)})
	_ = c.Close()
}

func scenario() {
	if admin {
		create(partitions, 1)
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": true})
	if err != nil {
		fail("producer", err)
	}
	produce(p, 1, 0, records, true)
	consume(1, records, false)
	produce(p, 2, records, records+records2, true)
	p.Close()
	consume(2, records2, true)
}

func stock() {
	if admin {
		create(-1, -1)
	}
	c, err := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": bootstrap, "group.id": group})
	if err != nil {
		fail("consume", err)
	}
	assigned := make(chan struct{}, 1)
	if err := c.SubscribeTopics([]string{topic}, rebalance(1, assigned)); err != nil {
		fail("consume", err)
	}
	got := map[int]bool{}
	deadline := time.Now().Add(timeout)
	for len(assigned) == 0 {
		if time.Now().After(deadline) {
			fail("consume", errors.New("consumer was never assigned partitions"))
		}
		pollFor(c, 1, got, 0, 200*time.Millisecond)
	}
	// Readiness barrier. librdkafka's default auto.offset.reset is latest and Position reports only
	// the last consumed offset + 1, so the barrier is the cached high watermark: librdkafka learns
	// it from the first fetch (or offset lookup) of a partition, which needs the start position
	// resolved. On the fresh topic that watermark is where the consumer starts.
	mu.Lock()
	pending := append([]int32(nil), held...)
	mu.Unlock()
	for len(pending) > 0 {
		if time.Now().After(deadline) {
			fail("consume", fmt.Errorf("start position never resolved on partitions %v", pending))
		}
		pollFor(c, 1, got, 0, 200*time.Millisecond)
		var left []int32
		for _, part := range pending {
			if _, hi, err := c.GetWatermarkOffsets(topic, part); err == nil && hi >= 0 {
				emit("position", map[string]any{"phase": 1, "partition": part, "offset": hi})
			} else {
				left = append(left, part)
			}
		}
		pending = left
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
	if err != nil {
		fail("producer", err)
	}
	produce(p, 1, 0, records, false)
	p.Close()
	pollFor(c, 1, got, records, timeout)
	if len(got) < records {
		fail("consume", fmt.Errorf("received %d of %d records before the deadline", len(got), records))
	}
	_ = c.Close()
}
