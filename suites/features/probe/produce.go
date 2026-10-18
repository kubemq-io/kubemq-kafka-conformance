package probe

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	register("produce-acks-0", produceAcks(kgo.NoAck()))
	register("produce-acks-1", produceAcks(kgo.LeaderAck()))
	register("produce-acks-all", produceAcks(kgo.AllISRAcks()))
	register("produce-codec-gzip", produceCodec(kgo.GzipCompression(), "gzip"))
	register("produce-codec-snappy", produceCodec(kgo.SnappyCompression(), "snappy"))
	register("produce-codec-lz4", produceCodec(kgo.Lz4Compression(), "lz4"))
	register("produce-codec-zstd", produceCodec(kgo.ZstdCompression(), "zstd"))
	register("produce-broker-recompression-gzip", brokerRecompression)
	register("produce-headers", produceHeaders)
	register("produce-default-partitioner", defaultPartitioner)
	register("produce-max-message-bytes", maxMessageBytes)
}

var codecNames = map[uint8]string{0: "none", 1: "gzip", 2: "snappy", 3: "lz4", 4: "zstd"}

// roundTrip creates a fresh topic, produces sent with the given producer options (each record
// to its Partition), consumes everything back and analyzes it. It also reports the codecs the
// broker returned the records in.
func roundTrip(ctx context.Context, env Env, name string, partitions int32, configs map[string]*string, sent []Sent, opts ...kgo.Opt) (Observation, error) {
	return roundTripWarm(ctx, env, name, partitions, configs, sent, false, opts...)
}

// roundTripWarm: with warm set, sent[0:partitions] (one record per partition) are produced
// first with acks=all, so every partition leader is known to accept writes before the rest is
// sent. acks=0 gives no delivery feedback, so without this the first records can be lost on
// any broker while a new partition is still becoming writable.
func roundTripWarm(ctx context.Context, env Env, name string, partitions int32, configs map[string]*string, sent []Sent, warm bool, opts ...kgo.Opt) (Observation, error) {
	all := sent
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := topicName(env, name)
	if err := createTopic(ctx, adm, topic, partitions, configs); err != nil {
		return nil, err
	}
	base := []kgo.Opt{kgo.SeedBrokers(env.Bootstrap), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.DefaultProduceTopic(topic)}
	if warm {
		w, err := kgo.NewClient(base...)
		if err != nil {
			return nil, err
		}
		for _, s := range sent[:partitions] {
			if err := w.ProduceSync(ctx, &kgo.Record{Partition: s.Partition, Key: s.Key, Value: s.Value}).FirstErr(); err != nil {
				w.Close()
				return Observation{"warmup_error_code": errCode(err)}, nil
			}
		}
		w.Close()
		sent = sent[partitions:]
	}
	pr, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	// Produce asynchronously and flush, so records travel in multi-record batches (a codec is
	// only applied to batches it shrinks). Records stay in send order per partition.
	var mu sync.Mutex
	firstErr := 0
	for _, s := range sent {
		r := &kgo.Record{Partition: s.Partition, Key: s.Key, Value: s.Value}
		for _, h := range s.Headers {
			r.Headers = append(r.Headers, kgo.RecordHeader{Key: string(h[0]), Value: h[1]})
		}
		pr.Produce(ctx, r, func(_ *kgo.Record, err error) {
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == 0 {
				firstErr = errCode(err)
			}
		})
	}
	if err := pr.Flush(ctx); err != nil {
		return nil, fmt.Errorf("flush: %w", err)
	}
	obs := Observation{"produce_error_code": firstErr}
	if firstErr != 0 {
		return obs, nil
	}
	recs, err := consumeAll(ctx, env, topic, len(all))
	if err != nil {
		obs["consume_error"] = "incomplete" // a category, never error text: it must not vary run to run
	}
	var got []Got
	codecs := map[string]bool{}
	for _, r := range recs {
		g := Got{Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value}
		for _, h := range r.Headers {
			g.Headers = append(g.Headers, [2][]byte{[]byte(h.Key), h.Value})
		}
		got = append(got, g)
		codecs[codecNames[r.Attrs.CompressionType()]] = true
	}
	for k, v := range AnalyzeRoundTrip(all, got).Observation() {
		obs[k] = v
	}
	if withCodecs(name) {
		obs["stored_codecs"] = sortedKeys(codecs)
	}
	return obs, nil
}

// withCodecs: only the codec rows observe stored codecs; elsewhere they depend on how records
// happen to batch and would flap.
func withCodecs(name string) bool {
	return strings.HasPrefix(name, "codec-") || name == "recompress"
}

// CodecPrecondition is a codec row's own precondition: on the oracle, every record must come
// back in the expected codec. If not, the producer never compressed (for example records sent
// one at a time, too small to compress) and the row would compare two uncompressed runs and
// read PASS while proving nothing; it is an error, so the row is VOID.
func CodecPrecondition(target string, obs Observation, want string) error {
	if target != "real-kafka" {
		return nil
	}
	got, _ := obs["stored_codecs"].([]string)
	if !slices.Equal(got, []string{want}) {
		return fmt.Errorf("probe precondition: the oracle returned codecs %v, not %s; the producer did not compress", got, want)
	}
	return nil
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// spread builds n records over partitions, values unique and compressible.
func spread(n int, partitions int32) []Sent {
	out := make([]Sent, n)
	for i := range out {
		out[i] = Sent{
			Partition: int32(i) % partitions,
			Key:       []byte(fmt.Sprintf("key-%04d", i)),
			Value:     []byte(fmt.Sprintf("value-%04d-%s", i, "abcdefghijabcdefghijabcdefghijabcdefghij")),
		}
	}
	return out
}

func produceAcks(acks kgo.Acks) Probe {
	return func(ctx context.Context, env Env) (Observation, error) {
		opts := []kgo.Opt{kgo.RequiredAcks(acks)}
		if acks != kgo.AllISRAcks() {
			opts = append(opts, kgo.DisableIdempotentWrite()) // idempotence requires acks=all
		}
		return roundTripWarm(ctx, env, "acks", 3, nil, spread(300, 3), true, opts...)
	}
}

func produceCodec(codec kgo.CompressionCodec, name string) Probe {
	return func(ctx context.Context, env Env) (Observation, error) {
		obs, err := roundTrip(ctx, env, "codec-"+name, 1, nil, spread(200, 1),
			kgo.ProducerBatchCompression(codec), kgo.ProducerLinger(50*time.Millisecond))
		if err != nil {
			return obs, err
		}
		return obs, CodecPrecondition(env.Target, obs, name)
	}
}

// brokerRecompression: the topic's compression.type is gzip, the producer sends lz4 batches.
// Kafka recompresses on append, so consumers receive gzip batches.
func brokerRecompression(ctx context.Context, env Env) (Observation, error) {
	gz := "gzip"
	obs, err := roundTrip(ctx, env, "recompress", 1, map[string]*string{"compression.type": &gz}, spread(200, 1),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()), kgo.ProducerLinger(50*time.Millisecond))
	if err != nil {
		return obs, err
	}
	return obs, CodecPrecondition(env.Target, obs, "gzip")
}

// produceHeaders: duplicate keys, an empty value, a null value, a non-ASCII key, many headers.
func produceHeaders(ctx context.Context, env Env) (Observation, error) {
	sets := [][][2][]byte{
		{{[]byte("a"), []byte("1")}, {[]byte("a"), []byte("2")}},
		{{[]byte("empty"), []byte{}}},
		{{[]byte("null"), nil}},
		{{[]byte("ключ-キー"), []byte("value")}},
		{},
	}
	var many [][2][]byte
	for i := 0; i < 50; i++ {
		many = append(many, [2][]byte{[]byte(fmt.Sprintf("h%02d", i)), []byte(fmt.Sprint(i))})
	}
	sets = append(sets, many)
	var sent []Sent
	for i := 0; i < 60; i++ {
		sent = append(sent, Sent{Partition: 0, Key: []byte(fmt.Sprint(i)), Value: []byte(fmt.Sprintf("v%d", i)), Headers: sets[i%len(sets)]})
	}
	return roundTrip(ctx, env, "headers", 1, nil, sent)
}

// defaultPartitioner: keyed records through the client's default partitioner must land on
// murmur2(key) mod partitions, the rule of the Kafka 4.x Java client; unkeyed records must all
// be delivered (their placement is uniform sticky, KIP-794, and not deterministic).
func defaultPartitioner(ctx context.Context, env Env) (Observation, error) {
	const partitions = 6
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := topicName(env, "partitioner")
	if err := createTopic(ctx, adm, topic, partitions, nil); err != nil {
		return nil, err
	}
	pr, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.DefaultProduceTopic(topic)) // default partitioner
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	mismatch, keyed, unkeyed := 0, 0, 0
	for i := 0; i < 600; i++ {
		r := &kgo.Record{Value: []byte(fmt.Sprintf("v%d", i))}
		if i%2 == 0 {
			r.Key = []byte(fmt.Sprintf("customer-%d", i%60))
		}
		if err := pr.ProduceSync(ctx, r).FirstErr(); err != nil {
			return Observation{"produce_error_code": errCode(err)}, nil
		}
		if r.Key != nil {
			keyed++
			if r.Partition != javaPartition(r.Key, partitions) {
				mismatch++
			}
		} else {
			unkeyed++
		}
	}
	recs, err := consumeAll(ctx, env, topic, 600)
	// Where the broker actually stored each keyed record, not where the client asked.
	stored := 0
	for _, r := range recs {
		if r.Key != nil && r.Partition != javaPartition(r.Key, partitions) {
			stored++
		}
	}
	obs := Observation{"keyed_records": keyed, "keyed_partition_mismatches": mismatch, "unkeyed_records": unkeyed,
		"consumed": len(recs), "consumed_keyed_partition_mismatches": stored}
	if err != nil {
		obs["consume_error"] = "incomplete"
	}
	return obs, nil
}

// javaPartition is org.apache.kafka.common.utils.Utils.toPositive(murmur2(key)) % n.
func javaPartition(key []byte, n int32) int32 {
	return int32((murmur2(key) & 0x7fffffff) % uint32(n))
}

func murmur2(data []byte) uint32 {
	const seed, m, r = uint32(0x9747b28c), uint32(0x5bd1e995), 24
	h := seed ^ uint32(len(data))
	for len(data) >= 4 {
		k := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
		data = data[4:]
	}
	switch len(data) {
	case 3:
		h ^= uint32(data[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[0])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// maxMessageBytes: a topic limited to 10 KiB accepts a 5 KiB record and refuses a 20 KiB one;
// a topic with the broker default refuses a 1.2 MiB record. Values are incompressible.
func maxMessageBytes(ctx context.Context, env Env) (Observation, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	limited, deflt := topicName(env, "maxbytes-limited"), topicName(env, "maxbytes-default")
	limit := "10240"
	if err := createTopic(ctx, adm, limited, 1, map[string]*string{"max.message.bytes": &limit}); err != nil {
		return nil, err
	}
	if err := createTopic(ctx, adm, deflt, 1, nil); err != nil {
		return nil, err
	}
	pr, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.ProducerBatchMaxBytes(4<<20),
		kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.RecordRetries(1))
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	rnd := rand.New(rand.NewSource(1))
	payload := func(n int) []byte { b := make([]byte, n); rnd.Read(b); return b }
	code := func(topic string, n int) int {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return errCode(pr.ProduceSync(c, &kgo.Record{Topic: topic, Value: payload(n)}).FirstErr())
	}
	return Observation{
		"limited_5KiB_error_code":    code(limited, 5*1024),
		"limited_20KiB_error_code":   code(limited, 20*1024),
		"default_1200KiB_error_code": code(deflt, 1200*1024),
	}, nil
}
