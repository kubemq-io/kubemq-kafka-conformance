package probe

// Operations rows: Kafka's own internal topics, read with Kafka's own formatters, as lag and
// monitoring tools (Burrow, offset explorers) read them.

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
)

func init() {
	register("ops-consumer-offsets-topic", opsConsumerOffsetsTopic)
	register("ops-transaction-state-topic", opsTransactionStateTopic)
}

// readInternal reads an internal topic from the beginning with a Kafka formatter until idle.
func (e Env) readInternal(ctx context.Context, topic, formatter string) (out string, processed int) {
	r := e.Box.Tool(ctx, "", 2*time.Minute, "kafka-console-consumer.sh", e.bs("--topic", topic, "--from-beginning",
		"--timeout-ms", "10000", "--formatter", formatter)...)
	return r.Stdout, kout.ProcessedTotal(r.Stderr)
}

// OffsetCommitsIn returns, for group and topic, partition to the last committed offset found in
// OffsetsMessageFormatter output (records are printed back to back, without separators).
func OffsetCommitsIn(out, group, topic string) map[int]int64 {
	re := regexp.MustCompile(`"group":"` + regexp.QuoteMeta(group) + `","topic":"` + regexp.QuoteMeta(topic) + `","partition":(\d+)\}\},"value":\{"version":\d+,"data":\{"offset":(-?\d+)`)
	m := map[int]int64{}
	for _, f := range re.FindAllStringSubmatch(out, -1) {
		p, _ := strconv.Atoi(f[1])
		o, _ := strconv.ParseInt(f[2], 10, 64)
		m[p] = o
	}
	return m
}

func opsConsumerOffsetsTopic(ctx context.Context, env Env) (Observation, error) {
	topic, group, _, err := env.seededGroup(ctx, "offsets-topic", 30)
	if err != nil {
		return nil, err
	}
	committed := map[int]int64{}
	for _, r := range env.describeGroup(ctx, group) {
		committed[r.Partition] = r.Current
	}
	out, processed := env.readInternal(ctx, "__consumer_offsets", "org.apache.kafka.tools.consumer.OffsetsMessageFormatter")
	seen := OffsetCommitsIn(out, group, topic)
	return Observation{
		"offsets_topic_listed":                env.listed(ctx, "kafka-topics.sh", "__consumer_offsets", "--list"),
		"offsets_topic_readable":              processed > 0,
		"group_commits_found":                 len(seen) > 0,
		"group_commits_match_committed":       kout.OffsetList(seen) == kout.OffsetList(committed),
		"group_committed_partitions_describe": len(committed),
	}, nil
}

func opsTransactionStateTopic(ctx context.Context, env Env) (Observation, error) {
	t := name(env, "txstate")
	if err := env.createTopic(ctx, t, 1); err != nil {
		return nil, err
	}
	txid := name(env, "txid")
	perf := env.tool(ctx, "", "kafka-producer-perf-test.sh", "--topic", t, "--num-records", "100", "--record-size", "50",
		"--throughput", "-1", "--transactional-id", txid, "--transaction-duration-ms", "100",
		"--producer-props", "bootstrap.servers="+env.Bootstrap)
	if perf.Exit != 0 {
		return nil, fmt.Errorf("transactional producer: %s", firstErrorLine(perf.Combined()))
	}
	out, processed := env.readInternal(ctx, "__transaction_state", "org.apache.kafka.tools.consumer.TransactionLogMessageFormatter")
	return Observation{
		"transactional_records_sent":    kout.PerfProducerSent(perf.Stdout),
		"transaction_state_listed":      env.listed(ctx, "kafka-topics.sh", "__transaction_state", "--list"),
		"transaction_state_readable":    processed > 0,
		"transactional_id_record_found": regexp.MustCompile(`"transactionalId":"` + regexp.QuoteMeta(txid) + `"`).MatchString(out),
	}, nil
}
