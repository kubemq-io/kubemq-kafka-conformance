package probe

// compaction_java.go — compacted topics written in each codec, read back by the Java consumer.
// A log cleaner rewrites batches; a rewritten compressed batch that the Java client cannot decode
// (wrong codec framing, broken CRC, offsets out of order) would pass a franz-go read-back and still
// break every JVM application. So after compaction the topic is consumed by Apache Kafka's own
// Java consumer (kafka-console-consumer from the pinned Kafka image), run in the broker's network
// namespace through runner/client-run.sh.

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// JavaClient says how to start the Java consumer: the runner's client-run script and the Kafka
// image (pinned by digest) whose kafka-console-consumer is the Java client. Set by the track.
var JavaClient struct {
	ClientRun string
	Image     string
}

func init() {
	register("log-compaction-java-gzip", compactionJava(kgo.GzipCompression(), "gzip"))
	register("log-compaction-java-snappy", compactionJava(kgo.SnappyCompression(), "snappy"))
	register("log-compaction-java-lz4", compactionJava(kgo.Lz4Compression(), "lz4"))
	register("log-compaction-java-zstd", compactionJava(kgo.ZstdCompression(), "zstd"))
}

// compactionJava: the compaction scenario of log-compaction-batched-compressed (keys k0..k9, five
// versions each, async batches in the codec, then a k0 tombstone), then a read-back of the whole
// topic by the Java consumer. On the oracle the records must be stored in the codec before
// compaction, or the probe never tested that codec (precondition, VOID).
func compactionJava(codec kgo.CompressionCodec, name string) Probe {
	return func(ctx context.Context, env Env) (Observation, error) {
		if JavaClient.ClientRun == "" || JavaClient.Image == "" || env.Container == "" {
			return nil, fmt.Errorf("the Java consumer is not configured (client-run script, Kafka image and broker container are set by suites/features/track.sh)")
		}
		cl, adm, err := adminClient(env)
		if err != nil {
			return nil, err
		}
		defer cl.Close()
		topic := topicName(env, "compact-java-"+name)
		if err := createTopic(ctx, adm, topic, 1, map[string]*string{
			"cleanup.policy": str("compact"), "segment.ms": str("1000"), "min.cleanable.dirty.ratio": str("0.01"),
			"delete.retention.ms": str("5000"), "min.compaction.lag.ms": str("0")}); err != nil {
			return nil, err
		}
		pr, err := producer(env, topic, kgo.ProducerBatchCompression(codec), kgo.ProducerLinger(50*time.Millisecond))
		if err != nil {
			return nil, err
		}
		defer pr.Close()
		for v := 1; v <= 5; v++ {
			for k := 0; k < 10; k++ {
				pr.Produce(ctx, &kgo.Record{Key: []byte(fmt.Sprintf("k%d", k)), Value: []byte(fmt.Sprintf("v%d-%s", v, "payload-payload-payload-payload"))}, nil)
			}
		}
		if err := pr.Flush(ctx); err != nil {
			return nil, err
		}
		if env.Target == "real-kafka" {
			recs, err := readAll(ctx, env, adm, topic)
			if err != nil {
				return nil, err
			}
			stored := map[string]bool{}
			for _, r := range recs {
				stored[codecNames[r.Attrs.CompressionType()]] = true
			}
			if got := sortedKeys(stored); len(got) != 1 || got[0] != name {
				return nil, fmt.Errorf("probe precondition: the oracle stored codecs %v, not %s; the producer did not compress", got, name)
			}
		}
		if err := pr.ProduceSync(ctx, &kgo.Record{Key: []byte("k0"), Value: nil}).FirstErr(); err != nil {
			return Observation{"tombstone_error_code": errCode(err)}, nil
		}
		compacted, latestOK, tombstoneGone := waitCompacted(ctx, env, adm, pr, topic)
		obs := Observation{"compacted_within_120s": compacted, "latest_value_per_key_kept": latestOK, "tombstone_removed_within_120s": tombstoneGone}
		jobs, err := javaReadBack(ctx, env, topic)
		if err != nil {
			return nil, err
		}
		for k, v := range jobs {
			obs[k] = v
		}
		return obs, nil
	}
}

// javaExceptionLine matches a Java exception class at the start of a stderr line.
var javaExceptionLine = regexp.MustCompile(`^(?:Caused by: )?([a-z][a-z0-9_]*(?:\.[a-zA-Z0-9_$]+)+(?:Exception|Error))\b`)

// javaReadBack consumes the whole topic with the Java consumer (stops after 10 s without a new
// record) and reports what a JVM application would see: whether the consumer ended cleanly, any
// exception other than the idle timeout, how many non-roll records it read, whether k1..k9 hold
// exactly their latest value, and whether k0 (tombstoned, past delete.retention.ms) is gone.
// A consumer that never reached its summary line did not run (the container did not start, or
// the JVM died); that is an error of the probe, not an observation, and carries its output.
func javaReadBack(ctx context.Context, env Env, topic string) (Observation, error) {
	c, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(c, JavaClient.ClientRun, "--network", "container:"+env.Container, "--",
		JavaClient.Image, "/opt/kafka/bin/kafka-console-consumer.sh",
		"--bootstrap-server", env.Bootstrap, "--topic", topic, "--from-beginning", "--timeout-ms", "10000",
		"--formatter-property", "print.key=true", "--formatter-property", "key.separator=|",
		"--formatter-property", "null.literal=<tombstone>")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	runErr := cmd.Run()
	if !strings.Contains(stderr.String(), "Processed a total of ") {
		return nil, fmt.Errorf("the Java consumer did not run (%v): %s", runErr, tail(stderr.String()+stdout.String(), 600))
	}
	return parseJavaReadBack(stdout.Bytes(), stderr.Bytes(), runErr), nil
}

// tail: the last n bytes of s, on one line.
func tail(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		s = "..." + s[len(s)-n:]
	}
	return s
}

// parseJavaReadBack turns the console consumer's output into the observation. Pure, unit-tested.
func parseJavaReadBack(stdoutB, stderrB []byte, runErr error) Observation {
	stdout, stderr := bytes.NewBuffer(stdoutB), bytes.NewBuffer(stderrB)
	exceptions := map[string]bool{}
	processed := -1
	sc := bufio.NewScanner(stderr)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if m := javaExceptionLine.FindStringSubmatch(line); m != nil && m[1] != "org.apache.kafka.common.errors.TimeoutException" {
			exceptions[m[1]] = true
		}
		if rest, ok := strings.CutPrefix(line, "Processed a total of "); ok {
			if n, err := strconv.Atoi(strings.Fields(rest)[0]); err == nil {
				processed = n
			}
		}
	}
	perKey := map[string][]string{}
	read := 0
	sc = bufio.NewScanner(stdout)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), "|")
		if !ok || k == "roll" {
			continue
		}
		read++
		perKey[k] = append(perKey[k], v)
	}
	latest := true
	for k := 1; k < 10; k++ {
		vs := perKey[fmt.Sprintf("k%d", k)]
		if len(vs) != 1 || !strings.HasPrefix(vs[0], "v5-") {
			latest = false
		}
	}
	exc := make([]string, 0, len(exceptions))
	for e := range exceptions {
		exc = append(exc, e)
	}
	sort.Strings(exc)
	return Observation{
		"java_consumer_ended_cleanly": runErr == nil && processed >= 0,
		"java_exceptions":             exc,
		"java_records_read":           read,
		"java_latest_value_per_key":   latest,
		"java_tombstoned_key_absent":  len(perKey["k0"]) == 0,
	}
}
