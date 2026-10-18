// Package mirror runs MirrorMaker 2 (connect-mirror-maker.sh, dedicated mode) and a second,
// real Apache Kafka broker from the pinned Kafka image, for the tools track's MirrorMaker 2 rows
// and the operations track's migration rows. Every container goes through suites/tools/box, so
// only the engine abstraction is called.
//
// A Cluster is one broker as seen from two sides: Bootstrap is what Kafka tools dial (from the
// container they run in), Advertise is what MirrorMaker 2 dials (from its own container).
package mirror

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/box"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
)

// Cluster is one broker and the container its tools run in.
type Cluster struct {
	Box       *box.Box
	Bootstrap string
	Advertise string
}

// Tool runs a Kafka tool against the cluster.
func (c Cluster) Tool(ctx context.Context, stdin, tool string, args ...string) box.Result {
	return c.Box.Tool(ctx, stdin, 2*time.Minute, tool, append([]string{"--bootstrap-server", c.Bootstrap}, args...)...)
}

// Listed reports whether topic exists.
func (c Cluster) Listed(ctx context.Context, topic string) bool {
	for _, t := range kout.Lines(c.Tool(ctx, "", "kafka-topics.sh", "--list").Stdout) {
		if t == topic {
			return true
		}
	}
	return false
}

// Topics lists every topic.
func (c Cluster) Topics(ctx context.Context) []string {
	return kout.Lines(c.Tool(ctx, "", "kafka-topics.sh", "--list").Stdout)
}

// EndOffsets returns partition to log-end offset.
func (c Cluster) EndOffsets(ctx context.Context, topic string) map[int]int64 {
	return kout.ParseGetOffsets(c.Tool(ctx, "", "kafka-get-offsets.sh", "--topic", topic, "--time", "-1").Stdout, topic)
}

// CleanupPolicy is the topic's effective cleanup.policy.
func (c Cluster) CleanupPolicy(ctx context.Context, topic string) string {
	return kout.ParseConfigsDescribe(c.Tool(ctx, "", "kafka-configs.sh", "--describe", "--all", "--entity-type", "topics", "--entity-name", topic).Stdout)["cleanup.policy"]
}

// GroupOffsets returns partition to committed offset of group on topic (partitions without a
// commit are absent).
func (c Cluster) GroupOffsets(ctx context.Context, group, topic string) map[int]int64 {
	out := map[int]int64{}
	for _, row := range kout.ParseGroupDescribe(c.Tool(ctx, "", "kafka-consumer-groups.sh", "--describe", "--group", group).Stdout, group) {
		if row.Topic == topic && row.Current >= 0 {
			out[row.Partition] = row.Current
		}
	}
	return out
}

// Drain reads the first n records of topic: one line per record with partition, offset, headers,
// key and value, as the console consumer prints them.
func (c Cluster) Drain(ctx context.Context, topic string, n int64) ([]string, error) {
	r := c.Tool(ctx, "", "kafka-console-consumer.sh", "--topic", topic, "--from-beginning", "--max-messages", fmt.Sprint(n), "--timeout-ms", "20000",
		"--formatter-property", "print.partition=true", "--formatter-property", "print.offset=true",
		"--formatter-property", "print.headers=true", "--formatter-property", "print.key=true")
	var out []string
	for _, l := range strings.Split(r.Stdout, "\n") {
		if strings.HasPrefix(l, "Partition:") {
			out = append(out, l)
		}
	}
	if kout.ProcessedTotal(r.Stderr) != len(out) {
		return out, fmt.Errorf("drain %s: consumer reported %d records, read %d lines", topic, kout.ProcessedTotal(r.Stderr), len(out))
	}
	return out, nil
}

// StartKafka starts a single-node KRaft Apache Kafka broker from the pinned image on network,
// advertised as its container name, and waits until it answers. Its log is followed into
// logPath. Tools run inside its own container. stop removes it.
func StartKafka(ctx context.Context, name, network, logPath string) (c Cluster, stop func(), err error) {
	opts := append(box.JavaEnv(), "--network", network,
		"-e", "KAFKA_NODE_ID=1", "-e", "KAFKA_PROCESS_ROLES=broker,controller",
		"-e", "CLUSTER_ID=4L6g3nShT-eMCtK--X86sw",
		"-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093", "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
		"-e", "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://localhost:9093",
		"-e", "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://"+name+":9092",
		"-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
		"-e", "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
		"-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1", "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
		"-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1", "-e", "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=1",
		"-e", "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=1", "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
		"-e", "KAFKA_HEAP_OPTS=-Xmx768m -Xms256m")
	b, err := box.Start(ctx, name, opts, box.Image())
	if err != nil {
		return c, nil, err
	}
	stopLog := func() {}
	if logPath != "" {
		s, err := box.Follow(name, logPath)
		if err != nil {
			b.Remove()
			return c, nil, fmt.Errorf("follow the log of %s: %w", name, err)
		}
		stopLog = s
	}
	stop = func() { b.Remove(); stopLog() }
	c = Cluster{Box: b, Bootstrap: "localhost:9092", Advertise: name + ":9092"}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if b.Tool(ctx, "", 30*time.Second, "kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092").Exit == 0 {
			return c, stop, nil
		}
		time.Sleep(2 * time.Second)
	}
	stop()
	return c, nil, fmt.Errorf("Kafka broker %s did not become ready in 90 s", name)
}

// Spec is one MirrorMaker 2 flow, SrcAlias to TgtAlias.
type Spec struct {
	Name     string // container name
	Network  string // the network the second broker is on
	ConfDir  string // host directory for the properties file (mounted read-only)
	SrcAlias string
	TgtAlias string
	Src, Tgt Cluster
	Topics   string // MirrorMaker 2 topics regex
	Groups   string // MirrorMaker 2 groups regex
	// Extra properties, added after (and overriding) the defaults below.
	Extra map[string]string
}

// Properties renders the dedicated-mode configuration: one enabled flow, offset syncs on the
// target, one copy of every internal topic (broker default), 2 s sync, checkpoint and heartbeat
// intervals, offset.lag.max=0 so every record can be a sync point. Kept from the kubemq-server
// MirrorMaker 2 leg.
func Properties(s Spec) string {
	flow := s.SrcAlias + "->" + s.TgtAlias
	kv := map[string]string{
		"clusters":                                  s.SrcAlias + ", " + s.TgtAlias,
		s.SrcAlias + ".bootstrap.servers":           s.Src.Advertise,
		s.TgtAlias + ".bootstrap.servers":           s.Tgt.Advertise,
		flow + ".enabled":                           "true",
		s.TgtAlias + "->" + s.SrcAlias + ".enabled": "false",
		flow + ".topics":                            s.Topics,
		flow + ".groups":                            s.Groups,
		flow + ".offset-syncs.topic.location":       "target",
		"replication.factor":                        "-1",
		"checkpoints.topic.replication.factor":      "-1",
		"heartbeats.topic.replication.factor":       "-1",
		"offset-syncs.topic.replication.factor":     "-1",
		"offset.storage.replication.factor":         "-1",
		"status.storage.replication.factor":         "-1",
		"config.storage.replication.factor":         "-1",
		"sync.group.offsets.enabled":                "true",
		"sync.group.offsets.interval.seconds":       "2",
		"emit.checkpoints.interval.seconds":         "2",
		"emit.heartbeats.interval.seconds":          "2",
		"refresh.topics.interval.seconds":           "5",
		"refresh.groups.interval.seconds":           "5",
		"offset.lag.max":                            "0",
	}
	for k, v := range s.Extra {
		kv[k] = v
	}
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s = %s\n", k, kv[k])
	}
	return b.String()
}

// Start writes the properties and starts MirrorMaker 2 on the spec's network, following its log
// into logPath. stop removes the container.
func Start(ctx context.Context, s Spec, logPath string) (mm *box.Box, stop func(), err error) {
	file := s.Name + ".properties"
	if err := os.WriteFile(filepath.Join(s.ConfDir, file), []byte(Properties(s)), 0o644); err != nil {
		return nil, nil, err
	}
	opts := append(box.JavaEnv(), "--network", s.Network, "-v", s.ConfDir+":/conf:ro",
		"-e", "KAFKA_HEAP_OPTS=-Xms256m -Xmx768m", "-e", "LOG_DIR=/tmp/mm2-logs",
		"--entrypoint", "/opt/kafka/bin/connect-mirror-maker.sh")
	mm, err = box.Start(ctx, s.Name, opts, box.Image(), "/conf/"+file)
	if err != nil {
		return nil, nil, err
	}
	stopLog := func() {}
	if logPath != "" {
		f, err := box.Follow(s.Name, logPath)
		if err != nil {
			mm.Remove()
			return nil, nil, fmt.Errorf("follow the log of %s: %w", s.Name, err)
		}
		stopLog = f
	}
	return mm, func() { mm.Remove(); stopLog() }, nil
}

// WaitRemote waits until topic exists on tgt and holds at least n records.
func WaitRemote(ctx context.Context, tgt Cluster, topic string, n int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if tgt.Listed(ctx, topic) && sum(tgt.EndOffsets(ctx, topic)) >= n {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

func sum(m map[int]int64) int64 {
	var s int64
	for _, v := range m {
		s += v
	}
	return s
}

var topicIDLine = regexp.MustCompile(`TopicId: (\S+)`)

// TopicIDPresent reports whether kafka-topics.sh --describe output shows a real topic id (not
// empty and not the all-zero id).
func TopicIDPresent(describe string) bool {
	m := topicIDLine.FindStringSubmatch(describe)
	return m != nil && m[1] != "AAAAAAAAAAAAAAAAAAAAAA"
}

// QuoteGroup returns a MirrorMaker 2 groups regex matching exactly one group name.
func QuoteGroup(group string) string { return regexp.QuoteMeta(group) }
