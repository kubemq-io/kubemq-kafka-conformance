package probe

// Lag and monitoring tools: Burrow, kafka-lag-exporter and Kafbat UI, third-party images pinned by
// digest in compose/versions.env, pulled at run time and
// never redistributed. One scenario per broker: a 3-partition topic with 60 keyed records and a
// group that consumed part of it, so every partition has a known committed offset and lag
// (read from kafka-consumer-groups.sh, the reference). Each tool runs in its own container on a
// private network and reaches the broker through the host-gateway alias; its HTTP API is read
// from a client container on the same network.
//
// The kit runs both brokers with offsets.retention.minutes=1 (for the features track's retention
// row), so an empty group's committed offsets expire about a minute after its consumer leaves.
// Each tool therefore gets its own group, created only once the tool answers, and is read
// within that minute.
//
// Instrument facts found on real Apache Kafka 4.3.1 (kept so nobody re-discovers them):
//   - Burrow v1.9.4 (the latest release) cannot decode Kafka 4.x offset-commit records in
//     __consumer_offsets ("failed to decode"), so it lists groups (from group-metadata records)
//     but has no committed offsets and answers 404 for a group's lag, on Kafka itself. A broker
//     that writes value schema v3 (KubeMQ) gets lag; group_lag_available is therefore
//     informational, recorded but not compared (expect.go, Informational).
//   - kafka-lag-exporter (archived upstream, last release 0.8.2) reads lag through the admin API.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/box"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
)

func init() {
	for _, s := range []string{"burrow", "kafka-lag-exporter", "kafbat-ui"} {
		register("lag-"+s, sectionProbe("lag", lagScenario, s))
	}
}

type lagRun struct {
	env      Env
	dir      string
	net      string
	topic    string
	group    string
	lag      map[int]int64 // per-partition lag from kafka-consumer-groups.sh
	commit   map[int]int64
	ends     map[int]int64
	client   *box.Box
	stops    []func()
	contName func(role string) string
}

func (l *lagRun) cleanup() {
	for i := len(l.stops) - 1; i >= 0; i-- {
		l.stops[i]()
	}
}

// get reads an HTTP URL from the client container.
func (l *lagRun) get(ctx context.Context, url string) (string, error) {
	r := l.client.Sh(ctx, "", 20*time.Second, "wget -qO- '"+url+"'")
	if r.Exit != 0 {
		return r.Stdout, fmt.Errorf("GET %s: %s", url, strings.TrimSpace(r.Stderr))
	}
	return r.Stdout, nil
}

// start runs a tool container on the scenario's network and follows its log.
func (l *lagRun) start(ctx context.Context, role, image string, opts ...string) (*box.Box, error) {
	return l.startCmd(ctx, role, image, opts)
}

// startCmd is start with an explicit container command.
func (l *lagRun) startCmd(ctx context.Context, role, image string, opts []string, cmd ...string) (*box.Box, error) {
	name := l.contName(role)
	b, err := box.Start(ctx, name, append([]string{"--network", l.net}, opts...), image, cmd...)
	if err != nil {
		return nil, err
	}
	stopLog := func() {}
	if s, err := box.Follow(name, filepath.Join(l.dir, "logs", role+".log")); err == nil {
		stopLog = s
	}
	l.stops = append(l.stops, func() { b.Remove(); stopLog() })
	return b, nil
}

// freshGroup makes a new group consume 30 of the topic's 60 records and records its committed
// offsets and lag per partition from kafka-consumer-groups.sh, the reference for the tools.
func (l *lagRun) freshGroup(ctx context.Context, tag string) error {
	l.group = name(l.env, tag)
	if _, err := l.env.consumeGroup(ctx, l.topic, l.group, 30); err != nil {
		return err
	}
	l.lag, l.commit, l.ends = map[int]int64{}, map[int]int64{}, map[int]int64{}
	for _, r := range l.env.describeGroup(ctx, l.group) {
		l.lag[r.Partition], l.commit[r.Partition], l.ends[r.Partition] = r.Lag, r.Current, r.LogEnd
	}
	if len(l.lag) != 3 {
		return fmt.Errorf("kafka-consumer-groups.sh shows %d partitions for group %s, expected 3", len(l.lag), l.group)
	}
	return nil
}

func lagScenario(ctx context.Context, env Env, s *Sections) {
	dir, err := workDir(env, "lag")
	if err != nil {
		s.stop("burrow", err)
		return
	}
	l := &lagRun{env: env, dir: dir, net: containerName(env, "lag", "net"),
		contName: func(role string) string { return containerName(env, "lag", role) }}
	rmNet, err := box.Network(ctx, l.net)
	if err != nil {
		s.stop("burrow", err)
		return
	}
	defer rmNet()
	defer l.cleanup()

	// The topic: 60 keyed records over 3 partitions. Groups are made per tool (freshGroup).
	l.topic = name(env, "lag")
	if err := env.createTopic(ctx, l.topic, 3); err != nil {
		s.stop("burrow", err)
		return
	}
	if err := env.produceKeyed(ctx, l.topic, keyedLines(0, 60, 6, "v")); err != nil {
		s.stop("burrow", err)
		return
	}
	client, err := box.Start(ctx, l.contName("http"), []string{"--network", l.net, "--entrypoint", "sleep"}, box.Image(), "3600")
	if err != nil {
		s.stop("burrow", err)
		return
	}
	l.client = client
	l.stops = append(l.stops, client.Remove)

	l.burrow(ctx, s)
	l.lagExporter(ctx, s)
	l.kafbat(ctx, s)
}

func (l *lagRun) burrow(ctx context.Context, s *Sections) {
	conf := fmt.Sprintf(`[general]
pidfile="/tmp/burrow.pid"
[logging]
level="info"
[client-profile.kcert]
client-id="burrow-kcert"
kafka-version="3.6.0"
[cluster.kcert]
class-name="kafka"
servers=[ %[1]q ]
client-profile="kcert"
topic-refresh=10
offset-refresh=5
[consumer.kcert]
class-name="kafka"
cluster="kcert"
servers=[ %[1]q ]
client-profile="kcert"
offsets-topic="__consumer_offsets"
start-latest=false
[httpserver.default]
address=":8000"
[evaluator.default]
class-name="caching"
expire-cache=10
[storage.default]
class-name="inmemory"
workers=20
intervals=15
expire-group=604800
min-distance=1
`, l.env.Bootstrap)
	// Burrow's consumer module gives up for good if __consumer_offsets is missing or still electing
	// leaders when it starts (seen on a fresh Kafka 4.3.1). A warm-up group makes Kafka create the
	// topic; Burrow starts once every partition of it has a leader. A broker that does not list the
	// topic (KubeMQ before kubemq-server#687) has nothing to wait for.
	if err := l.freshGroup(ctx, "lagw"); err != nil {
		s.fail("burrow", err)
		return
	}
	if l.env.listed(ctx, "kafka-topics.sh", "__consumer_offsets", "--list") {
		waitFor(ctx, 60*time.Second, 2*time.Second, func() bool {
			d := kout.ParseTopicDescribe(l.env.tool(ctx, "", "kafka-topics.sh", l.env.bs("--describe", "--topic", "__consumer_offsets")...).Stdout, "__consumer_offsets")
			return d.Found && d.AllLed()
		})
	}
	cdir := filepath.Join(l.dir, "conf", "burrow")
	if err := os.MkdirAll(cdir, 0o755); err != nil {
		s.fail("burrow", err)
		return
	}
	if err := os.WriteFile(filepath.Join(cdir, "burrow.toml"), []byte(conf), 0o644); err != nil {
		s.fail("burrow", err)
		return
	}
	if _, err := l.start(ctx, "burrow", os.Getenv("BURROW_IMAGE"), "-v", cdir+":/etc/burrow:ro"); err != nil {
		s.fail("burrow", err)
		return
	}
	base := "http://" + l.contName("burrow") + ":8000/v3/kafka"
	var clusters, groups []string
	var topicOffsets []int64
	o := Observation{}
	o["api_answers"] = waitFor(ctx, 90*time.Second, 3*time.Second, func() bool {
		var c struct {
			Clusters []string `json:"clusters"`
		}
		out, err := l.get(ctx, base)
		return err == nil && json.Unmarshal([]byte(out), &c) == nil && len(c.Clusters) > 0 && func() bool { clusters = c.Clusters; return true }()
	})
	// Burrow's cluster module is running once it reports the topic's end offsets (refreshed every
	// 10 s); only then is the group made, so it is read well inside the one-minute retention.
	topicSeen := func() bool {
		var t struct {
			Offsets []int64 `json:"offsets"`
		}
		out, err := l.get(ctx, base+"/kcert/topic/"+l.topic)
		if err == nil && json.Unmarshal([]byte(out), &t) == nil {
			topicOffsets = t.Offsets
		}
		return len(topicOffsets) == 3
	}
	waitFor(ctx, 90*time.Second, 3*time.Second, topicSeen)
	if err := l.freshGroup(ctx, "lagb"); err != nil {
		s.fail("burrow", err)
		return
	}
	waitFor(ctx, 45*time.Second, 3*time.Second, func() bool {
		var g struct {
			Consumers []string `json:"consumers"`
		}
		if out, err := l.get(ctx, base+"/kcert/consumer"); err == nil && json.Unmarshal([]byte(out), &g) == nil {
			groups = g.Consumers
		}
		return contains(groups, l.group)
	})
	topicSeen()
	ends := make([]string, len(topicOffsets))
	for i, v := range topicOffsets {
		ends[i] = fmt.Sprintf("%d=%d", i, v)
	}
	o["cluster_listed"] = contains(clusters, "kcert")
	o["topic_end_offsets_match"] = strings.Join(ends, " ") == kout.OffsetList(l.ends)
	o["group_listed"] = contains(groups, l.group)
	_, lagErr := l.get(ctx, base+"/kcert/consumer/"+l.group+"/lag")
	o["group_lag_available"] = lagErr == nil
	s.set("burrow", o)
}

var lagSeries = regexp.MustCompile(`(?m)^kafka_consumergroup_group_lag\{([^}]*)\} ([0-9.eE+-]+)$`)

// ExporterLag reads kafka_consumergroup_group_lag series for group and topic from a Prometheus
// text page: partition to lag.
func ExporterLag(page, group, topic string) map[int]int64 {
	out := map[int]int64{}
	for _, m := range lagSeries.FindAllStringSubmatch(page, -1) {
		labels := m[1]
		if !strings.Contains(labels, `group="`+group+`"`) || !strings.Contains(labels, `topic="`+topic+`"`) {
			continue
		}
		pm := regexp.MustCompile(`partition="(\d+)"`).FindStringSubmatch(labels)
		if pm == nil {
			continue
		}
		p, _ := strconv.Atoi(pm[1])
		v, err := strconv.ParseFloat(m[2], 64)
		if err != nil {
			continue
		}
		out[p] = int64(v)
	}
	return out
}

func (l *lagRun) lagExporter(ctx context.Context, s *Sections) {
	conf := fmt.Sprintf("kafka-lag-exporter {\n  port = 8000\n  poll-interval = 5 seconds\n  clusters = [ { name = \"kcert\", bootstrap-brokers = %q } ]\n}\n", l.env.Bootstrap)
	f := filepath.Join(l.dir, "conf", "kafka-lag-exporter.conf")
	if err := os.WriteFile(f, []byte(conf), 0o644); err != nil {
		s.fail("kafka-lag-exporter", err)
		return
	}
	// Admin-client calls at DEBUG, so a failed poll names the request that failed (evidence in the
	// run directory's raw logs).
	lb := filepath.Join(l.dir, "conf", "kafka-lag-exporter-logback.xml")
	if err := os.WriteFile(lb, []byte(`<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender"><encoder><pattern>%d %-5level %logger{36} - %msg%n</pattern></encoder></appender>
  <logger name="org.apache.kafka.clients.admin.KafkaAdminClient" level="DEBUG"/>
  <root level="INFO"><appender-ref ref="STDOUT"/></root>
</configuration>
`), 0o644); err != nil {
		s.fail("kafka-lag-exporter", err)
		return
	}
	// The image's own command names its install directory; the configuration is mounted elsewhere
	// and passed on the command line (the binary path is relative to the image's working directory).
	if _, err := l.startCmd(ctx, "lagexp", os.Getenv("KAFKA_LAG_EXPORTER_IMAGE"),
		[]string{"-v", f + ":/etc/kcert/application.conf:ro", "-v", lb + ":/etc/kcert/logback.xml:ro"},
		"bin/kafka-lag-exporter", "-Dconfig.file=/etc/kcert/application.conf", "-Dlogback.configurationFile=/etc/kcert/logback.xml"); err != nil {
		s.fail("kafka-lag-exporter", err)
		return
	}
	url := "http://" + l.contName("lagexp") + ":8000/metrics"
	answered := waitFor(ctx, 120*time.Second, 3*time.Second, func() bool {
		_, err := l.get(ctx, url)
		return err == nil
	})
	if err := l.freshGroup(ctx, "lage"); err != nil {
		s.fail("kafka-lag-exporter", err)
		return
	}
	var got map[int]int64
	exact := waitFor(ctx, 40*time.Second, 3*time.Second, func() bool {
		page, err := l.get(ctx, url)
		if err != nil {
			return false
		}
		got = ExporterLag(page, l.group, l.topic)
		return kout.OffsetList(got) == kout.OffsetList(l.lag)
	})
	s.set("kafka-lag-exporter", Observation{"metrics_page_answers": answered, "group_lag_series_present": len(got) > 0,
		"lag_equals_kafka_consumer_groups_per_partition": exact})
}

func (l *lagRun) kafbat(ctx context.Context, s *Sections) {
	if _, err := l.start(ctx, "kafbat", os.Getenv("KAFBAT_UI_IMAGE"),
		"-e", "KAFKA_CLUSTERS_0_NAME=kcert", "-e", "KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS="+l.env.Bootstrap); err != nil {
		s.fail("kafbat-ui", err)
		return
	}
	api := "http://" + l.contName("kafbat") + ":8080/api/clusters"
	o := Observation{}
	var clusters []struct {
		Name        string `json:"name"`
		Status      string `json:"status"`
		BrokerCount int    `json:"brokerCount"`
	}
	o["api_answers"] = waitFor(ctx, 120*time.Second, 3*time.Second, func() bool {
		out, err := l.get(ctx, api)
		return err == nil && json.Unmarshal([]byte(out), &clusters) == nil && len(clusters) == 1 && clusters[0].Status == "ONLINE"
	})
	if len(clusters) == 1 {
		o["cluster_status"], o["cluster_broker_count"] = clusters[0].Status, clusters[0].BrokerCount
	}
	if err := l.freshGroup(ctx, "lagk"); err != nil {
		s.fail("kafbat-ui", err)
		return
	}
	var brokers []struct {
		ID int `json:"id"`
	}
	if out, err := l.get(ctx, api+"/kcert/brokers"); err == nil {
		_ = json.Unmarshal([]byte(out), &brokers)
	}
	o["brokers_listed"] = len(brokers)
	var topics struct {
		Topics []struct {
			Name           string `json:"name"`
			PartitionCount int    `json:"partitionCount"`
			MessagesCount  int64  `json:"messagesCount"`
		} `json:"topics"`
	}
	if out, err := l.get(ctx, api+"/kcert/topics?search="+l.topic); err == nil {
		_ = json.Unmarshal([]byte(out), &topics)
	}
	o["topic_listed"], o["topic_partitions"], o["topic_messages_count"] = false, 0, int64(0)
	for _, t := range topics.Topics {
		if t.Name == l.topic {
			o["topic_listed"], o["topic_partitions"], o["topic_messages_count"] = true, t.PartitionCount, t.MessagesCount
		}
	}
	var groups struct {
		ConsumerGroups []struct {
			GroupID     string `json:"groupId"`
			State       string `json:"state"`
			ConsumerLag int64  `json:"consumerLag"`
		} `json:"consumerGroups"`
	}
	if out, err := l.get(ctx, api+"/kcert/consumer-groups/paged?search="+l.group); err == nil {
		_ = json.Unmarshal([]byte(out), &groups)
	}
	var want int64
	for _, v := range l.lag {
		want += v
	}
	o["group_listed"], o["group_state"], o["group_lag_equals_kafka_consumer_groups"] = false, "", false
	for _, g := range groups.ConsumerGroups {
		if g.GroupID == l.group {
			o["group_listed"], o["group_state"], o["group_lag_equals_kafka_consumer_groups"] = true, g.State, g.ConsumerLag == want
		}
	}
	s.set("kafbat-ui", o)
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
