package probe

// Kafka Streams rows: the WordCountDemo shipped in the Apache Kafka 4.3.1 image
// (kafka-streams-examples), unmodified, with processing.guarantee=exactly_once_v2. Ported from
// kubemq-server tests/kafka-ecosystem/streams.sh onto the kit's brokers and engine abstraction.
//
// Phases (one matrix row each, in order):
//   topology-topics   Streams creates its repartition topic (delete policy) and changelog topic
//                     (compact policy) with the input's 3 partitions, replication factor 1
//   word-counts       300 lines: every word's committed updates (read_committed) are exactly
//                     1, 2, ..., c (the demo turns its record cache off, so each input word emits
//                     one update): no duplicate, no gap, exact final counts
//   exactly-once      4000 lines stream in over about 20 s; the app is SIGKILLed once its group
//                     has committed part of them (the kill must land mid-flight); a fresh
//                     instance with no local state restores from the changelog and finishes:
//                     every word's committed updates are still exactly 1..c
//   continued-counting 300 more lines on the fresh instance continue from the restored counts
//
// Two instrument facts kept from the kubemq-server legs: the apache/kafka image is Alpine-based
// without libstdc++, so RocksDB cannot load and the app uses the in-memory store suppliers (the
// stores are still backed by and restored from the changelog); and an unkeyed console producer
// sends every line to one partition, so input lines are keyed to spread them.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/box"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
)

func init() {
	for _, s := range []string{"topology-topics", "word-counts", "exactly-once", "continued-counting"} {
		register("streams-"+s, sectionProbe("streams", streamsScenario, s))
	}
}

const (
	streamsIn      = "streams-plaintext-input"  // fixed by WordCountDemo
	streamsOut     = "streams-wordcount-output" // fixed by WordCountDemo
	streamsParts   = 3
	streamsLines1  = 300
	streamsLines2  = 4000
	streamsLines3  = 300
	streamsTimeout = 300 * time.Second
)

// WordCountCheck summarizes committed WordCount updates against the expected counts. Ported from
// the kubemq-server legs' eco_verify_wordcounts.
type WordCountCheck struct {
	Words              int `json:"words"`
	WrongFinal         int `json:"wrong_final"`         // a word's last committed count differs from expected
	SequenceViolations int `json:"sequence_violations"` // a word's update is not previous+1 (duplicate or loss)
	UnexpectedWords    int `json:"unexpected_words"`
}

// WordUpdate is one committed record of the output topic: word and its count.
type WordUpdate struct {
	Word  string
	Count int64
}

// AnalyzeWordCounts checks updates (in partition order) against expected final counts.
func AnalyzeWordCounts(expected map[string]int64, updates []WordUpdate) WordCountCheck {
	c := WordCountCheck{Words: len(expected)}
	last := map[string]int64{}
	unexpected := map[string]bool{}
	for _, u := range updates {
		if _, ok := expected[u.Word]; !ok {
			unexpected[u.Word] = true
			continue
		}
		if u.Count != last[u.Word]+1 {
			c.SequenceViolations++
		}
		if u.Count > last[u.Word] {
			last[u.Word] = u.Count
		}
	}
	for w, n := range expected {
		if last[w] != n {
			c.WrongFinal++
		}
	}
	c.UnexpectedWords = len(unexpected)
	return c
}

// streamsWords generates line i's six words, deterministic and lower-case (the demo lower-cases
// and splits on non-word characters).
func streamsWords(i int) string {
	parts := make([]string, 6)
	for k := 0; k < 6; k++ {
		parts[k] = fmt.Sprintf("word%02d", (i*31+k*17+(i%7)*k)%40)
	}
	return strings.Join(parts, " ")
}

type streamsRun struct {
	env      Env
	dir      string
	appID    string
	lines    []string
	starts   int
	app      *box.Box
	stopLogs []func()
}

func (r *streamsRun) conf() string {
	return fmt.Sprintf(`application.id=%s
bootstrap.servers=%s
processing.guarantee=exactly_once_v2
replication.factor=-1
num.stream.threads=2
state.dir=/tmp/kafka-streams
commit.interval.ms=100
consumer.session.timeout.ms=10000
dsl.store.suppliers.class=org.apache.kafka.streams.state.BuiltInDslStoreSuppliers$InMemoryDslStoreSuppliers
`, r.appID, r.env.Bootstrap)
}

const streamsLog4j = `Configuration:
  Appenders:
    Console:
      name: STDOUT
      PatternLayout:
        pattern: "[%d] %p %m (%c)%n"
  Loggers:
    Root:
      level: WARN
      AppenderRef:
        - ref: STDOUT
    Logger:
      - name: org.apache.kafka.common.utils
        level: INFO
      - name: org.apache.kafka.streams.KafkaStreams
        level: INFO
`

// startApp starts a WordCountDemo instance and waits until it reports RUNNING.
func (r *streamsRun) startApp(ctx context.Context) (string, error) {
	r.starts++
	name := containerName(r.env, "streams", fmt.Sprintf("app%d", r.starts))
	opts := append(box.JavaEnv(), "-v", filepath.Join(r.dir, "conf")+":/conf:ro",
		"-e", "KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=/conf/streams-log4j2.yaml",
		"-e", "KAFKA_HEAP_OPTS=-Xms256m -Xmx768m",
		"--entrypoint", "/opt/kafka/bin/kafka-run-class.sh")
	b, err := box.Start(ctx, name, opts, box.Image(), "org.apache.kafka.streams.examples.wordcount.WordCountDemo", "/conf/streams.properties")
	if err != nil {
		return "", err
	}
	r.app = b
	logPath := filepath.Join(r.dir, "logs", fmt.Sprintf("app%d.log", r.starts))
	stop, err := box.Follow(name, logPath)
	if err != nil {
		return logPath, fmt.Errorf("follow the log of %s: %w", name, err)
	}
	r.stopLogs = append(r.stopLogs, stop)
	switch waitLog(ctx, logPath, streamsTimeout, "State transition from REBALANCING to RUNNING", "to ERROR", "to PENDING_ERROR") {
	case "State transition from REBALANCING to RUNNING":
		return logPath, nil
	case "":
		return logPath, fmt.Errorf("app %s did not reach RUNNING in %s", name, streamsTimeout)
	default:
		return logPath, fmt.Errorf("app %s went to ERROR", name)
	}
}

// keyed returns console-producer input for lines [from, to): "line<n>\t<words>".
func (r *streamsRun) keyed(from, to int) string {
	var b strings.Builder
	for i := from; i < to; i++ {
		w := streamsWords(i)
		r.lines = append(r.lines, w)
		fmt.Fprintf(&b, "line%07d\t%s\n", i, w)
	}
	return b.String()
}

func (r *streamsRun) produce(ctx context.Context, input string) error {
	res := r.env.Box.Tool(ctx, input, 5*time.Minute, "kafka-console-producer.sh",
		r.env.bs("--topic", streamsIn, "--reader-property", "parse.key=true", "--producer-property", "acks=all")...)
	if res.Exit != 0 {
		return fmt.Errorf("producing to %s: %s", streamsIn, firstErrorLine(res.Combined()))
	}
	return nil
}

// feed produces input in chunks of 50 lines with a pause between chunks, in the background; the
// returned channel yields the producer's result.
func (r *streamsRun) feed(ctx context.Context, input string) <-chan error {
	done := make(chan error, 1)
	pr, pw := io.Pipe()
	go func() {
		sc := bufio.NewScanner(strings.NewReader(input))
		n := 0
		for sc.Scan() {
			if _, err := io.WriteString(pw, sc.Text()+"\n"); err != nil {
				break
			}
			n++
			if n%50 == 0 {
				time.Sleep(250 * time.Millisecond)
			}
		}
		pw.Close()
	}()
	go func() {
		res := r.env.Box.ToolStream(ctx, pr, 5*time.Minute, "kafka-console-producer.sh",
			r.env.bs("--topic", streamsIn, "--reader-property", "parse.key=true", "--producer-property", "acks=all")...)
		pr.CloseWithError(io.ErrClosedPipe) // a producer that exits early must not leave the writer blocked
		if res.Exit != 0 {
			done <- fmt.Errorf("feeding %s: %s", streamsIn, firstErrorLine(res.Combined()))
			return
		}
		done <- nil
	}()
	return done
}

// committed is the application group's committed position on the input topic.
func (r *streamsRun) committed(ctx context.Context) int64 {
	var s int64
	for _, row := range r.env.describeGroup(ctx, r.appID) {
		if row.Topic == streamsIn && row.Current > 0 {
			s += row.Current
		}
	}
	return s
}

func (r *streamsRun) waitConsumed(ctx context.Context, phase string) error {
	want := int64(len(r.lines))
	if !waitFor(ctx, streamsTimeout, 3*time.Second, func() bool { return r.committed(ctx) == want }) {
		return fmt.Errorf("%s: group %s committed %d of %d input lines after %s", phase, r.appID, r.committed(ctx), want, streamsTimeout)
	}
	return nil
}

// drain reads the whole output topic with read_committed: word and count per record.
func (r *streamsRun) drain(ctx context.Context, isolation string) ([]WordUpdate, error) {
	n := sumOffsets(kout.ParseGetOffsets(r.env.tool(ctx, "", "kafka-get-offsets.sh", r.env.bs("--topic", streamsOut, "--time", "-1")...).Stdout, streamsOut))
	res := r.env.Box.Tool(ctx, "", 3*time.Minute, "kafka-console-consumer.sh", r.env.bs("--topic", streamsOut, "--from-beginning",
		"--timeout-ms", "15000", "--isolation-level", isolation,
		"--formatter-property", "print.key=true",
		"--key-deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
		"--value-deserializer", "org.apache.kafka.common.serialization.LongDeserializer")...)
	var ups []WordUpdate
	for _, l := range strings.Split(res.Stdout, "\n") {
		w, v, ok := strings.Cut(l, "\t")
		if !ok {
			continue
		}
		c, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			continue
		}
		ups = append(ups, WordUpdate{Word: w, Count: c})
	}
	if total := kout.ProcessedTotal(res.Stderr); total < 0 || total != len(ups) {
		return ups, fmt.Errorf("draining %s: consumer reported %d records, parsed %d (output end offsets total %d)", streamsOut, total, len(ups), n)
	}
	return ups, nil
}

func (r *streamsRun) expected() map[string]int64 {
	m := map[string]int64{}
	for _, l := range r.lines {
		for _, w := range strings.Fields(l) {
			m[w]++
		}
	}
	return m
}

// verify drains until the final counts are reached (or six tries), then judges what is there.
func (r *streamsRun) verify(ctx context.Context) (WordCountCheck, error) {
	exp := r.expected()
	var ups []WordUpdate
	var err error
	for i := 0; i < 6; i++ {
		ups, err = r.drain(ctx, "read_committed")
		if err == nil && AnalyzeWordCounts(exp, ups).WrongFinal == 0 {
			break
		}
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		return WordCountCheck{}, err
	}
	return AnalyzeWordCounts(exp, ups), nil
}

func (r *streamsRun) cleanup() {
	if r.app != nil {
		r.app.Remove()
	}
	for _, s := range r.stopLogs {
		s()
	}
}

func streamsScenario(ctx context.Context, env Env, s *Sections) {
	dir, err := workDir(env, "streams")
	if err != nil {
		s.stop("topology-topics", err)
		return
	}
	r := &streamsRun{env: env, dir: dir, appID: name(env, "wc")}
	defer r.cleanup()
	for f, body := range map[string]string{"streams.properties": r.conf(), "streams-log4j2.yaml": streamsLog4j} {
		if err := os.WriteFile(filepath.Join(dir, "conf", f), []byte(body), 0o644); err != nil {
			s.stop("topology-topics", err)
			return
		}
	}
	// The demo's topic names are fixed: remove leftovers from an earlier run on this broker.
	for _, t := range []string{streamsIn, streamsOut} {
		if env.listed(ctx, "kafka-topics.sh", t, "--list") {
			env.tool(ctx, "", "kafka-topics.sh", env.bs("--delete", "--topic", t)...)
			waitFor(ctx, 60*time.Second, 2*time.Second, func() bool { return !env.listed(ctx, "kafka-topics.sh", t, "--list") })
		}
		if err := env.createTopic(ctx, t, streamsParts); err != nil {
			s.stop("topology-topics", err)
			return
		}
	}

	// Phase 1: first instance, 300 lines, topics and exact counts.
	if _, err := r.startApp(ctx); err != nil {
		s.stop("topology-topics", err)
		return
	}
	if err := r.produce(ctx, r.keyed(1, 1+streamsLines1)); err != nil {
		s.stop("topology-topics", err)
		return
	}
	if err := r.waitConsumed(ctx, "first batch"); err != nil {
		s.stop("topology-topics", err)
		return
	}
	topo := Observation{}
	var repart, changelog string
	for _, t := range kout.Lines(env.tool(ctx, "", "kafka-topics.sh", env.bs("--list")...).Stdout) {
		if strings.HasPrefix(t, r.appID+"-") && strings.HasSuffix(t, "-repartition") {
			repart = t
		}
		if strings.HasPrefix(t, r.appID+"-") && strings.HasSuffix(t, "-changelog") {
			changelog = t
		}
	}
	for key, t := range map[string]string{"repartition": repart, "changelog": changelog} {
		topo[key+"_exists"] = t != ""
		if t == "" {
			continue
		}
		d := kout.ParseTopicDescribe(env.tool(ctx, "", "kafka-topics.sh", env.bs("--describe", "--topic", t)...).Stdout, t)
		all := kout.ParseConfigsDescribe(env.tool(ctx, "", "kafka-configs.sh", env.bs("--describe", "--all", "--entity-type", "topics", "--entity-name", t)...).Stdout)
		topo[key+"_partitions"], topo[key+"_replication_factor"], topo[key+"_cleanup_policy"] = d.PartitionCount, d.RF, all["cleanup.policy"]
	}
	s.set("topology-topics", topo)
	wc, err := r.verify(ctx)
	if err != nil {
		s.stop("word-counts", err)
		return
	}
	o := Observation{}
	put(o, "counts", wc)
	s.set("word-counts", o)

	// Phase 2: SIGKILL mid-flight, restore in a fresh instance.
	total12 := streamsLines1 + streamsLines2
	fed := r.feed(ctx, r.keyed(1+streamsLines1, 1+total12))
	var feedErr error
	feedDone := false
	waitFor(ctx, streamsTimeout, time.Second, func() bool {
		if !feedDone {
			select {
			case feedErr = <-fed:
				feedDone = true
			default:
			}
		}
		return r.committed(ctx) > streamsLines1 || feedDone
	})
	if err := r.app.Kill(ctx, "KILL"); err != nil {
		s.stop("exactly-once", err)
		return
	}
	r.app.Gone(ctx, 60*time.Second)
	time.Sleep(2 * time.Second)
	var atKill int64
	for i := 0; i < 5 && atKill == 0; i++ { // a transient describe failure reads as 0; never judge on it
		atKill = r.committed(ctx)
		if atKill == 0 {
			time.Sleep(2 * time.Second)
		}
	}
	if !feedDone {
		feedErr = <-fed
	}
	if feedErr != nil {
		s.stop("exactly-once", feedErr)
		return
	}
	if atKill <= streamsLines1 || atKill >= int64(total12) {
		s.stop("exactly-once", fmt.Errorf("the kill did not land mid-flight: the first instance had committed %d input lines, not strictly between %d and %d, so a restore after a kill was not exercised", atKill, streamsLines1, total12))
		return
	}
	if _, err := r.startApp(ctx); err != nil {
		s.stop("exactly-once", err)
		return
	}
	if err := r.waitConsumed(ctx, "after the kill"); err != nil {
		s.stop("exactly-once", err)
		return
	}
	wc, err = r.verify(ctx)
	if err != nil {
		s.stop("exactly-once", err)
		return
	}
	o = Observation{"kill_landed_mid_flight": true}
	put(o, "counts", wc)
	s.set("exactly-once", o)

	// Phase 3: more lines on the fresh instance continue from the restored counts.
	if err := r.produce(ctx, r.keyed(1+total12, 1+total12+streamsLines3)); err != nil {
		s.stop("continued-counting", err)
		return
	}
	if err := r.waitConsumed(ctx, "third batch"); err != nil {
		s.stop("continued-counting", err)
		return
	}
	wc, err = r.verify(ctx)
	if err != nil {
		s.stop("continued-counting", err)
		return
	}
	o = Observation{}
	put(o, "counts", wc)
	versions := map[string]bool{}
	for i := 1; i <= r.starts; i++ {
		for _, v := range strings.Split(clientVersionsInLog(filepath.Join(dir, "logs", fmt.Sprintf("app%d.log", i))), ",") {
			if v != "" {
				versions[v] = true
			}
		}
	}
	o["apps_loaded_kafka_clients"] = strings.Join(sortedKeys(versions), ",")
	s.set("continued-counting", o)
}
