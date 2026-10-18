package probe

// Kafka Connect distributed rows: connect-distributed.sh and the image's own connect-file jar
// (FileStreamSource and FileStreamSink), Apache Kafka 4.3.1, unmodified. Ported from
// kubemq-server tests/kafka-ecosystem/connect.sh onto the kit's brokers and engine abstraction.
//
// Phases (one matrix row each, in order):
//   rf-refusal        a worker asking for replication factor 3 on one broker is refused the way
//                     Kafka refuses it: Connect retries the refused creation, then gives up after
//                     its own 60 s with a creation timeout; no internal topic exists
//   internal-topics   Connect creates its config/offset/status topics: 1/25/5 partitions,
//                     cleanup.policy=compact, replication factor 1 (broker default)
//   file-roundtrip    FileStreamSource to topic to FileStreamSink reproduces 200 lines byte for
//                     byte; the topic holds exactly 200 records
//   second-worker     a second worker joins, work moves onto it, 100 more lines arrive exact
//   graceful-restart  worker A stopped with SIGTERM and started again: lines written while it is
//                     down and after it returns arrive with no loss and no duplicate
//   sigkill-sink      SIGKILL of the worker running the sink task once the pipeline is quiet:
//                     no loss, no reorder, no duplicate (offsets were committed)
//   final             the topic holds exactly one record per line; the sink group's committed
//                     offset equals the log end; workers loaded kafka-clients 4.3.1

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/box"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
)

func init() {
	for _, s := range []string{"rf-refusal", "internal-topics", "file-roundtrip", "second-worker", "graceful-restart", "sigkill-sink", "final"} {
		register("connect-"+s, sectionProbe("connect", connectScenario, s))
	}
}

const (
	connectLines      = 200
	connectPhaseLimit = 180 * time.Second
)

type connectRun struct {
	env       Env
	dir       string
	net       string
	group     string
	topic     string
	src, sink string
	in, out   string
	total     int
	workers   map[string]*box.Box
	stopLogs  []func()
	starts    map[string]int
}

func (c *connectRun) workerConf(name, group string, rf int) string {
	return fmt.Sprintf(`bootstrap.servers=%s
group.id=%s
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
config.storage.topic=%[2]s-configs
offset.storage.topic=%[2]s-offsets
status.storage.topic=%[2]s-status
config.storage.replication.factor=%[3]d
offset.storage.replication.factor=%[3]d
status.storage.replication.factor=%[3]d
offset.flush.interval.ms=1000
scheduled.rebalance.max.delay.ms=5000
listeners=http://0.0.0.0:8083
rest.advertised.host.name=%[4]s
rest.advertised.port=8083
plugin.path=/opt/kafka/libs/connect-file-%[5]s.jar
`, c.env.Bootstrap, group, rf, name, os.Getenv("KAFKA_VERSION"))
}

// startWorker starts a Connect worker container on the scenario's network and follows its log.
func (c *connectRun) startWorker(ctx context.Context, role, group string, rf int) (*box.Box, string, error) {
	name := containerName(c.env, "connect", role)
	if err := os.WriteFile(filepath.Join(c.dir, "conf", role+".properties"), []byte(c.workerConf(name, group, rf)), 0o644); err != nil {
		return nil, "", err
	}
	opts := append(box.JavaEnv(), "--network", c.net,
		"-v", filepath.Join(c.dir, "data")+":/data", "-v", filepath.Join(c.dir, "conf")+":/conf:ro",
		"-e", "KAFKA_HEAP_OPTS=-Xms256m -Xmx768m", "-e", "LOG_DIR=/tmp/connect-logs",
		"--entrypoint", "/opt/kafka/bin/connect-distributed.sh")
	b, err := box.Start(ctx, name, opts, box.Image(), "/conf/"+role+".properties")
	if err != nil {
		return nil, "", err
	}
	c.starts[role]++
	c.workers[role] = b
	logPath := filepath.Join(c.dir, "logs", fmt.Sprintf("%s-%d.log", role, c.starts[role]))
	stop, err := box.Follow(name, logPath)
	if err != nil {
		return nil, "", fmt.Errorf("follow the log of %s: %w", name, err)
	}
	c.stopLogs = append(c.stopLogs, stop)
	return b, logPath, nil
}

// rest calls a worker's REST API from inside the worker container. body "" is a GET.
func (c *connectRun) rest(ctx context.Context, w *box.Box, path, body string) (string, error) {
	script := "wget -qO- http://localhost:8083" + path
	if body != "" {
		script = "cat > /tmp/rest-body.json && wget -qO- --header 'Content-Type: application/json' --post-file=/tmp/rest-body.json http://localhost:8083" + path
	}
	r := w.Sh(ctx, body, 30*time.Second, script)
	if r.Exit != 0 {
		return r.Stdout, fmt.Errorf("REST %s on %s: %s", path, w.Name, strings.TrimSpace(r.Stderr+" "+r.Stdout))
	}
	return r.Stdout, nil
}

// waitREST waits for a worker's REST root and returns the Kafka version it reports.
func (c *connectRun) waitREST(ctx context.Context, w *box.Box) (string, error) {
	var v string
	ok := waitFor(ctx, 120*time.Second, time.Second, func() bool {
		out, err := c.rest(ctx, w, "/", "")
		if err != nil {
			return false
		}
		var root struct {
			Version string `json:"version"`
		}
		if json.Unmarshal([]byte(out), &root) == nil && root.Version != "" {
			v = root.Version
			return true
		}
		return false
	})
	if !ok {
		return "", fmt.Errorf("worker %s REST did not come up in 120 s", w.Name)
	}
	return v, nil
}

// live returns a running worker to send REST calls to (Connect forwards writes to its leader).
func (c *connectRun) live(ctx context.Context) *box.Box {
	for _, role := range []string{"wa", "wb"} {
		if w := c.workers[role]; w != nil && w.Running(ctx) {
			return w
		}
	}
	return nil
}

type connectorStatus struct {
	Connector struct {
		State    string `json:"state"`
		WorkerID string `json:"worker_id"`
	} `json:"connector"`
	Tasks []struct {
		State    string `json:"state"`
		WorkerID string `json:"worker_id"`
	} `json:"tasks"`
}

func (c *connectRun) status(ctx context.Context, connector string) (connectorStatus, error) {
	var s connectorStatus
	w := c.live(ctx)
	if w == nil {
		return s, fmt.Errorf("no live worker")
	}
	out, err := c.rest(ctx, w, "/connectors/"+connector+"/status", "")
	if err != nil {
		return s, err
	}
	return s, json.Unmarshal([]byte(out), &s)
}

func (c *connectRun) waitRunning(ctx context.Context, connector string) error {
	var last string
	if waitFor(ctx, 120*time.Second, time.Second, func() bool {
		s, err := c.status(ctx, connector)
		if err != nil || len(s.Tasks) == 0 {
			return false
		}
		last = s.Connector.State + "," + s.Tasks[0].State
		return s.Connector.State == "RUNNING" && s.Tasks[0].State == "RUNNING"
	}) {
		return nil
	}
	return fmt.Errorf("connector %s did not reach RUNNING with its task (last %q)", connector, last)
}

// append grows the source file with deterministic ASCII lines (FileStreamSource tracks its
// position in characters and seeks in bytes, so multi-byte input would re-read on restart; a
// known Kafka instrument trap).
func (c *connectRun) append(n int) error {
	f, err := os.OpenFile(c.in, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o666)
	if err != nil {
		return err
	}
	defer f.Close()
	for i := c.total + 1; i <= c.total+n; i++ {
		if _, err := fmt.Fprintf(f, "%s-%06d k=v;x=y payload-%d\n", c.group, i, i*7919%100003); err != nil {
			return err
		}
	}
	c.total += n
	return nil
}

func readLines(path string) []string {
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 {
		return nil
	}
	return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
}

// waitSink waits until the sink file holds every line written so far, then a little longer so a
// late duplicate would be seen.
func (c *connectRun) waitSink(ctx context.Context, phase string) error {
	if !waitFor(ctx, connectPhaseLimit, time.Second, func() bool { return len(readLines(c.out)) >= c.total }) {
		return fmt.Errorf("%s: the sink file holds %d of %d lines after %s", phase, len(readLines(c.out)), c.total, connectPhaseLimit)
	}
	time.Sleep(4 * time.Second)
	return nil
}

func (c *connectRun) stopWorker(ctx context.Context, role, signal string) error {
	w := c.workers[role]
	if w == nil {
		return fmt.Errorf("no worker %s", role)
	}
	if err := w.Kill(ctx, signal); err != nil {
		return err
	}
	if !w.Gone(ctx, 60*time.Second) {
		return fmt.Errorf("worker %s still exists 60 s after %s", w.Name, signal)
	}
	return nil
}

func (c *connectRun) cleanup() {
	for _, w := range c.workers {
		w.Remove()
	}
	for _, s := range c.stopLogs {
		s()
	}
}

func connectScenario(ctx context.Context, env Env, s *Sections) {
	dir, err := workDir(env, "connect")
	if err != nil {
		s.stop("rf-refusal", err)
		return
	}
	c := &connectRun{env: env, dir: dir, workers: map[string]*box.Box{}, starts: map[string]int{}}
	c.group = name(env, "conn")
	c.topic = c.group + "-data"
	c.src, c.sink = c.group+"-src", c.group+"-sink"
	c.in, c.out = filepath.Join(dir, "data", "in.txt"), filepath.Join(dir, "data", "out.txt")
	c.net = containerName(env, "connect", "net")
	rmNet, err := box.Network(ctx, c.net)
	if err != nil {
		s.stop("rf-refusal", err)
		return
	}
	defer rmNet()
	defer c.cleanup()

	for _, phase := range []struct {
		name string
		run  func(context.Context) (Observation, error)
	}{
		{"rf-refusal", c.rfRefusal},
		{"internal-topics", c.roundTripAndTopics(s)},
		{"second-worker", c.secondWorker},
		{"graceful-restart", c.gracefulRestart},
		{"sigkill-sink", c.sigkillSink},
		{"final", c.final},
	} {
		if phase.name == "internal-topics" {
			// roundTripAndTopics fills both internal-topics and file-roundtrip.
			if _, err := phase.run(ctx); err != nil {
				s.stop(firstMissing(s, "internal-topics", "file-roundtrip"), err)
				return
			}
			continue
		}
		o, err := phase.run(ctx)
		if err != nil {
			s.stop(phase.name, err)
			return
		}
		s.set(phase.name, o)
	}
}

func firstMissing(s *Sections, sections ...string) string {
	for _, x := range sections {
		if _, ok := s.obs[x]; !ok {
			return x
		}
	}
	return sections[len(sections)-1]
}

func (c *connectRun) rfRefusal(ctx context.Context) (Observation, error) {
	const rf = 3 // Kafka Connect's own default; one broker cannot meet it
	group := c.group + "-rfx"
	w, logPath, err := c.startWorker(ctx, "wr", group, rf)
	if err != nil {
		return nil, err
	}
	state := "timeout"
	switch waitLog(ctx, logPath, 150*time.Second, "Herder started", "Uncaught exception in herder work thread") {
	case "Herder started":
		state = "started"
	case "Uncaught exception in herder work thread":
		state = "gave-up"
	default:
		if !w.Running(ctx) {
			state = "exited"
		}
	}
	time.Sleep(2 * time.Second)
	if err := c.stopWorker(ctx, "wr", "KILL"); err != nil {
		return nil, fmt.Errorf("stop the refused worker: %w", err) // it stays registered for cleanup
	}
	delete(c.workers, "wr")
	retry := "'" + group + "-offsets' topic creation failed due to"
	b, _ := os.ReadFile(logPath)
	retries, namingRF := 0, 0
	for _, l := range strings.Split(string(b), "\n") {
		if strings.Contains(l, retry) {
			retries++
			if strings.Contains(strings.ToLower(l), "replication factor") {
				namingRF++
			}
		}
	}
	created := 0
	for _, t := range []string{"-configs", "-offsets", "-status"} {
		if c.env.listed(ctx, "kafka-topics.sh", group+t, "--list") {
			created++
		}
	}
	return Observation{
		"worker_outcome":                      state,
		"refused_creation_retried_at_least_2": retries >= 2,
		"every_refusal_names_replication":     retries > 0 && namingRF == retries,
		"gave_up_with_creation_timeout":       strings.Contains(string(b), "Timeout expired while trying to create topic"),
		"internal_topics_created":             created,
	}, nil
}

// roundTripAndTopics starts worker A, runs the first file round trip and records the internal
// topics Connect created.
func (c *connectRun) roundTripAndTopics(s *Sections) func(context.Context) (Observation, error) {
	return func(ctx context.Context) (Observation, error) {
		if err := c.env.createTopic(ctx, c.topic, 1); err != nil {
			return nil, err
		}
		if err := os.WriteFile(c.out, nil, 0o666); err != nil {
			return nil, err
		}
		_ = os.Chmod(c.out, 0o666)
		if err := c.append(connectLines); err != nil {
			return nil, err
		}
		wa, _, err := c.startWorker(ctx, "wa", c.group, -1)
		if err != nil {
			return nil, err
		}
		version, err := c.waitREST(ctx, wa)
		if err != nil {
			return nil, err
		}
		for _, cn := range []struct{ name, class, key string }{
			{c.src, "org.apache.kafka.connect.file.FileStreamSourceConnector", "topic"},
			{c.sink, "org.apache.kafka.connect.file.FileStreamSinkConnector", "topics"},
		} {
			file := "/data/in.txt"
			if cn.key == "topics" {
				file = "/data/out.txt"
			}
			body, _ := json.Marshal(map[string]any{"name": cn.name, "config": map[string]string{
				"connector.class": cn.class, "tasks.max": "1", "file": file, cn.key: c.topic}})
			if _, err := c.rest(ctx, wa, "/connectors", string(body)); err != nil {
				return nil, err
			}
		}
		for _, cn := range []string{c.src, c.sink} {
			if err := c.waitRunning(ctx, cn); err != nil {
				return nil, err
			}
		}
		topics := Observation{"worker_rest_version": version}
		for _, t := range []string{"configs", "offsets", "status"} {
			name := c.group + "-" + t
			d := kout.ParseTopicDescribe(c.env.tool(ctx, "", "kafka-topics.sh", c.env.bs("--describe", "--topic", name)...).Stdout, name)
			all := kout.ParseConfigsDescribe(c.env.tool(ctx, "", "kafka-configs.sh", c.env.bs("--describe", "--all", "--entity-type", "topics", "--entity-name", name)...).Stdout)
			topics[t+"_exists"] = d.Found
			topics[t+"_partitions"] = d.PartitionCount
			topics[t+"_replication_factor"] = d.RF
			topics[t+"_cleanup_policy"] = all["cleanup.policy"]
		}
		s.set("internal-topics", topics)
		if err := c.waitSink(ctx, "file round trip"); err != nil {
			return nil, err
		}
		rt := Observation{}
		put(rt, "sink", AnalyzeLines(readLines(c.in), readLines(c.out)))
		rt["topic_records_equal_lines"] = sumOffsets(kout.ParseGetOffsets(c.env.tool(ctx, "", "kafka-get-offsets.sh", c.env.bs("--topic", c.topic, "--time", "-1")...).Stdout, c.topic)) == int64(c.total)
		s.set("file-roundtrip", rt)
		return nil, nil
	}
}

func (c *connectRun) secondWorker(ctx context.Context) (Observation, error) {
	wb, _, err := c.startWorker(ctx, "wb", c.group, -1)
	if err != nil {
		return nil, err
	}
	if _, err := c.waitREST(ctx, wb); err != nil {
		return nil, err
	}
	moved := waitFor(ctx, 90*time.Second, time.Second, func() bool {
		for _, cn := range []string{c.src, c.sink} {
			st, err := c.status(ctx, cn)
			if err != nil {
				continue
			}
			for _, t := range st.Tasks {
				if strings.HasPrefix(t.WorkerID, wb.Name+":") {
					return true
				}
			}
			if strings.HasPrefix(st.Connector.WorkerID, wb.Name+":") {
				return true
			}
		}
		return false
	})
	if err := c.append(100); err != nil {
		return nil, err
	}
	if err := c.waitSink(ctx, "second worker"); err != nil {
		return nil, err
	}
	o := Observation{"work_moved_to_second_worker": moved}
	put(o, "sink", AnalyzeLines(readLines(c.in), readLines(c.out)))
	return o, nil
}

func (c *connectRun) gracefulRestart(ctx context.Context) (Observation, error) {
	if err := c.stopWorker(ctx, "wa", "TERM"); err != nil {
		return nil, err
	}
	if err := c.append(50); err != nil {
		return nil, err
	}
	if err := c.waitSink(ctx, "graceful restart (worker A down)"); err != nil {
		return nil, err
	}
	wa, _, err := c.startWorker(ctx, "wa", c.group, -1)
	if err != nil {
		return nil, err
	}
	if _, err := c.waitREST(ctx, wa); err != nil {
		return nil, err
	}
	if err := c.append(50); err != nil {
		return nil, err
	}
	if err := c.waitSink(ctx, "graceful restart (worker A back)"); err != nil {
		return nil, err
	}
	o := Observation{}
	put(o, "sink", AnalyzeLines(readLines(c.in), readLines(c.out)))
	return o, nil
}

func (c *connectRun) sigkillSink(ctx context.Context) (Observation, error) {
	st, err := c.status(ctx, c.sink)
	if err != nil || len(st.Tasks) == 0 {
		return nil, fmt.Errorf("cannot tell which worker runs the sink task: %v", err)
	}
	victim := ""
	for role, w := range c.workers {
		if strings.HasPrefix(st.Tasks[0].WorkerID, w.Name+":") {
			victim = role
		}
	}
	if victim == "" {
		return nil, fmt.Errorf("sink task runs on %q, not a worker of this scenario", st.Tasks[0].WorkerID)
	}
	time.Sleep(3 * time.Second) // pipeline idle, offsets flushed (offset.flush.interval.ms=1000)
	if err := c.stopWorker(ctx, victim, "KILL"); err != nil {
		return nil, err
	}
	if err := c.append(50); err != nil {
		return nil, err
	}
	if err := c.waitSink(ctx, "SIGKILL (after the kill)"); err != nil {
		return nil, err
	}
	w, _, err := c.startWorker(ctx, victim, c.group, -1)
	if err != nil {
		return nil, err
	}
	if _, err := c.waitREST(ctx, w); err != nil {
		return nil, err
	}
	if err := c.append(50); err != nil {
		return nil, err
	}
	if err := c.waitSink(ctx, "SIGKILL (victim back)"); err != nil {
		return nil, err
	}
	o := Observation{}
	put(o, "sink", AnalyzeLines(readLines(c.in), readLines(c.out)))
	return o, nil
}

func (c *connectRun) final(ctx context.Context) (Observation, error) {
	o := Observation{}
	o["topic_records_equal_lines"] = sumOffsets(kout.ParseGetOffsets(c.env.tool(ctx, "", "kafka-get-offsets.sh", c.env.bs("--topic", c.topic, "--time", "-1")...).Stdout, c.topic)) == int64(c.total)
	group := "connect-" + c.sink
	o["sink_group_committed_at_end"] = waitFor(ctx, 60*time.Second, 2*time.Second, func() bool {
		rows := c.env.describeGroup(ctx, group)
		if len(rows) == 0 {
			return false
		}
		for _, r := range rows {
			if r.Current != r.LogEnd {
				return false
			}
		}
		return true
	})
	versions := map[string]bool{}
	for role := range c.starts {
		for i := 1; i <= c.starts[role]; i++ {
			if role == "wr" {
				continue
			}
			for _, v := range strings.Split(clientVersionsInLog(filepath.Join(c.dir, "logs", fmt.Sprintf("%s-%d.log", role, i))), ",") {
				if v != "" {
					versions[v] = true
				}
			}
		}
	}
	o["workers_loaded_kafka_clients"] = strings.Join(sortedKeys(versions), ",")
	return o, nil
}
