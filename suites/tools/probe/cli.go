package probe

// Kafka CLI rows: Apache Kafka 4.3.1's own command-line tools, run unmodified from the pinned
// image. Scenario steps follow the Kafka CLI admin-tools sweep of kubemq-server
// tests/kafka-conformance (conformance_admin_tools_test.go), extended for the tools track.

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/box"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
)

func init() {
	register("cli-topics-lifecycle", cliTopicsLifecycle)
	register("cli-console-roundtrip", cliConsoleRoundTrip)
	register("cli-groups-describe", cliGroupsDescribe)
	register("cli-groups-reset", cliGroupsReset)
	register("cli-groups-active", cliGroupsActive)
	register("cli-groups-delete", cliGroupsDelete)
	register("cli-configs-topic", cliConfigsTopic)
	register("cli-get-offsets", cliGetOffsets)
	register("cli-delete-records", cliDeleteRecords)
	register("cli-verifiable", cliVerifiable)
	register("cli-perf-test", cliPerfTest)
	register("cli-broker-api-versions", cliBrokerAPIVersions)
}

// createTopic creates a topic with the CLI and waits until describe shows every partition led.
func (e Env) createTopic(ctx context.Context, topic string, partitions int, extra ...string) error {
	args := e.bs(append([]string{"--create", "--topic", topic, "--partitions", strconv.Itoa(partitions), "--replication-factor", "1"}, extra...)...)
	if _, err := e.mustTool(ctx, "create topic "+topic, "", "kafka-topics.sh", args...); err != nil {
		return err
	}
	if !waitFor(ctx, 30*time.Second, time.Second, func() bool {
		d := kout.ParseTopicDescribe(e.tool(ctx, "", "kafka-topics.sh", e.bs("--describe", "--topic", topic)...).Stdout, topic)
		return d.Found && d.PartitionCount == partitions && d.AllLed()
	}) {
		return fmt.Errorf("topic %s never showed %d led partitions", topic, partitions)
	}
	return nil
}

// produceKeyed sends "key<TAB>value" lines with the console producer.
func (e Env) produceKeyed(ctx context.Context, topic string, lines []string) error {
	_, err := e.mustTool(ctx, "console producer to "+topic, strings.Join(lines, "\n")+"\n", "kafka-console-producer.sh",
		e.bs("--topic", topic, "--reader-property", "parse.key=true", "--producer-property", "acks=all")...)
	return err
}

func keyedLines(from, to, keys int, tag string) []string {
	var out []string
	for i := from; i < to; i++ {
		out = append(out, fmt.Sprintf("k%d\t%s-%04d", i%keys, tag, i))
	}
	return out
}

// consumeGroup reads n records of topic from the beginning with the console consumer in group g;
// the consumer commits on close. Returns the tool's own processed count.
func (e Env) consumeGroup(ctx context.Context, topic, group string, n int) (int, error) {
	r, err := e.mustTool(ctx, "console consumer group "+group, "", "kafka-console-consumer.sh",
		e.bs("--topic", topic, "--group", group, "--from-beginning", "--max-messages", strconv.Itoa(n), "--timeout-ms", "30000")...)
	if err != nil {
		return -1, err
	}
	return kout.ProcessedTotal(r.Stderr), nil
}

func (e Env) describeGroup(ctx context.Context, group string) []kout.GroupOffsetRow {
	return kout.ParseGroupDescribe(e.tool(ctx, "", "kafka-consumer-groups.sh", e.bs("--describe", "--group", group)...).Stdout, group)
}

func committedSum(rows []kout.GroupOffsetRow) int64 {
	var s int64
	for _, r := range rows {
		if r.Current > 0 {
			s += r.Current
		}
	}
	return s
}

func committedList(rows []kout.GroupOffsetRow) string {
	m := map[int]int64{}
	for _, r := range rows {
		m[r.Partition] = r.Current
	}
	return kout.OffsetList(m)
}

func (e Env) listed(ctx context.Context, tool, item string, args ...string) bool {
	return slices.Contains(kout.Lines(e.tool(ctx, "", tool, e.bs(args...)...).Stdout), item)
}

// --- topics ----------------------------------------------------------------------------------

func cliTopicsLifecycle(ctx context.Context, env Env) (Observation, error) {
	t := name(env, "topics")
	obs := Observation{}
	create := env.tool(ctx, "", "kafka-topics.sh", env.bs("--create", "--topic", t, "--partitions", "3", "--replication-factor", "1", "--config", "retention.ms=86400000")...)
	obs["create_exit"] = create.Exit
	if create.Exit != 0 {
		return obs, fmt.Errorf("create: %s", firstErrorLine(create.Combined()))
	}
	var d kout.TopicDescription
	waitFor(ctx, 30*time.Second, time.Second, func() bool {
		d = kout.ParseTopicDescribe(env.tool(ctx, "", "kafka-topics.sh", env.bs("--describe", "--topic", t)...).Stdout, t)
		return d.Found && d.AllLed()
	})
	obs["describe_partitions"] = d.PartitionCount
	obs["describe_replication_factor"] = d.RF
	obs["describe_all_partitions_led"] = d.AllLed()
	obs["describe_shows_retention_ms"] = d.Configs["retention.ms"]
	obs["listed_after_create"] = env.listed(ctx, "kafka-topics.sh", t, "--list")

	dup := env.tool(ctx, "", "kafka-topics.sh", env.bs("--create", "--topic", t, "--partitions", "3", "--replication-factor", "1")...)
	obs["create_duplicate_exit"], obs["create_duplicate_exception"] = dup.Exit, box.Exception(dup)

	grow := env.tool(ctx, "", "kafka-topics.sh", env.bs("--alter", "--topic", t, "--partitions", "5")...)
	obs["alter_increase_exit"] = grow.Exit
	waitFor(ctx, 30*time.Second, time.Second, func() bool {
		d = kout.ParseTopicDescribe(env.tool(ctx, "", "kafka-topics.sh", env.bs("--describe", "--topic", t)...).Stdout, t)
		return d.PartitionCount == 5 && d.AllLed()
	})
	obs["partitions_after_increase"] = d.PartitionCount
	obs["all_led_after_increase"] = d.AllLed()

	shrink := env.tool(ctx, "", "kafka-topics.sh", env.bs("--alter", "--topic", t, "--partitions", "2")...)
	obs["alter_decrease_exit"], obs["alter_decrease_exception"] = shrink.Exit, box.Exception(shrink)

	missing := env.tool(ctx, "", "kafka-topics.sh", env.bs("--describe", "--topic", name(env, "never"))...)
	obs["describe_missing_exit"], obs["describe_missing_exception"] = missing.Exit, box.Exception(missing)

	del := env.tool(ctx, "", "kafka-topics.sh", env.bs("--delete", "--topic", t)...)
	obs["delete_exit"] = del.Exit
	obs["gone_from_list_after_delete"] = waitFor(ctx, 30*time.Second, time.Second, func() bool {
		return !env.listed(ctx, "kafka-topics.sh", t, "--list")
	})
	return obs, nil
}

// --- console producer and consumer -----------------------------------------------------------

func cliConsoleRoundTrip(ctx context.Context, env Env) (Observation, error) {
	t := name(env, "console")
	if err := env.createTopic(ctx, t, 3); err != nil {
		return nil, err
	}
	const n = 120
	var sent []ConsoleSent
	var in strings.Builder
	for i := 0; i < n; i++ {
		s := ConsoleSent{Key: fmt.Sprintf("k%d", i%12), Value: fmt.Sprintf("v-%04d", i), Headers: fmt.Sprintf("seq:%d,src:cli", i)}
		sent = append(sent, s)
		fmt.Fprintf(&in, "%s\t%s\t%s\n", s.Headers, s.Key, s.Value)
	}
	prod := env.tool(ctx, in.String(), "kafka-console-producer.sh", env.bs("--topic", t,
		"--reader-property", "parse.key=true", "--reader-property", "parse.headers=true", "--producer-property", "acks=all")...)
	obs := Observation{"producer_exit": prod.Exit}
	if prod.Exit != 0 {
		return obs, fmt.Errorf("console producer: %s", firstErrorLine(prod.Combined()))
	}
	cons := env.tool(ctx, "", "kafka-console-consumer.sh", env.bs("--topic", t, "--from-beginning",
		"--max-messages", strconv.Itoa(n), "--timeout-ms", "30000",
		"--formatter-property", "print.partition=true", "--formatter-property", "print.offset=true",
		"--formatter-property", "print.headers=true", "--formatter-property", "print.key=true")...)
	recs, malformed := kout.ParseConsoleRecords(cons.Stdout)
	obs["consumer_processed_total"] = kout.ProcessedTotal(cons.Stderr)
	obs["consumer_malformed_lines"] = malformed
	put(obs, "roundtrip", AnalyzeConsole(sent, recs))
	return obs, nil
}

// --- consumer groups -------------------------------------------------------------------------

// seededGroup creates a 3-partition topic with n keyed records and a group that consumed them all.
func (e Env) seededGroup(ctx context.Context, tag string, n int) (topic, group string, processed int, err error) {
	topic, group = name(e, tag), name(e, tag+"g")
	if err = e.createTopic(ctx, topic, 3); err != nil {
		return
	}
	if err = e.produceKeyed(ctx, topic, keyedLines(0, n, 6, "v")); err != nil {
		return
	}
	processed, err = e.consumeGroup(ctx, topic, group, n)
	return
}

func cliGroupsDescribe(ctx context.Context, env Env) (Observation, error) {
	topic, group, processed, err := env.seededGroup(ctx, "gdesc", 60)
	if err != nil {
		return nil, err
	}
	obs := Observation{"consumer_processed_total": processed}
	obs["listed"] = env.listed(ctx, "kafka-consumer-groups.sh", group, "--list")
	desc := env.tool(ctx, "", "kafka-consumer-groups.sh", env.bs("--describe", "--group", group)...)
	rows := kout.ParseGroupDescribe(desc.Stdout, group)
	allCaughtUp, topicOK := true, true
	for _, r := range rows {
		if r.Current != r.LogEnd || r.Lag != 0 {
			allCaughtUp = false
		}
		if r.Topic != topic {
			topicOK = false
		}
	}
	obs["describe_exit"] = desc.Exit
	obs["describe_partitions"] = len(rows)
	obs["describe_topic_matches"] = topicOK
	obs["committed_sum"] = committedSum(rows)
	obs["all_partitions_lag_zero"] = allCaughtUp
	obs["says_no_active_members"] = strings.Contains(desc.Combined(), "has no active members")
	st, ok := kout.ParseGroupState(env.tool(ctx, "", "kafka-consumer-groups.sh", env.bs("--describe", "--group", group, "--state")...).Stdout, group)
	obs["state_parsed"], obs["state"], obs["state_members"] = ok, st.State, st.Members
	members := env.tool(ctx, "", "kafka-consumer-groups.sh", env.bs("--describe", "--group", group, "--members")...)
	obs["members_exit"] = members.Exit
	obs["members_listed"] = len(kout.ParseGroupDescribe(members.Stdout, group))
	return obs, nil
}

func cliGroupsReset(ctx context.Context, env Env) (Observation, error) {
	topic, group := name(env, "greset"), name(env, "gresetg")
	if err := env.createTopic(ctx, topic, 3); err != nil {
		return nil, err
	}
	if err := env.produceKeyed(ctx, topic, keyedLines(0, 30, 6, "a")); err != nil {
		return nil, err
	}
	firstBatch := kout.ParseGetOffsets(env.tool(ctx, "", "kafka-get-offsets.sh", env.bs("--topic", topic, "--time", "-1")...).Stdout, topic)
	// A datetime strictly between the two batches: second granularity, so wait on both sides.
	time.Sleep(2 * time.Second)
	dt := env.Box.Sh(ctx, "", 10*time.Second, "date -u +%Y-%m-%dT%H:%M:%S.000")
	if dt.Exit != 0 {
		return nil, fmt.Errorf("read the toolbox clock: %s", dt.Stderr)
	}
	datetime := strings.TrimSpace(dt.Stdout)
	time.Sleep(2 * time.Second)
	if err := env.produceKeyed(ctx, topic, keyedLines(30, 60, 6, "b")); err != nil {
		return nil, err
	}
	if _, err := env.consumeGroup(ctx, topic, group, 60); err != nil {
		return nil, err
	}
	obs := Observation{"committed_before": committedSum(env.describeGroup(ctx, group))}
	reset := func(key string, args ...string) map[int]int64 {
		a := env.bs(append([]string{"--reset-offsets", "--group", group, "--topic", topic}, args...)...)
		r := env.tool(ctx, "", "kafka-consumer-groups.sh", a...)
		out := kout.ParseResetOutput(r.Stdout, group, topic)
		obs[key+"_exit"] = r.Exit
		obs[key+"_new_offsets"] = kout.OffsetList(out)
		return out
	}
	reset("dry_run_to_earliest", "--to-earliest", "--dry-run")
	obs["committed_after_dry_run"] = committedSum(env.describeGroup(ctx, group))
	reset("to_earliest", "--to-earliest", "--execute")
	obs["committed_after_to_earliest"] = committedList(env.describeGroup(ctx, group))
	reset("to_offset_4", "--to-offset", "4", "--execute")
	obs["committed_after_to_offset_4"] = committedList(env.describeGroup(ctx, group))
	reset("shift_by_3", "--shift-by", "3", "--execute")
	obs["committed_after_shift_by_3"] = committedList(env.describeGroup(ctx, group))
	reset("shift_by_minus_2", "--shift-by", "-2", "--execute")
	obs["committed_after_shift_by_minus_2"] = committedList(env.describeGroup(ctx, group))
	reset("to_latest", "--to-latest", "--execute")
	obs["committed_after_to_latest"] = committedList(env.describeGroup(ctx, group))
	got := reset("to_datetime", "--to-datetime", datetime, "--execute")
	obs["to_datetime_equals_first_batch_end"] = kout.OffsetList(got) == kout.OffsetList(firstBatch)
	obs["committed_after_to_datetime"] = committedSum(env.describeGroup(ctx, group))
	reset("to_future_datetime", "--to-datetime", "2099-01-01T00:00:00.000", "--dry-run")
	return obs, nil
}

// cliGroupsActive holds a group open with a running console consumer and checks that the tool
// refuses to reset or delete it. The holder idles far longer than the row needs and is stopped
// explicitly inside the toolbox (by its unique client id), so it cannot leave early.
func cliGroupsActive(ctx context.Context, env Env) (Observation, error) {
	topic, group, _, err := env.seededGroup(ctx, "gact", 30)
	if err != nil {
		return nil, err
	}
	holderID := name(env, "holder")
	stopHolder := func() {
		env.Box.Sh(context.Background(), "", 20*time.Second, "pkill -f "+holderID+" || true")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Reads nothing new (the group is at the end), so it stays a member until stopped.
		env.Box.Tool(ctx, "", 6*time.Minute, "kafka-console-consumer.sh",
			env.bs("--topic", topic, "--group", group, "--timeout-ms", "300000", "--consumer-property", "client.id="+holderID)...)
	}()
	defer func() { stopHolder(); <-done }()
	state := func() kout.GroupState {
		st, _ := kout.ParseGroupState(env.tool(ctx, "", "kafka-consumer-groups.sh", env.bs("--describe", "--group", group, "--state")...).Stdout, group)
		return st
	}
	var st kout.GroupState
	stable := waitFor(ctx, 90*time.Second, time.Second, func() bool {
		st = state()
		return st.State == "Stable" && st.Members == 1
	})
	obs := Observation{"holder_seen_stable_one_member": stable}
	if !stable {
		return obs, fmt.Errorf("the holding consumer never showed as the group's one member (last state %q, %d members)", st.State, st.Members)
	}
	rs := env.tool(ctx, "", "kafka-consumer-groups.sh", env.bs("--reset-offsets", "--to-earliest", "--execute", "--group", group, "--topic", topic)...)
	obs["reset_active_exit"] = rs.Exit
	obs["reset_active_refused_inactive_required"] = strings.Contains(rs.Combined(), "can only be reset if the group '"+group+"' is inactive")
	obs["reset_active_new_offsets"] = kout.OffsetList(kout.ParseResetOutput(rs.Stdout, group, topic))
	obs["committed_after_refused_reset"] = committedSum(env.describeGroup(ctx, group))
	del := env.tool(ctx, "", "kafka-consumer-groups.sh", env.bs("--delete", "--group", group)...)
	obs["delete_active_exit"], obs["delete_active_exception"] = del.Exit, box.Exception(del)
	obs["listed_after_refused_delete"] = env.listed(ctx, "kafka-consumer-groups.sh", group, "--list")
	after := state()
	if after.State != "Stable" || after.Members != 1 {
		// The holder left during the row: the refusals above were not made against an active group.
		return obs, fmt.Errorf("the holding consumer left during the row (state %q, %d members after the refusals)", after.State, after.Members)
	}
	obs["still_stable_after_refusals"] = true
	return obs, nil
}

func cliGroupsDelete(ctx context.Context, env Env) (Observation, error) {
	_, group, _, err := env.seededGroup(ctx, "gdel", 30)
	if err != nil {
		return nil, err
	}
	obs := Observation{}
	del := env.tool(ctx, "", "kafka-consumer-groups.sh", env.bs("--delete", "--group", group)...)
	obs["delete_exit"] = del.Exit
	obs["delete_says_successful"] = strings.Contains(del.Combined(), "Deletion of requested consumer groups ('"+group+"') was successful.")
	obs["gone_from_list"] = waitFor(ctx, 20*time.Second, time.Second, func() bool {
		return !env.listed(ctx, "kafka-consumer-groups.sh", group, "--list")
	})
	desc := env.tool(ctx, "", "kafka-consumer-groups.sh", env.bs("--describe", "--group", group)...)
	obs["describe_deleted_exit"], obs["describe_deleted_exception"] = desc.Exit, box.Exception(desc)
	again := env.tool(ctx, "", "kafka-consumer-groups.sh", env.bs("--delete", "--group", group)...)
	obs["delete_again_exit"], obs["delete_again_exception"] = again.Exit, box.Exception(again)
	return obs, nil
}

// --- configs ---------------------------------------------------------------------------------

func cliConfigsTopic(ctx context.Context, env Env) (Observation, error) {
	t := name(env, "configs")
	if err := env.createTopic(ctx, t, 1); err != nil {
		return nil, err
	}
	describe := func() map[string]string {
		return kout.ParseConfigsDescribe(env.tool(ctx, "", "kafka-configs.sh", env.bs("--describe", "--entity-type", "topics", "--entity-name", t)...).Stdout)
	}
	obs := Observation{"dynamic_configs_at_create": len(describe())}
	alter := func(key string, args ...string) {
		r := env.tool(ctx, "", "kafka-configs.sh", env.bs(append([]string{"--alter", "--entity-type", "topics", "--entity-name", t}, args...)...)...)
		obs[key+"_exit"] = r.Exit
		obs[key+"_exception"] = box.Exception(r)
		obs[key+"_completed"] = strings.Contains(r.Stdout, "Completed updating config for topic "+t+".")
	}
	alter("add", "--add-config", "retention.ms=3600000,max.message.bytes=500000")
	c := describe()
	obs["after_add_retention_ms"], obs["after_add_max_message_bytes"] = c["retention.ms"], c["max.message.bytes"]
	alter("delete", "--delete-config", "retention.ms")
	c = describe()
	_, still := c["retention.ms"]
	obs["after_delete_retention_ms_present"], obs["after_delete_max_message_bytes"] = still, c["max.message.bytes"]
	alter("unknown_name", "--add-config", "no.such.config=1")
	alter("invalid_value", "--add-config", "retention.ms=not-a-number")
	all := env.tool(ctx, "", "kafka-configs.sh", env.bs("--describe", "--all", "--entity-type", "topics", "--entity-name", t)...)
	obs["describe_all_exit"] = all.Exit
	obs["describe_all_shows_cleanup_policy_delete"] = kout.ParseConfigsDescribe(all.Stdout)["cleanup.policy"] == "delete"
	return obs, nil
}

// --- offsets ---------------------------------------------------------------------------------

func cliGetOffsets(ctx context.Context, env Env) (Observation, error) {
	t := name(env, "offsets")
	if err := env.createTopic(ctx, t, 2); err != nil {
		return nil, err
	}
	if err := env.produceKeyed(ctx, t, keyedLines(0, 10, 10, "v")); err != nil {
		return nil, err
	}
	obs := Observation{}
	get := func(key, time string) map[int]int64 {
		r := env.tool(ctx, "", "kafka-get-offsets.sh", env.bs("--topic", t, "--time", time)...)
		m := kout.ParseGetOffsets(r.Stdout, t)
		obs[key+"_exit"] = r.Exit
		return m
	}
	latest := get("latest", "-1")
	obs["latest"] = kout.OffsetList(latest)
	obs["earliest"] = kout.OffsetList(get("earliest", "-2"))
	maxTS := get("max_timestamp", "-3")
	valid := len(maxTS) == len(latest)
	for p, o := range maxTS {
		if o < 0 || o >= latest[p] {
			valid = false
		}
	}
	obs["max_timestamp_offsets_inside_log"] = valid
	obs["by_old_timestamp"] = kout.OffsetList(get("by_old_timestamp", "1000"))
	obs["by_future_timestamp"] = kout.OffsetList(get("by_future_timestamp", "4102444800000"))
	obs["all_partitions_latest_sum"] = sumOffsets(latest)
	return obs, nil
}

func cliDeleteRecords(ctx context.Context, env Env) (Observation, error) {
	t := name(env, "delrec")
	if err := env.createTopic(ctx, t, 1); err != nil {
		return nil, err
	}
	if err := env.produceKeyed(ctx, t, keyedLines(0, 20, 1, "v")); err != nil {
		return nil, err
	}
	obs := Observation{}
	run := func(key string, offset int) {
		f := "/tmp/" + t + "-" + key + ".json"
		_ = env.Box.WriteFile(ctx, f, fmt.Sprintf(`{"partitions":[{"topic":%q,"partition":0,"offset":%d}],"version":1}`, t, offset))
		r := env.tool(ctx, "", "kafka-delete-records.sh", env.bs("--offset-json-file", f)...)
		obs[key+"_exit"] = r.Exit
		obs[key+"_low_watermark_reported"] = strings.Contains(r.Stdout, fmt.Sprintf("partition: %s-0\tlow_watermark: %d", t, offset))
		obs[key+"_exception"] = box.Exception(r)
	}
	run("delete_to_7", 7)
	obs["earliest_after"] = kout.OffsetList(kout.ParseGetOffsets(env.tool(ctx, "", "kafka-get-offsets.sh", env.bs("--topic", t, "--time", "-2")...).Stdout, t))
	obs["latest_after"] = kout.OffsetList(kout.ParseGetOffsets(env.tool(ctx, "", "kafka-get-offsets.sh", env.bs("--topic", t, "--time", "-1")...).Stdout, t))
	cons := env.tool(ctx, "", "kafka-console-consumer.sh", env.bs("--topic", t, "--from-beginning", "--max-messages", "13", "--timeout-ms", "20000",
		"--formatter-property", "print.partition=true", "--formatter-property", "print.offset=true",
		"--formatter-property", "print.headers=true", "--formatter-property", "print.key=true")...)
	recs, _ := kout.ParseConsoleRecords(cons.Stdout)
	obs["consumed_after"] = len(recs)
	if len(recs) > 0 {
		obs["first_offset_after"], obs["first_value_after"] = recs[0].Offset, recs[0].Value
	}
	run("delete_beyond_end", 50)
	return obs, nil
}

// --- verifiable producer and consumer (upstream system-test tools) ----------------------------

func cliVerifiable(ctx context.Context, env Env) (Observation, error) {
	t, group := name(env, "verif"), name(env, "verifg")
	if err := env.createTopic(ctx, t, 3); err != nil {
		return nil, err
	}
	const n = 300
	prod := env.tool(ctx, "", "kafka-verifiable-producer.sh", env.bs("--topic", t, "--max-messages", strconv.Itoa(n), "--acks", "-1")...)
	cons := env.tool(ctx, "", "kafka-verifiable-consumer.sh", env.bs("--topic", t, "--group-id", group, "--max-messages", strconv.Itoa(n), "--reset-policy", "earliest", "--verbose")...)
	obs := Observation{"producer_exit": prod.Exit, "consumer_exit": cons.Exit}
	put(obs, "check", AnalyzeVerifiable(kout.ParseVerifiable(prod.Stdout), kout.ParseVerifiable(cons.Stdout)))
	return obs, nil
}

// --- perf tools --------------------------------------------------------------------------------

func cliPerfTest(ctx context.Context, env Env) (Observation, error) {
	t := name(env, "perf")
	if err := env.createTopic(ctx, t, 3); err != nil {
		return nil, err
	}
	const n = 20000
	prod := env.tool(ctx, "", "kafka-producer-perf-test.sh", "--topic", t, "--num-records", strconv.Itoa(n), "--record-size", "100",
		"--throughput", "-1", "--producer-props", "bootstrap.servers="+env.Bootstrap, "acks=all")
	cons := env.tool(ctx, "", "kafka-consumer-perf-test.sh", env.bs("--topic", t, "--num-records", strconv.Itoa(n), "--timeout", "60000", "--group", name(env, "perfg"))...)
	return Observation{
		"producer_exit": prod.Exit, "producer_records_sent": kout.PerfProducerSent(prod.Stdout),
		"consumer_exit": cons.Exit, "consumer_records": kout.PerfConsumerCount(cons.Stdout),
	}, nil
}

// --- broker api versions -----------------------------------------------------------------------

func cliBrokerAPIVersions(ctx context.Context, env Env) (Observation, error) {
	r := env.tool(ctx, "", "kafka-broker-api-versions.sh", env.bs()...)
	s := kout.ParseAPIVersions(r.Stdout)
	return Observation{"exit": r.Exit, "brokers_listed": s.Brokers, "api_lines": s.APILines, "unparseable_lines": s.Unparseable}, nil
}
