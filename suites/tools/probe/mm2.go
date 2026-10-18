package probe

// MirrorMaker 2 rows: connect-mirror-maker.sh (dedicated mode) from the Apache Kafka 4.3.1 image,
// unmodified. Ported from kubemq-server tests/kafka-ecosystem/mirrormaker2.sh and extended to
// both directions:
//
//   into   a second, real Apache Kafka 4.3.1 broker (started by this scenario) is the source;
//          the broker under test is the target (the migration direction)
//   from   the broker under test is the source; the second Kafka broker is the target (the
//          rollback direction)
//
// The oracle leg runs the same two directions with the Kafka oracle as the broker under test.
// Per direction, three rows:
//   mirror        the remote topic exists with the source's partition count; per-partition end
//                 offsets are equal; every record's partition, offset, headers, key and value are
//                 identical (records carry multi-byte characters)
//   internal      offset-syncs (placed on the target), checkpoints, heartbeats and the three
//                 Connect topics exist on the target, compacted where MirrorMaker 2 asks for it
//   group-offsets a source group committed at the end of every partition gets exactly the same
//                 offsets on the target (MirrorMaker 2 translates exactly at a sync point; the
//                 kubemq-server leg found that a committed offset of 0 may not be translated)

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/box"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/mirror"
)

func init() {
	for _, dir := range []string{"into", "from"} {
		for _, s := range []string{"mirror", "internal", "group-offsets"} {
			register("mm2-"+dir+"-"+s, sectionProbe("mm2", mm2Scenario, dir+"-"+s))
		}
	}
}

const (
	mm2Records    = 600
	mm2Partitions = 3
	mm2Timeout    = 180 * time.Second
)

type mm2Run struct {
	env   Env
	dir   string
	net   string
	under mirror.Cluster // the broker under test
	kafka mirror.Cluster // the second Kafka broker
	stops []func()
}

func (m *mm2Run) cleanup() {
	for i := len(m.stops) - 1; i >= 0; i-- {
		m.stops[i]()
	}
}

func mm2Scenario(ctx context.Context, env Env, s *Sections) {
	dir, err := workDir(env, "mm2")
	if err != nil {
		s.stop("into-mirror", err)
		return
	}
	m := &mm2Run{env: env, dir: dir, net: containerName(env, "mm2", "net")}
	rmNet, err := box.Network(ctx, m.net)
	if err != nil {
		s.stop("into-mirror", err)
		return
	}
	defer rmNet()
	defer m.cleanup()
	kafka, stopKafka, err := mirror.StartKafka(ctx, containerName(env, "mm2", "kafka"), m.net, filepath.Join(dir, "logs", "kafka-second.log"))
	if err != nil {
		s.stop("into-mirror", err)
		return
	}
	m.stops = append(m.stops, stopKafka)
	m.kafka = kafka
	m.under = mirror.Cluster{Box: env.Box, Bootstrap: env.Bootstrap, Advertise: env.Bootstrap}
	m.direction(ctx, s, "into", m.kafka, m.under)
	m.direction(ctx, s, "from", m.under, m.kafka)
}

// direction mirrors one topic from src to tgt and fills the three sections of that direction.
// A failure stops only this direction; the other still runs.
func (m *mm2Run) direction(ctx context.Context, s *Sections, dir string, src, tgt mirror.Cluster) {
	// stop records err on the given section and on every later section of this direction.
	stop := func(section string, err error) {
		later := false
		for _, x := range []string{"mirror", "internal", "group-offsets"} {
			if x == section {
				later = true
				s.fail(dir+"-"+x, err)
				continue
			}
			if later {
				s.fail(dir+"-"+x, fmt.Errorf("the %s direction stopped in its %s phase, before this one: %v", dir, section, err))
			}
		}
	}
	topic := name(m.env, "mm2"+dir)
	// Aliases name MirrorMaker 2's checkpoint and Connect topics: unique per run so no topic
	// left by another MirrorMaker 2 run on the same broker can stand in for this run's.
	suffix := topic[len(topic)-6:]
	srcAlias, tgtAlias := dir+"src"+suffix, dir+"tgt"+suffix
	remote := srcAlias + "." + topic
	group := topic + "-grp"

	// Source topic: 600 keyed records with headers and multi-byte characters.
	if r := src.Tool(ctx, "", "kafka-topics.sh", "--create", "--topic", topic, "--partitions", fmt.Sprint(mm2Partitions), "--replication-factor", "1"); r.Exit != 0 {
		stop("mirror", fmt.Errorf("create source topic: %s", firstErrorLine(r.Combined())))
		return
	}
	var in strings.Builder
	var produced []string
	for i := 1; i <= mm2Records; i++ {
		line := fmt.Sprintf("origin:eco,seq:%d\tk%06d\tvalue-%06d mirrored \u00e9\u2713 %d", i, i%97, i, i*7919%100003)
		produced = append(produced, line)
		in.WriteString(line + "\n")
	}
	if r := src.Tool(ctx, in.String(), "kafka-console-producer.sh", "--topic", topic,
		"--reader-property", "parse.headers=true", "--reader-property", "parse.key=true", "--producer-property", "acks=all"); r.Exit != 0 {
		stop("mirror", fmt.Errorf("produce to the source: %s", firstErrorLine(r.Combined())))
		return
	}
	srcEnd := src.EndOffsets(ctx, topic)
	if sumOffsets(srcEnd) != mm2Records {
		stop("mirror", fmt.Errorf("the source took %d of %d records", sumOffsets(srcEnd), mm2Records))
		return
	}
	// The source group commits the end of every partition, where translation is exact.
	if r := src.Tool(ctx, "", "kafka-consumer-groups.sh", "--group", group, "--reset-offsets", "--topic", topic, "--to-latest", "--execute"); r.Exit != 0 {
		stop("mirror", fmt.Errorf("commit the source group: %s", firstErrorLine(r.Combined())))
		return
	}
	srcGroup := src.GroupOffsets(ctx, group, topic)

	heartbeatsBefore := sumOffsets(tgt.EndOffsets(ctx, "heartbeats"))
	logPath := filepath.Join(m.dir, "logs", dir+"-mm2.log")
	mm, stopMM, err := mirror.Start(ctx, mirror.Spec{
		Name: containerName(m.env, "mm2", dir), Network: m.net, ConfDir: filepath.Join(m.dir, "conf"),
		SrcAlias: srcAlias, TgtAlias: tgtAlias, Src: src, Tgt: tgt,
		Topics: topic, Groups: mirror.QuoteGroup(group),
	}, logPath)
	if err != nil {
		stop("mirror", err)
		return
	}
	defer stopMM()

	// --- mirror -----------------------------------------------------------------------------
	arrived := mirror.WaitRemote(ctx, tgt, remote, mm2Records, mm2Timeout)
	if !mm.Running(ctx) {
		stop("mirror", fmt.Errorf("MirrorMaker 2 exited (log in the run directory: %s)", logPath))
		return
	}
	time.Sleep(5 * time.Second) // a late duplicate would land now
	o := Observation{"remote_topic_complete": arrived}
	d := kout.ParseTopicDescribe(tgt.Tool(ctx, "", "kafka-topics.sh", "--describe", "--topic", remote).Stdout, remote)
	o["remote_topic_exists"] = d.Found
	o["remote_partitions_equal_source"] = d.PartitionCount == mm2Partitions
	tgtEnd := tgt.EndOffsets(ctx, remote)
	o["end_offsets_equal_per_partition"] = kout.OffsetList(tgtEnd) == kout.OffsetList(srcEnd)
	srcRecs, err1 := src.Drain(ctx, topic, mm2Records)
	tgtRecs, err2 := tgt.Drain(ctx, remote, sumOffsets(tgtEnd))
	if err1 != nil {
		stop("mirror", err1)
		return
	}
	o["remote_drain_complete"] = err2 == nil
	o["records_identical"] = SortedEqual(srcRecs, tgtRecs)
	// The source itself must hold what was produced (headers, key, value): otherwise a source that
	// mangles records would be mirrored faithfully and still compare identical.
	o["source_holds_what_was_produced"] = SortedEqual(stripPartitionOffset(srcRecs), produced)
	o["remote_topic_has_topic_id"] = mirror.TopicIDPresent(tgt.Tool(ctx, "", "kafka-topics.sh", "--describe", "--topic", remote).Stdout)
	s.set(dir+"-mirror", o)

	// --- internal topics ----------------------------------------------------------------------
	o = Observation{}
	var syncs string
	for _, t := range tgt.Topics(ctx) {
		if strings.HasPrefix(t, "mm2-offset-syncs.") && strings.HasSuffix(t, ".internal") {
			syncs = t
		}
	}
	o["offset_syncs_topic_exists"] = syncs != ""
	if syncs != "" {
		o["offset_syncs_cleanup_policy"] = tgt.CleanupPolicy(ctx, syncs)
		o["offset_syncs_not_empty"] = sumOffsets(tgt.EndOffsets(ctx, syncs)) > 0
	}
	o["heartbeats_written_by_this_run"] = sumOffsets(tgt.EndOffsets(ctx, "heartbeats")) > heartbeatsBefore
	for _, t := range []string{srcAlias + ".checkpoints.internal", "heartbeats", "mm2-configs." + srcAlias + ".internal", "mm2-offsets." + srcAlias + ".internal", "mm2-status." + srcAlias + ".internal"} {
		key := strings.NewReplacer(srcAlias+".", "", "."+srcAlias, "", ".internal", "", ".", "_", "-", "_").Replace(t)
		o[key+"_exists"] = tgt.Listed(ctx, t)
		o[key+"_cleanup_policy"] = tgt.CleanupPolicy(ctx, t)
	}
	s.set(dir+"-internal", o)

	// --- translated group offsets -----------------------------------------------------------
	var got map[int]int64
	exact := waitFor(ctx, mm2Timeout, 3*time.Second, func() bool {
		got = tgt.GroupOffsets(ctx, group, remote)
		return kout.OffsetList(got) == kout.OffsetList(srcGroup)
	})
	ahead, behind, missing := 0, 0, 0
	for p, want := range srcGroup {
		g, ok := got[p]
		switch {
		case !ok:
			missing++
		case g > want:
			ahead++
		case g < want:
			behind++
		}
	}
	o = Observation{"source_group_committed_at_end": len(srcGroup) == mm2Partitions && kout.OffsetList(srcGroup) == kout.OffsetList(srcEnd),
		"translated_exactly": exact, "partitions_missing": missing, "partitions_ahead": ahead, "partitions_behind": behind,
		"mirror_maker_loaded_kafka_clients": clientVersionsInLog(logPath)}
	s.set(dir+"-group-offsets", o)
}

// stripPartitionOffset drops the "Partition:p\tOffset:o\t" prefix of drained lines, leaving
// headers, key and value.
func stripPartitionOffset(lines []string) []string {
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		f := strings.SplitN(l, "\t", 3)
		if len(f) == 3 {
			out = append(out, f[2])
		} else {
			out = append(out, l)
		}
	}
	return out
}
