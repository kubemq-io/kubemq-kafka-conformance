package kout

// Parser tests on output captured from the real Apache Kafka 4.3.1 tools (2026-10-07).

import (
	"testing"
)

const topicsDescribe = "Topic: t1\tTopicId: Fz8iEBmCSjqHAb7KuNuXmA\tPartitionCount: 3\tReplicationFactor: 1\tConfigs: min.insync.replicas=1,segment.bytes=1073741824,retention.ms=86400000\n" +
	"\tTopic: t1\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n" +
	"\tTopic: t1\tPartition: 1\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n" +
	"\tTopic: t1\tPartition: 2\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n"

func TestParseTopicDescribe(t *testing.T) {
	d := ParseTopicDescribe(topicsDescribe, "t1")
	if !d.Found || d.PartitionCount != 3 || d.RF != 1 || d.Configs["retention.ms"] != "86400000" || !d.AllLed() {
		t.Fatalf("parsed %+v", d)
	}
	if ParseTopicDescribe(topicsDescribe, "t2").Found {
		t.Error("another topic's block must not match")
	}
	// A missing partition line or a partition without a leader is not "all led".
	short := ParseTopicDescribe(topicsDescribe[:len(topicsDescribe)-60], "t1")
	if short.AllLed() {
		t.Error("two of three partition lines must not count as all led")
	}
	noLeader := ParseTopicDescribe("Topic: t1\tTopicId: x\tPartitionCount: 1\tReplicationFactor: 1\tConfigs: \n\tTopic: t1\tPartition: 0\tLeader: none\tReplicas: 1\tIsr: \tElr: \tLastKnownElr: \n", "t1")
	if !noLeader.Found || noLeader.AllLed() {
		t.Errorf("leaderless partition: %+v", noLeader)
	}
}

const groupDescribe = `Consumer group 'g1' has no active members.
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
g1              t1              0          12              12              0               -               -               -
g1              t1              1          4               4               0               -               -               -
g1              t1              2          -               5               -               -               -               -
`

func TestParseGroupDescribe(t *testing.T) {
	rows := ParseGroupDescribe(groupDescribe, "g1")
	if len(rows) != 3 || rows[0].Current != 12 || rows[1].LogEnd != 4 || rows[2].Current != -1 || rows[2].Lag != -1 {
		t.Fatalf("rows %+v", rows)
	}
}

func TestParseGroupState(t *testing.T) {
	out := "Consumer group 'g1' has no active members.\n" +
		"GROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS\n" +
		"g1              localhost:9092  (1)       -                    Empty                0\n"
	st, ok := ParseGroupState(out, "g1")
	if !ok || st.State != "Empty" || st.Members != 0 {
		t.Fatalf("%+v %v", st, ok)
	}
	stable := "GROUP COORDINATOR (ID) ASSIGNMENT-STRATEGY STATE #MEMBERS\ng2 host:1 (1) range Stable 1\n"
	if st, ok := ParseGroupState(stable, "g2"); !ok || st.State != "Stable" || st.Members != 1 {
		t.Fatalf("%+v %v", st, ok)
	}
}

func TestParseResetAndGetOffsets(t *testing.T) {
	reset := "Warn: Partition 1 from topic t1 is empty. Falling back to latest known offset.\n" +
		"GROUP           TOPIC           PARTITION  NEW-OFFSET\n" +
		"g1              t1              1          4\n" +
		"g1              t1              0          12\n"
	if got := OffsetList(ParseResetOutput(reset, "g1", "t1")); got != "0=12 1=4" {
		t.Errorf("reset: %s", got)
	}
	if got := OffsetList(ParseGetOffsets("t1:0:12\nt1:1:4\nt10:0:9\n", "t1")); got != "0=12 1=4" {
		t.Errorf("get-offsets: %s", got)
	}
	if got := OffsetList(ParseGetOffsets("", "t1")); got != "" {
		t.Errorf("no output must be no offsets: %q", got)
	}
}

func TestParseConfigsDescribe(t *testing.T) {
	out := "Dynamic configs for topic t1 are:\n" +
		"  cleanup.policy=compact sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=compact, DEFAULT_CONFIG:log.cleanup.policy=delete}\n" +
		"  retention.ms=1234 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=1234}\n"
	c := ParseConfigsDescribe(out)
	if len(c) != 2 || c["cleanup.policy"] != "compact" || c["retention.ms"] != "1234" {
		t.Fatalf("%v", c)
	}
}

func TestParseConsoleAndCounts(t *testing.T) {
	out := "Partition:0\tOffset:9\tNO_HEADERS\tk4\tv25\nPartition:2\tOffset:0\tseq:1,src:cli\tk1\tv-0001\ngarbage line\n"
	recs, bad := ParseConsoleRecords(out)
	if len(recs) != 2 || bad != 1 || recs[1].Headers != "seq:1,src:cli" || recs[1].Partition != 2 {
		t.Fatalf("%+v bad=%d", recs, bad)
	}
	if ProcessedTotal("x\nProcessed a total of 30 messages\n") != 30 || ProcessedTotal("") != -1 {
		t.Error("processed total")
	}
	if PerfProducerSent("1000 records sent, 5.0 records/sec\n2000 records sent, 4750.59 records/sec (0.45 MB/sec), 6.52 ms avg latency\n") != 2000 {
		t.Error("perf producer: the final line counts")
	}
	csv := "Warning: --messages is deprecated.\nstart.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec\n" +
		"2026-10-07 07:07:51:221, 2026-10-07 07:07:54:562, 0.1907, 0.0571, 2000, 598.6232, 3304, 37, 5.1550, 54054.0541\n"
	if PerfConsumerCount(csv) != 2000 || PerfConsumerCount("no csv") != -1 {
		t.Error("perf consumer count")
	}
}

func TestParseAPIVersions(t *testing.T) {
	out := "localhost:9092 (id: 1 rack: null isFenced: false) -> (\n" +
		"\tProduce(0): 3 to 13 [usable: 13],\n" +
		"\tOffsetDelete(47): 0 [usable: 0],\n" +
		"\tPushTelemetry(72): UNSUPPORTED,\n" +
		"\tDeleteShareGroupOffsets(92): 0 [usable: 0]\n" +
		")\n"
	s := ParseAPIVersions(out)
	if s.Brokers != 1 || s.APILines != 4 || s.Unparseable != 0 {
		t.Fatalf("%+v", s)
	}
	if s := ParseAPIVersions(out[:len(out)-3] + "\n\tWeird(99): something,\n)\n"); s.Unparseable != 1 {
		t.Errorf("an unknown version form must count as unparseable: %+v", s)
	}
}
