// Package kout parses the text the Apache Kafka 4.3.1 command-line tools print (kafka-topics.sh,
// kafka-consumer-groups.sh, kafka-get-offsets.sh, kafka-configs.sh, the console consumer, the
// verifiable and perf tools, kafka-broker-api-versions.sh).
package kout

// Parsers for the text the Apache Kafka 4.3.1 tools print. Pure, unit-tested in parse_test.go
// against output captured from the real tools.

import (
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// TopicDescription is what kafka-topics.sh --describe prints for one topic.
type TopicDescription struct {
	Found          bool
	PartitionCount int
	RF             int
	Configs        map[string]string
	Partitions     []PartitionLine
}

// PartitionLine is one partition line of kafka-topics.sh --describe.
type PartitionLine struct {
	Partition int
	Leader    int
	Replicas  string
	Isr       string
}

var (
	topicHeader   = regexp.MustCompile(`^Topic: (\S+)\tTopicId: \S*\tPartitionCount: (\d+)\tReplicationFactor: (\d+)\tConfigs: ?(.*)$`)
	partitionLine = regexp.MustCompile(`^\s+Topic: (\S+)\tPartition: (\d+)\tLeader: (-?\d+|none)\tReplicas: ([0-9,]*)\tIsr: ([0-9,]*)`)
)

// ParseTopicDescribe reads the block for topic out of kafka-topics.sh --describe output.
func ParseTopicDescribe(out, topic string) TopicDescription {
	d := TopicDescription{Configs: map[string]string{}}
	for _, line := range strings.Split(out, "\n") {
		if m := topicHeader.FindStringSubmatch(line); m != nil && m[1] == topic {
			d.Found = true
			d.PartitionCount, _ = strconv.Atoi(m[2])
			d.RF, _ = strconv.Atoi(m[3])
			for _, kv := range strings.Split(m[4], ",") {
				if k, v, ok := strings.Cut(strings.TrimSpace(kv), "="); ok {
					d.Configs[k] = v
				}
			}
			continue
		}
		if m := partitionLine.FindStringSubmatch(line); m != nil && m[1] == topic {
			p, _ := strconv.Atoi(m[2])
			l := -1
			if m[3] != "none" {
				l, _ = strconv.Atoi(m[3])
			}
			d.Partitions = append(d.Partitions, PartitionLine{Partition: p, Leader: l, Replicas: m[4], Isr: m[5]})
		}
	}
	sort.Slice(d.Partitions, func(i, j int) bool { return d.Partitions[i].Partition < d.Partitions[j].Partition })
	return d
}

// AllLed reports whether every partition 0..PartitionCount-1 is listed once with a leader that
// is also its only in-sync replica on one node.
func (d TopicDescription) AllLed() bool {
	if len(d.Partitions) != d.PartitionCount {
		return false
	}
	for i, p := range d.Partitions {
		if p.Partition != i || p.Leader < 0 || p.Isr == "" {
			return false
		}
	}
	return true
}

// Lines returns the non-empty trimmed lines of s.
func Lines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

// GroupOffsetRow is one partition line of kafka-consumer-groups.sh --describe.
type GroupOffsetRow struct {
	Topic     string
	Partition int
	Current   int64 // -1 when the tool prints "-"
	LogEnd    int64
	Lag       int64 // -1 when the tool prints "-"
}

func dashInt(s string) int64 {
	if s == "-" {
		return -1
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return -2
	}
	return v
}

// ParseGroupDescribe reads the offsets table of kafka-consumer-groups.sh --describe --group g.
func ParseGroupDescribe(out, group string) []GroupOffsetRow {
	var rows []GroupOffsetRow
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) < 6 || f[0] != group {
			continue
		}
		p, err := strconv.Atoi(f[2])
		if err != nil {
			continue
		}
		rows = append(rows, GroupOffsetRow{Topic: f[1], Partition: p, Current: dashInt(f[3]), LogEnd: dashInt(f[4]), Lag: dashInt(f[5])})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Partition < rows[j].Partition })
	return rows
}

// GroupState is the --describe --state line: state and member count.
type GroupState struct {
	State   string
	Members int
}

// ParseGroupState reads kafka-consumer-groups.sh --describe --state for group.
func ParseGroupState(out, group string) (GroupState, bool) {
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) >= 4 && f[0] == group && f[0] != "GROUP" {
			n, err := strconv.Atoi(f[len(f)-1])
			if err != nil {
				continue
			}
			return GroupState{State: f[len(f)-2], Members: n}, true
		}
	}
	return GroupState{}, false
}

// ParseResetOutput reads the NEW-OFFSET table of kafka-consumer-groups.sh --reset-offsets:
// partition to new offset, for one group and topic.
func ParseResetOutput(out, group, topic string) map[int]int64 {
	m := map[int]int64{}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) != 4 || f[0] != group || f[1] != topic {
			continue
		}
		p, err1 := strconv.Atoi(f[2])
		o, err2 := strconv.ParseInt(f[3], 10, 64)
		if err1 == nil && err2 == nil {
			m[p] = o
		}
	}
	return m
}

// ParseGetOffsets reads kafka-get-offsets.sh output ("topic:partition:offset" lines) for topic.
func ParseGetOffsets(out, topic string) map[int]int64 {
	m := map[int]int64{}
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		rest, ok := strings.CutPrefix(line, topic+":")
		if !ok {
			continue
		}
		ps, os, ok := strings.Cut(rest, ":")
		if !ok {
			continue
		}
		p, err1 := strconv.Atoi(ps)
		o, err2 := strconv.ParseInt(os, 10, 64)
		if err1 == nil && err2 == nil {
			m[p] = o
		}
	}
	return m
}

// OffsetList renders a partition-to-offset map as "p0=o0 p1=o1 ..." in partition order, the
// deterministic form rows put into observations.
func OffsetList(m map[int]int64) string {
	ps := make([]int, 0, len(m))
	for p := range m {
		ps = append(ps, p)
	}
	sort.Ints(ps)
	parts := make([]string, 0, len(ps))
	for _, p := range ps {
		parts = append(parts, strconv.Itoa(p)+"="+strconv.FormatInt(m[p], 10))
	}
	return strings.Join(parts, " ")
}

var dynamicConfigLine = regexp.MustCompile(`^\s+([a-z0-9._-]+)=(\S*) sensitive=`)

// ParseConfigsDescribe reads kafka-configs.sh --describe: config name to value.
func ParseConfigsDescribe(out string) map[string]string {
	m := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		if f := dynamicConfigLine.FindStringSubmatch(line); f != nil {
			m[f[1]] = f[2]
		}
	}
	return m
}

// ConsoleRecord is one record as the console consumer printed it with print.partition,
// print.offset, print.headers and print.key on, tab-separated.
type ConsoleRecord struct {
	Partition int
	Offset    int64
	Headers   string
	Key       string
	Value     string
}

// ParseConsoleRecords reads console-consumer lines "Partition:p\tOffset:o\theaders\tkey\tvalue".
// Lines of any other shape are counted as malformed.
func ParseConsoleRecords(out string) (recs []ConsoleRecord, malformed int) {
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "Processed a total of") {
			continue
		}
		f := strings.SplitN(line, "\t", 5)
		if len(f) != 5 || !strings.HasPrefix(f[0], "Partition:") || !strings.HasPrefix(f[1], "Offset:") {
			malformed++
			continue
		}
		p, err1 := strconv.Atoi(strings.TrimPrefix(f[0], "Partition:"))
		o, err2 := strconv.ParseInt(strings.TrimPrefix(f[1], "Offset:"), 10, 64)
		if err1 != nil || err2 != nil {
			malformed++
			continue
		}
		recs = append(recs, ConsoleRecord{Partition: p, Offset: o, Headers: f[2], Key: f[3], Value: f[4]})
	}
	return recs, malformed
}

// ProcessedTotal is the console consumer's own "Processed a total of N messages" count, -1 if absent.
func ProcessedTotal(stderr string) int {
	m := regexp.MustCompile(`Processed a total of (\d+) messages`).FindStringSubmatch(stderr)
	if m == nil {
		return -1
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

// VerifiableEvent is one JSON line of kafka-verifiable-producer.sh / -consumer.sh.
type VerifiableEvent struct {
	Name       string `json:"name"`
	Value      string `json:"value"`
	Partition  int    `json:"partition"`
	Offset     int64  `json:"offset"`
	Sent       int    `json:"sent"`
	Acked      int    `json:"acked"`
	Count      int    `json:"count"`
	Success    *bool  `json:"success"`
	Partitions []struct {
		Partition int   `json:"partition"`
		Count     int   `json:"count"`
		MinOffset int64 `json:"minOffset"`
		MaxOffset int64 `json:"maxOffset"`
		Offset    int64 `json:"offset"`
	} `json:"partitions"`
	Offsets []struct {
		Partition int   `json:"partition"`
		Offset    int64 `json:"offset"`
	} `json:"offsets"`
}

// ParseVerifiable reads the JSON event lines; non-JSON lines are skipped.
func ParseVerifiable(out string) []VerifiableEvent {
	var evs []VerifiableEvent
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var e VerifiableEvent
		if json.Unmarshal([]byte(line), &e) == nil && e.Name != "" {
			evs = append(evs, e)
		}
	}
	return evs
}

var perfProducerLine = regexp.MustCompile(`(?m)^(\d+) records sent, `)

// PerfProducerSent is the final "N records sent" count of kafka-producer-perf-test.sh, -1 if absent.
func PerfProducerSent(out string) int {
	all := perfProducerLine.FindAllStringSubmatch(out, -1)
	if len(all) == 0 {
		return -1
	}
	n, _ := strconv.Atoi(all[len(all)-1][1])
	return n
}

// PerfConsumerCount is data.consumed.in.nMsg of kafka-consumer-perf-test.sh's CSV line, -1 if absent.
func PerfConsumerCount(out string) int {
	var header []string
	for _, line := range strings.Split(out, "\n") {
		f := strings.Split(line, ",")
		for i := range f {
			f[i] = strings.TrimSpace(f[i])
		}
		if len(f) > 4 && f[0] == "start.time" {
			header = f
			continue
		}
		if header != nil && len(f) == len(header) {
			for i, h := range header {
				if h == "data.consumed.in.nMsg" {
					n, err := strconv.Atoi(f[i])
					if err == nil {
						return n
					}
				}
			}
		}
	}
	return -1
}

var apiVersionsHeader = regexp.MustCompile(`(?m)^(\S+:\d+) \(id: (-?\d+) rack: `)
var apiVersionsLine = regexp.MustCompile(`^\s*([A-Za-z]+)\((\d+)\): (.+?),?$`)

// APIVersionsSummary describes kafka-broker-api-versions.sh output: brokers listed and how many
// API lines parse as either a version range or UNSUPPORTED.
type APIVersionsSummary struct {
	Brokers     int
	APILines    int
	Unparseable int
}

// ParseAPIVersions summarizes kafka-broker-api-versions.sh output.
func ParseAPIVersions(out string) APIVersionsSummary {
	var s APIVersionsSummary
	s.Brokers = len(apiVersionsHeader.FindAllString(out, -1))
	versionText := regexp.MustCompile(`^(UNSUPPORTED|\d+( to \d+)? \[usable: (\d+|NONE)\])$`)
	in := false
	for _, line := range strings.Split(out, "\n") {
		if apiVersionsHeader.MatchString(line) {
			in = true
			continue
		}
		if !in || strings.TrimSpace(line) == ")" || strings.TrimSpace(line) == "" {
			continue
		}
		m := apiVersionsLine.FindStringSubmatch(line)
		if m == nil {
			s.Unparseable++
			continue
		}
		s.APILines++
		if !versionText.MatchString(strings.TrimSuffix(m[3], ",")) {
			s.Unparseable++
		}
	}
	return s
}
