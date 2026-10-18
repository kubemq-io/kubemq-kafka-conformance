package verify

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// toolEvent is one line of kafka-verifiable-producer or kafka-verifiable-consumer output (the
// format Apache Kafka's ducktape services parse), plus the version line the track prints first.
type toolEvent struct {
	Name       string  `json:"name"`
	Version    string  `json:"version"`
	Key        *string `json:"key"`
	Value      *string `json:"value"`
	Partition  int32   `json:"partition"`
	Offset     int64   `json:"offset"`
	Exception  string  `json:"exception"`
	Message    string  `json:"message"`
	Success    *bool   `json:"success"`
	Error      string  `json:"error"`
	Partitions []struct {
		Partition int32 `json:"partition"`
	} `json:"partitions"`
}

// FromVerifiable turns the two tools' output into a ledger (SCENARIO.md), so the verifiable row
// is judged by exactly the same checks as every driver. The producer output must begin with the
// track's {"name":"kcert_version"} line. A tool that did not reach shutdown_complete leaves the
// ledger without its done event.
func FromVerifiable(producer, consumer []byte) Ledger {
	var l Ledger
	prod, pp := toolLines("producer", producer)
	cons, cp := toolLines("consumer", consumer)
	l.Problems = append(pp, cp...)
	version := ""
	for _, e := range prod {
		if e.Name == "kcert_version" {
			version = e.Version
		}
	}
	add := func(e Event) { e.Line = len(l.Events) + 1; l.Events = append(l.Events, e) }
	add(Event{Ev: "start", Client: "kafka-clients", Version: version, Mode: verifiableMode, GroupProtocol: "classic"})
	prodDone, consDone := false, false
	for _, e := range prod {
		switch e.Name {
		case "producer_send_success":
			add(Event{Ev: "ack", Phase: 1, Seq: seqp(e.Value), Partition: e.Partition, Offset: e.Offset})
		case "producer_send_error":
			add(Event{Ev: "ack", Phase: 1, Seq: seqp(e.Value), Partition: -1, Offset: -1, ErrorCode: -1,
				Error: e.Exception + ": " + e.Message})
		case "shutdown_complete":
			prodDone = true
		}
	}
	for _, e := range cons {
		switch e.Name {
		case "partitions_assigned":
			ps := make([]int32, 0, len(e.Partitions))
			for _, p := range e.Partitions {
				ps = append(ps, p.Partition)
			}
			sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
			add(Event{Ev: "assigned", Phase: 1, Partitions: ps})
		case "record_data":
			add(Event{Ev: "recv", Phase: 1, Seq: seqp(e.Value), Partition: e.Partition, Offset: e.Offset, Key: e.Key, Value: e.Value})
		case "offsets_committed":
			ev := Event{Ev: "commit", Phase: 1}
			if e.Success == nil || !*e.Success {
				ev.ErrorCode, ev.Error = -1, "offsets_committed success=false: "+e.Error
			}
			add(ev)
		case "shutdown_complete":
			consDone = true
		}
	}
	if !prodDone {
		add(Event{Ev: "error", Step: "produce", ErrorCode: -1, Error: "kafka-verifiable-producer did not reach shutdown_complete"})
	}
	if !consDone {
		add(Event{Ev: "error", Step: "consume", ErrorCode: -1, Error: "kafka-verifiable-consumer did not reach shutdown_complete"})
	}
	if prodDone && consDone {
		add(Event{Ev: "done"})
	}
	return l
}

func toolLines(tool string, out []byte) ([]toolEvent, []string) {
	var evs []toolEvent
	var problems []string
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	n := 0
	for sc.Scan() {
		n++
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e toolEvent
		if err := json.Unmarshal(line, &e); err != nil || e.Name == "" {
			problems = append(problems, fmt.Sprintf("%s output line %d is not a tool event: %.120q", tool, n, line))
			continue
		}
		evs = append(evs, e)
	}
	return evs, problems
}

func seqp(v *string) *int {
	n := -1
	if v != nil {
		if i, err := strconv.Atoi(*v); err == nil {
			n = i
		}
	}
	return &n
}
