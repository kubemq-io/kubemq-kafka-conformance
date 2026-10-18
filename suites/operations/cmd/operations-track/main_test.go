package main

// Row-level verdict mapping of the operations track, including gap binding. No broker.

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/operations/config"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/operations/metric"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/operations/workload"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
)

const kIn = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:Count"

func row(kubemq *metric.Selector) metric.Row {
	return metric.Row{ID: "operations.metrics.messages-in", Workload: "produce-consume",
		Kafka:  metric.Selector{Name: kIn, Labels: map[string]string{"topic": "${topic}"}},
		KubeMQ: kubemq, Expect: metric.Expect{Kind: metric.DeltaExact, Of: "records"}}
}

func sideWith(topic string, before, after metric.Scrape) *side {
	return &side{before: before, after: after, run: testRun(topic)}
}

func testRun(topic string) *workload.Run {
	return &workload.Run{Vars: map[string]string{"topic": topic}, Quantities: map[string]float64{"records": 500}}
}

func kafkaSide(n float64) *side {
	return sideWith("t-k", nil, metric.Scrape{{Name: kIn, Labels: map[string]string{"topic": "t-k"}, Value: n}})
}

func kubemqSide(n float64) *side {
	return sideWith("t-q", metric.Scrape{{Name: "kubemq_kafka_messages_total", Labels: map[string]string{"op": "produced"}, Value: 1000}},
		metric.Scrape{{Name: "kubemq_kafka_messages_total", Labels: map[string]string{"op": "produced"}, Value: 1000 + n}})
}

var qsel = &metric.Selector{Name: "kubemq_kafka_messages_total", Labels: map[string]string{"op": "produced"}}

func judge(r metric.Row, o, k *side, reg map[string]gaps.Entry) Row {
	out := Row{ID: r.ID}
	JudgeRow(&out, r, o, k, reg, time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC))
	return out
}

func signed(dev string) map[string]gaps.Entry {
	return map[string]gaps.Entry{"operations.metrics.messages-in": {Row: "operations.metrics.messages-in", Deviation: dev,
		SignedBy: "reviewer", Signed: time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC), Valid: true}}
}

func TestJudgeRow(t *testing.T) {
	const lossDev = "expectation_met: Kafka true, KubeMQ false"
	const absentDev = "series_present: Kafka true, KubeMQ absent"
	cases := []struct {
		name    string
		r       metric.Row
		o, k    *side
		reg     map[string]gaps.Entry
		verdict string
	}{
		{"both meet", row(qsel), kafkaSide(500), kubemqSide(500), nil, "PASS"},
		{"KubeMQ loses one", row(qsel), kafkaSide(500), kubemqSide(499), nil, "FAIL"},
		{"KubeMQ loses one, registered", row(qsel), kafkaSide(500), kubemqSide(499), signed(lossDev), "GAP"},
		{"KubeMQ loses one, other deviation registered", row(qsel), kafkaSide(500), kubemqSide(499), signed(absentDev), "FAIL"},
		{"oracle wrong", row(qsel), kafkaSide(499), kubemqSide(500), nil, "VOID"},
		{"oracle workload failed", row(qsel), &side{err: errors.New("boom")}, kubemqSide(500), nil, "VOID"},
		{"KubeMQ workload failed", row(qsel), kafkaSide(500), &side{err: errors.New("boom")}, nil, "FAIL"},
		{"oracle wrong and KubeMQ failed", row(qsel), kafkaSide(1), &side{err: errors.New("boom")}, nil, "VOID"},
		{"no KubeMQ equivalent", row(nil), kafkaSide(500), kubemqSide(500), nil, "FAIL"},
		{"no KubeMQ equivalent, registered", row(nil), kafkaSide(500), kubemqSide(500), signed(absentDev), "GAP"},
		{"workload never ran", row(qsel), nil, nil, nil, "NOT-RUN"},
	}
	for _, c := range cases {
		got := judge(c.r, c.o, c.k, c.reg)
		if got.Verdict != c.verdict {
			t.Errorf("%s: verdict %s (%s), want %s", c.name, got.Verdict, got.Detail, c.verdict)
		}
		if got.Verdict == "GAP" && got.Deviation != c.reg["operations.metrics.messages-in"].Deviation {
			t.Errorf("%s: GAP row must carry the signed deviation, got %q", c.name, got.Deviation)
		}
		if (got.Verdict == "PASS" || got.Verdict == "VOID") && got.Deviation != "" {
			t.Errorf("%s: %s row carries a deviation %q", c.name, got.Verdict, got.Deviation)
		}
	}
}

func TestJudgeRowBindsWorkloadNames(t *testing.T) {
	// The oracle series is for topic t-k; a selector bound to another topic must not see it.
	o := kafkaSide(500)
	o.run = testRun("other")
	if got := judge(row(qsel), o, kubemqSide(500), nil); got.Verdict != "VOID" || !strings.Contains(got.Detail, "series absent") {
		t.Fatalf("got %s %s", got.Verdict, got.Detail)
	}
}

func TestJMXPatterns(t *testing.T) {
	rows := []metric.Row{
		{Kafka: metric.Selector{Name: kIn}},
		{Kafka: metric.Selector{Name: "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec:Count"}},
		{Kafka: metric.Selector{Name: "kafka.server:type=socket-server-metrics:connection-count"}},
		{Kafka: metric.Selector{Name: "admin:consumer_group_lag"}},
		{Kafka: metric.Selector{Name: kIn}},
	}
	want := []string{
		"kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,*",
		"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,*",
		"kafka.server:type=socket-server-metrics,*",
	}
	if got := JMXPatterns(rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

// Every Kafka selector in the committed matrix must be selectable by a pattern the track asks
// kafka-jmx.sh for, or the row could never see its series.
func TestCommittedMatrixPatternsCoverEveryRow(t *testing.T) {
	m, err := metric.LoadMatrix("../../../../matrix/operations.yaml")
	if err != nil {
		t.Fatal(err)
	}
	pats := JMXPatterns(m.Rows)
	for _, r := range m.Rows {
		n := r.Kafka.Name
		if r.Probe != "" {
			continue
		}
		if strings.HasPrefix(n, "admin:") {
			if !strings.HasPrefix(r.Workload, "group-lag") {
				t.Errorf("%s: admin series are only computed by the group-lag workloads", r.ID)
			}
			continue
		}
		prefix := n[:strings.LastIndexByte(n, ':')]
		found := false
		for _, p := range pats {
			if strings.TrimSuffix(p, ",*") == prefix {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: no JMX pattern covers %s", r.ID, n)
		}
	}
}

func TestJudgeProbe(t *testing.T) {
	reg := map[string]gaps.Entry{"operations.config.x": {Row: "operations.config.x", Deviation: "create_error_code: Kafka 40, KubeMQ 0",
		SignedBy: "reviewer", Signed: time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC), Valid: true}}
	run := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name    string
		o, k    config.Observation
		reg     map[string]gaps.Entry
		verdict string
	}{
		{"same", config.Observation{"create_error_code": 40}, config.Observation{"create_error_code": 40}, nil, "PASS"},
		{"wrong error code", config.Observation{"create_error_code": 40}, config.Observation{"create_error_code": 0}, nil, "FAIL"},
		{"wrong error code, registered", config.Observation{"create_error_code": 40}, config.Observation{"create_error_code": 0}, reg, "GAP"},
		{"other field differs too", config.Observation{"create_error_code": 40, "a": 1}, config.Observation{"create_error_code": 0}, reg, "FAIL"},
	} {
		row := Row{ID: "operations.config.x"}
		JudgeProbe(&row, c.o, c.k, c.reg, run)
		if row.Verdict != c.verdict {
			t.Errorf("%s: %s (%s)", c.name, row.Verdict, row.Detail)
		}
	}
}

// Every probe has an oracle precondition, and each precondition rejects an observation where
// Kafka accepted what it must refuse (both brokers equally wrong must not PASS).
func TestEveryProbeHasAnOraclePrecondition(t *testing.T) {
	for name := range config.Registry {
		if config.Expected[name] == nil {
			t.Errorf("probe %s has no oracle precondition", name)
		}
	}
	accepted := config.Observation{"create_error_code": 0, "init_error_code": 0, "produce_error_code": 0, "partitions": 1}
	for _, name := range []string{"unknown-topic-key", "min-insync-zero", "txn-timeout-too-big"} {
		if config.Expected[name](accepted) {
			t.Errorf("%s: a Kafka that accepted the request passes the precondition", name)
		}
	}
	if config.Expected["topic-defaults"](config.Observation{"retention.ms": "absent"}) {
		t.Error("topic-defaults: a Kafka that reported nothing passes the precondition")
	}
}
