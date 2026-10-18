package metric

// Verifier mutation tests for the operations track (docs/methodology.md): each injects one
// defect into a known-good pair of scrapes and proves the verifier reports it. Reorder has no
// meaning for a counter; its place is taken by a counter reset and a vanished series.

import (
	"strings"
	"testing"
)

const kafkaIn = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:Count"

func scrape(lines ...string) Scrape {
	sc, err := ParsePrometheus(strings.NewReader(strings.Join(lines, "\n")))
	if err != nil {
		panic(err)
	}
	return sc
}

var workload = map[string]float64{"records": 500, "payload_bytes": 512000, "partitions": 3, "consumed": 200, "lag": 300}

type mutCase struct {
	sel           Selector
	exp           Expect
	before, after Scrape
	mutations     map[string]Scrape // mutated "after" scrapes
}

func cases() map[string]mutCase {
	topic := Selector{Name: "kubemq_kafka_messages_total", Labels: map[string]string{"topic": "t1", "direction": "in"}}
	return map[string]mutCase{
		"exact counter (messages in)": {
			sel: topic, exp: Expect{Kind: DeltaExact, Of: "records"},
			before: scrape(`kubemq_kafka_messages_total{topic="t1",direction="in"} 100`, `kubemq_kafka_messages_total{topic="t2",direction="in"} 7`),
			after:  scrape(`kubemq_kafka_messages_total{topic="t1",direction="in"} 600`, `kubemq_kafka_messages_total{topic="t2",direction="in"} 9`),
			mutations: map[string]Scrape{
				"loss (one record not counted)":        scrape(`kubemq_kafka_messages_total{topic="t1",direction="in"} 599`),
				"duplicate (one record counted twice)": scrape(`kubemq_kafka_messages_total{topic="t1",direction="in"} 601`),
				"counter reset":                        scrape(`kubemq_kafka_messages_total{topic="t1",direction="in"} 50`),
				"stale counter":                        scrape(`kubemq_kafka_messages_total{topic="t1",direction="in"} 100`),
				"series renamed":                       scrape(`kubemq_kafka_messages_in_total{topic="t1",direction="in"} 600`),
				"wrong topic label":                    scrape(`kubemq_kafka_messages_total{topic="t2",direction="in"} 600`),
				"wrong direction label":                scrape(`kubemq_kafka_messages_total{topic="t1",direction="out"} 600`),
			},
		},
		"range counter (bytes in)": {
			sel:    Selector{Name: "kubemq_kafka_bytes_total", Labels: map[string]string{"direction": "in"}},
			exp:    Expect{Kind: DeltaRange, Of: "payload_bytes", Min: 1, Max: 1.5, Slack: 65536},
			before: scrape(`kubemq_kafka_bytes_total{direction="in",partition="0"} 10`, `kubemq_kafka_bytes_total{direction="in",partition="1"} 0`),
			after:  scrape(`kubemq_kafka_bytes_total{direction="in",partition="0"} 300010`, `kubemq_kafka_bytes_total{direction="in",partition="1"} 300000`),
			mutations: map[string]Scrape{
				"bytes lost":                  scrape(`kubemq_kafka_bytes_total{direction="in",partition="0"} 100010`, `kubemq_kafka_bytes_total{direction="in",partition="1"} 300000`),
				"bytes counted far too often": scrape(`kubemq_kafka_bytes_total{direction="in",partition="0"} 900010`, `kubemq_kafka_bytes_total{direction="in",partition="1"} 300000`),
				"one partition vanished":      scrape(`kubemq_kafka_bytes_total{direction="in",partition="0"} 600010`),
			},
		},
		"error counter by code (wrong error code)": {
			sel:    Selector{Name: "kubemq_kafka_errors_total", Labels: map[string]string{"code": "10"}},
			exp:    Expect{Kind: DeltaExact, Of: "partitions"},
			before: scrape(`kubemq_kafka_errors_total{code="10"} 0`),
			after:  scrape(`kubemq_kafka_errors_total{code="10"} 3`),
			mutations: map[string]Scrape{
				"error counted under code 0": scrape(`kubemq_kafka_errors_total{code="10"} 0`, `kubemq_kafka_errors_total{code="0"} 3`),
				"error counted under code 2": scrape(`kubemq_kafka_errors_total{code="2"} 3`),
			},
		},
		"gauge (consumer lag)": {
			sel:    Selector{Name: "kubemq_kafka_consumer_group_lag", Labels: map[string]string{"group": "g1"}},
			exp:    Expect{Kind: GaugeEquals, Of: "lag"},
			before: scrape(),
			after:  scrape(`kubemq_kafka_consumer_group_lag{group="g1",partition="0"} 100`, `kubemq_kafka_consumer_group_lag{group="g1",partition="1"} 200`),
			mutations: map[string]Scrape{
				"lag off by one (wrong offset)": scrape(`kubemq_kafka_consumer_group_lag{group="g1",partition="0"} 101`, `kubemq_kafka_consumer_group_lag{group="g1",partition="1"} 200`),
				"lag of one partition missing":  scrape(`kubemq_kafka_consumer_group_lag{group="g1",partition="1"} 200`),
				"lag reported for other group":  scrape(`kubemq_kafka_consumer_group_lag{group="g2",partition="0"} 300`),
			},
		},
		"gauge equals constant (under-replicated partitions)": {
			sel:    Selector{Name: "kubemq_cluster_under_replicated"},
			exp:    Expect{Kind: GaugeEquals, Value: 0},
			before: scrape(`kubemq_cluster_under_replicated 0`),
			after:  scrape(`kubemq_cluster_under_replicated 0`),
			mutations: map[string]Scrape{
				"one partition under-replicated": scrape(`kubemq_cluster_under_replicated 1`),
				"series absent":                  scrape(),
			},
		},
		"positive counter (produce requests)": {
			sel:    Selector{Name: "kubemq_kafka_requests_total", Labels: map[string]string{"api": "Produce"}},
			exp:    Expect{Kind: DeltaPositive},
			before: scrape(`kubemq_kafka_requests_total{api="Produce"} 5`),
			after:  scrape(`kubemq_kafka_requests_total{api="Produce"} 17`),
			mutations: map[string]Scrape{
				"no request counted":    scrape(`kubemq_kafka_requests_total{api="Produce"} 5`),
				"counted under Fetch":   scrape(`kubemq_kafka_requests_total{api="Produce"} 5`, `kubemq_kafka_requests_total{api="Fetch"} 12`),
				"counter went to NaN":   scrape(`kubemq_kafka_requests_total{api="Produce"} NaN`),
				"counter reset to zero": scrape(`kubemq_kafka_requests_total{api="Produce"} 0`),
			},
		},
	}
}

func TestMutation_CheckDetectsEveryDefect(t *testing.T) {
	for name, c := range cases() {
		good, err := Check(c.before, c.after, c.sel, c.exp, workload)
		if err != nil || !good.Met || !good.Present || !good.Monotonic {
			t.Fatalf("%s: control must pass: %+v %v", name, good, err)
		}
		if v, _, _ := Compare(good, &good); v != "PASS" {
			t.Fatalf("%s: identical legs must PASS, got %s", name, v)
		}
		for mname, after := range c.mutations {
			bad, err := Check(c.before, after, c.sel, c.exp, workload)
			if err != nil {
				t.Fatalf("%s / %s: %v", name, mname, err)
			}
			if bad.Met {
				t.Errorf("%s: verifier stayed green under %q: %+v", name, mname, bad)
			}
			// As the KubeMQ leg the defect must be a difference; as the oracle leg, VOID.
			if v, dev, _ := Compare(good, &bad); v != "DIFF" || dev == "" {
				t.Errorf("%s / %s as KubeMQ leg: got %s %q", name, mname, v, dev)
			}
			if v, _, _ := Compare(bad, &good); v != "VOID" {
				t.Errorf("%s / %s as oracle leg: got %s, want VOID", name, mname, v)
			}
		}
	}
}

func TestMutation_NoKubeMQEquivalentNeverPasses(t *testing.T) {
	c := cases()["exact counter (messages in)"]
	good, _ := Check(c.before, c.after, c.sel, c.exp, workload)
	v, dev, _ := Compare(good, nil)
	if v != "DIFF" || dev != "series_present: Kafka true, KubeMQ absent" {
		t.Fatalf("got %s %q", v, dev)
	}
}

func TestDeviationTextIsExact(t *testing.T) {
	c := cases()["exact counter (messages in)"]
	good, _ := Check(c.before, c.after, c.sel, c.exp, workload)
	bad, _ := Check(c.before, c.mutations["counter reset"], c.sel, c.exp, workload)
	_, dev, _ := Compare(good, &bad)
	want := "counter_monotonic: Kafka true, KubeMQ false; expectation_met: Kafka true, KubeMQ false"
	if dev != want {
		t.Fatalf("deviation %q, want %q", dev, want)
	}
}

func TestCheckRejectsBadInput(t *testing.T) {
	sel := Selector{Name: "x"}
	for name, e := range map[string]Expect{
		"unknown kind":        {Kind: "delta"},
		"exact without of":    {Kind: DeltaExact},
		"range without max":   {Kind: DeltaRange, Of: "records", Min: 1},
		"range min above max": {Kind: DeltaRange, Of: "records", Min: 2, Max: 1},
		"gauge min above max": {Kind: GaugeRange, Min: 2, Max: 1},
		"unrecorded quantity": {Kind: DeltaExact, Of: "nothing"},
	} {
		if _, err := Check(nil, nil, sel, e, workload); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
