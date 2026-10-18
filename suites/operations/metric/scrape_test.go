package metric

import (
	"math"
	"os"
	"strings"
	"testing"
)

func TestParsePrometheus(t *testing.T) {
	in := `# HELP kubemq_kafka_messages_total Records.
# TYPE kubemq_kafka_messages_total counter
kubemq_kafka_messages_total{topic="a,b",direction="in"} 42
kubemq_kafka_messages_total{topic="q\"x\\y",direction="in",} 1.5e3 1696000000000
kubemq_up 1
kubemq_lat_bucket{le="+Inf"} 7
kubemq_gauge NaN
kubemq_neg -Inf
`
	sc, err := ParsePrometheus(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(sc) != 6 {
		t.Fatalf("got %d series: %+v", len(sc), sc)
	}
	if sc[0].Labels["topic"] != "a,b" || sc[0].Value != 42 {
		t.Errorf("comma in label value: %+v", sc[0])
	}
	if sc[1].Labels["topic"] != `q"x\y` || sc[1].Value != 1500 {
		t.Errorf("escapes or timestamp: %+v", sc[1])
	}
	if sc[2].Name != "kubemq_up" || len(sc[2].Labels) != 0 {
		t.Errorf("bare series: %+v", sc[2])
	}
	if sc[3].Labels["le"] != "+Inf" || !math.IsNaN(sc[4].Value) || !math.IsInf(sc[5].Value, -1) {
		t.Errorf("special values: %+v", sc[3:])
	}
	got := sc.Select(Selector{Name: "kubemq_kafka_messages_total", Labels: map[string]string{"direction": "in"}})
	if len(got) != 2 {
		t.Errorf("selector without topic must match both topics: %v", got)
	}
}

func TestParsePrometheusRejectsGarbage(t *testing.T) {
	for _, in := range []string{
		`novalue`,
		`m{a="1" 3`,
		`m{a=1} 3`,
		`m{a="1"} three`,
		`m 1 2 3`,
	} {
		if _, err := ParsePrometheus(strings.NewReader(in)); err == nil {
			t.Errorf("accepted %q", in)
		}
	}
}

// testdata/kafka-jmx-4.3.1.csv is real output of kafka-jmx.sh from the pinned Kafka 4.3.1 image
// (trimmed to the lines below), captured 2026-10-07 on a throwaway single-node container.
func TestParseJmxCSV(t *testing.T) {
	f, err := os.Open("testdata/kafka-jmx-4.3.1.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc, err := ParseJmxCSV(f)
	if err != nil {
		t.Fatal(err)
	}
	one := func(sel Selector) float64 {
		t.Helper()
		got := sc.Select(sel)
		if len(got) != 1 {
			t.Fatalf("%+v: want one series, got %v", sel, got)
		}
		for _, v := range got {
			return v
		}
		return 0
	}
	if v := one(Selector{Name: "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:Count", Labels: map[string]string{"topic": "t1"}}); v != 50 {
		t.Errorf("per-topic messages in: %v", v)
	}
	if got := sc.Select(Selector{Name: "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:Count"}); len(got) != 2 {
		t.Errorf("selector without topic must match the per-topic and the all-topics MBean: %v", got)
	}
	if v := one(Selector{Name: "kafka.network:type=RequestMetrics,name=ErrorsPerSec:Count", Labels: map[string]string{"request": "Produce", "error": "MESSAGE_TOO_LARGE"}}); v != 1 {
		t.Errorf("errors by code: %v", v)
	}
	if v := one(Selector{Name: "kafka.log:type=Log,name=LogEndOffset:Value", Labels: map[string]string{"topic": "t1", "partition": "0"}}); v != 50 {
		t.Errorf("log end offset: %v", v)
	}
	if v := one(Selector{Name: "kafka.controller:type=KafkaController,name=ActiveControllerCount:Value"}); v != 1 {
		t.Errorf("active controller: %v", v)
	}
	if v := one(Selector{Name: "java.lang:type=OperatingSystem:OpenFileDescriptorCount"}); v != 169 {
		t.Errorf("open files: %v", v)
	}
	if got := sc.Select(Selector{Name: "kafka.server:type=socket-server-metrics:connection-count", Labels: map[string]string{"listener": "HOST"}}); len(got) != 3 {
		t.Errorf("connection-count per network processor: %v", got)
	}
	for _, s := range sc {
		if strings.HasSuffix(s.Name, ":EventType") || strings.HasSuffix(s.Name, ":Arch") || strings.HasSuffix(s.Name, ":ObjectName") {
			t.Errorf("non-numeric attribute kept: %+v", s)
		}
	}
}

func TestParseJmxCSVBannerAndRepeats(t *testing.T) {
	in := "Trying to connect to JMX url: service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi.\n\n" +
		"time,\"1\"\nd:type=T,name=N,topic=a:Count,\"5\"\nd:type=T,name=N,topic=a:Count,\"9\"\n"
	sc, err := ParseJmxCSV(strings.NewReader(in))
	if err != nil || len(sc) != 1 || sc[0].Value != 9 || sc[0].Name != "d:type=T,name=N:Count" {
		t.Fatalf("%+v %v", sc, err)
	}
}

func TestParseJmxCSVRejectsGarbage(t *testing.T) {
	for _, in := range []string{
		"",
		"Trying to connect\n",
		"\"time\",\"d:type=T:Count\"\n1,2\n", // the wide layout this kafka-jmx.sh does not print
		"time,\"1\"\nnoattribute,\"2\"\n",
		"time,\"1\"\nd:type=T,bad:Count,\"2\"\n",
	} {
		if _, err := ParseJmxCSV(strings.NewReader(in)); err == nil {
			t.Errorf("accepted %q", in)
		}
	}
}

func TestBind(t *testing.T) {
	s := Selector{Name: "m", Labels: map[string]string{"topic": "${topic}", "group": "g-${group}"}}.Bind(map[string]string{"topic": "t9", "group": "x"})
	if s.Labels["topic"] != "t9" || s.Labels["group"] != "g-x" {
		t.Fatalf("%+v", s)
	}
}
