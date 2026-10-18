package mirror

import (
	"strings"
	"testing"
)

func TestPropertiesOneFlowOverridable(t *testing.T) {
	p := Properties(Spec{SrcAlias: "a", TgtAlias: "b", Src: Cluster{Advertise: "s:9092"}, Tgt: Cluster{Advertise: "t:9092"},
		Topics: "t1", Groups: QuoteGroup("g.1"), Extra: map[string]string{"offset.lag.max": "100", "sync.topic.acls.enabled": "false"}})
	for _, want := range []string{"a->b.enabled = true\n", "b->a.enabled = false\n", "a.bootstrap.servers = s:9092\n", "b.bootstrap.servers = t:9092\n",
		"a->b.topics = t1\n", "a->b.groups = g\\.1\n", "a->b.offset-syncs.topic.location = target\n", "offset.lag.max = 100\n", "sync.topic.acls.enabled = false\n"} {
		if !strings.Contains(p, want) {
			t.Errorf("missing %q in\n%s", want, p)
		}
	}
	if strings.Count(p, "offset.lag.max") != 1 {
		t.Error("an extra property must replace the default, not repeat it")
	}
}

func TestTopicIDPresent(t *testing.T) {
	if !TopicIDPresent("Topic: t\tTopicId: Fz8iEBmCSjqHAb7KuNuXmA\tPartitionCount: 3") {
		t.Error("a real topic id must count")
	}
	if TopicIDPresent("Topic: t\tTopicId: AAAAAAAAAAAAAAAAAAAAAA\tPartitionCount: 3") || TopicIDPresent("Topic: t\tPartitionCount: 3") {
		t.Error("the all-zero id or no id must not count")
	}
}
