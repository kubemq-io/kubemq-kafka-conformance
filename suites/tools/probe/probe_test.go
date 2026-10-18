package probe

import (
	"testing"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/box"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
)

func TestCommittedSumAndList(t *testing.T) {
	rows := kout.ParseGroupDescribe(groupDescribeFixture, "g1")
	if committedSum(rows) != 16 || committedList(rows) != "0=12 1=4 2=-1" {
		t.Errorf("sum %d list %s", committedSum(rows), committedList(rows))
	}
}

const groupDescribeFixture = `GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
g1              t1              0          12              12              0               -               -               -
g1              t1              1          4               4               0               -               -               -
g1              t1              2          -               5               -               -               -               -
`

func TestException(t *testing.T) {
	r := box.Result{Stdout: "Error while executing config command\njava.util.concurrent.ExecutionException: org.apache.kafka.common.errors.InvalidConfigurationException: Unknown topic config name: x\n"}
	if got := box.Exception(r); got != "InvalidConfigurationException" {
		t.Errorf("got %q", got)
	}
	r = box.Result{Stderr: "[ERROR] java.lang.IllegalArgumentException: Topic 'nope' does not exist as expected"}
	if got := box.Exception(r); got != "IllegalArgumentException" {
		t.Errorf("got %q", got)
	}
	if box.Exception(box.Result{Stdout: "Created topic t1."}) != "" {
		t.Error("no exception must be empty")
	}
}

func TestOffsetCommitsIn(t *testing.T) {
	out := `{"key":{"type":1,"data":{"group":"g1","topic":"t1","partition":1}},"value":{"version":4,"data":{"offset":3,"leaderEpoch":0}}}` +
		`{"key":{"type":1,"data":{"group":"g1","topic":"t1","partition":0}},"value":{"version":4,"data":{"offset":7,"leaderEpoch":0}}}` +
		`{"key":{"type":1,"data":{"group":"g1","topic":"t1","partition":0}},"value":{"version":4,"data":{"offset":9,"leaderEpoch":0}}}` +
		`{"key":{"type":1,"data":{"group":"g10","topic":"t1","partition":2}},"value":{"version":4,"data":{"offset":5}}}`
	if got := kout.OffsetList(OffsetCommitsIn(out, "g1", "t1")); got != "0=9 1=3" {
		t.Errorf("last commit per partition, own group only: %s", got)
	}
}

func TestExporterLag(t *testing.T) {
	page := "# HELP kafka_consumergroup_group_lag lag\n" +
		`kafka_consumergroup_group_lag{cluster_name="kcert",group="lg",topic="lt",partition="2",member_host="unknown",consumer_id="unknown",client_id="unknown",} 0.0` + "\n" +
		`kafka_consumergroup_group_lag{cluster_name="kcert",group="lg",topic="lt",partition="1",member_host="unknown",consumer_id="unknown",client_id="unknown",} 10.0` + "\n" +
		`kafka_consumergroup_group_lag{cluster_name="kcert",group="lg",topic="other",partition="0",} 7.0` + "\n" +
		`kafka_consumergroup_group_lag{cluster_name="kcert",group="lg2",topic="lt",partition="0",} 9.0` + "\n" +
		`kafka_consumergroup_group_lag_seconds{cluster_name="kcert",group="lg",topic="lt",partition="0",} 3.0` + "\n"
	if got := kout.OffsetList(ExporterLag(page, "lg", "lt")); got != "1=10 2=0" {
		t.Errorf("own group and topic only, lag series only: %s", got)
	}
}
