package probe

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseJavaReadBack(t *testing.T) {
	var compacted strings.Builder
	for k := 1; k < 10; k++ {
		fmt.Fprintf(&compacted, "k%d|v5-payload\n", k)
	}
	compacted.WriteString("roll|1\nroll|2\n")
	idle := "[2026-10-07 10:15:04,253] ERROR Error processing message, terminating consumer process:  (org.apache.kafka.tools.consumer.ConsoleConsumer)\norg.apache.kafka.common.errors.TimeoutException\nProcessed a total of 11 messages\n"

	good := parseJavaReadBack([]byte(compacted.String()), []byte(idle), nil)
	if good["java_consumer_ended_cleanly"] != true || good["java_records_read"] != 9 || good["java_latest_value_per_key"] != true ||
		good["java_tombstoned_key_absent"] != true || len(good["java_exceptions"].([]string)) != 0 {
		t.Errorf("compacted topic read cleanly: %v", good)
	}

	uncompacted := compacted.String() + "k1|v4-payload\nk0|<tombstone>\n"
	if o := parseJavaReadBack([]byte(uncompacted), []byte(idle), nil); o["java_latest_value_per_key"] != false || o["java_tombstoned_key_absent"] != false || o["java_records_read"] != 11 {
		t.Errorf("duplicate key and tombstone must show: %v", o)
	}

	corrupt := "[2026-10-07 10:15:04,253] ERROR Error processing message (org.apache.kafka.tools.consumer.ConsoleConsumer)\norg.apache.kafka.common.KafkaException: Received exception when fetching the next record\nCaused by: org.apache.kafka.common.errors.CorruptRecordException: Record is corrupt\n\tat org.apache.kafka.clients.consumer.internals.CompletedFetch.maybeEnsureValid(CompletedFetch.java:1)\nProcessed a total of 3 messages\n"
	o := parseJavaReadBack([]byte("k1|v5-payload\n"), []byte(corrupt), errors.New("exit status 1"))
	exc := o["java_exceptions"].([]string)
	if o["java_consumer_ended_cleanly"] != false || len(exc) != 2 || exc[0] != "org.apache.kafka.common.KafkaException" || exc[1] != "org.apache.kafka.common.errors.CorruptRecordException" {
		t.Errorf("a decode failure must be reported: %v", o)
	}

	if o := parseJavaReadBack(nil, []byte("Error: image not found\n"), nil); o["java_consumer_ended_cleanly"] != false {
		t.Errorf("no summary line means the consumer never ran: %v", o)
	}
}
