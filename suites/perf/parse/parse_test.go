package parse

import "testing"

func TestParseProducer(t *testing.T) {
	out := `61523 records sent, 12302.1 records/sec (12.01 MB/sec), 1450.2 ms avg latency, 2101.0 ms max latency.
300000 records sent, 59988.002400 records/sec (58.58 MB/sec), 412.53 ms avg latency, 2101.00 ms max latency, 380 ms 50th, 901 ms 95th, 1250 ms 99th, 1900 ms 99.9th.
`
	p, err := ParseProducer(out)
	if err != nil || p.Records != 300000 || p.P99Ms != 1250 || p.P50Ms != 380 || p.MBPerSec != 58.58 {
		t.Fatalf("%+v %v", p, err)
	}
	if _, err := ParseProducer("61523 records sent, 12302.1 records/sec (12.01 MB/sec), 1450.2 ms avg latency, 2101.0 ms max latency.\n"); err == nil {
		t.Fatal("an interim line is not a final summary")
	}
}

func TestParseConsumer(t *testing.T) {
	out := `WARNING: something
start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
2026-10-07 15:00:00:000, 2026-10-07 15:00:10:000, 292.9688, 29.2969, 300000, 30000.0000, 3012, 6988, 41.9254, 42930.7384
`
	c, err := ParseConsumer(out)
	if err != nil || c.Records != 300000 || c.MBPerSec != 29.2969 || c.FetchMBPerSec != 41.9254 || c.FetchRecordsPerSec != 42930.7384 {
		t.Fatalf("%+v %v", c, err)
	}
	if _, err := ParseConsumer("start.time, end.time\n1, 2, 3\n"); err == nil {
		t.Fatal("column count mismatch accepted")
	}
	if _, err := ParseConsumer("nothing"); err == nil {
		t.Fatal("no result accepted")
	}
}

func TestMedian(t *testing.T) {
	if Median([]float64{3, 1, 2}) != 2 || Median([]float64{4, 1, 3, 2}) != 2 || Median(nil) != 0 {
		t.Fatal("median")
	}
}
