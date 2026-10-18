// Package parse reads the summary lines Apache Kafka's own load tools print, so the performance
// baseline (Track H) reports exactly what kafka-producer-perf-test and kafka-consumer-perf-test
// measured.
package parse

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Producer is the final summary line of kafka-producer-perf-test.
type Producer struct {
	Records       int64
	RecordsPerSec float64
	MBPerSec      float64
	AvgMs         float64
	MaxMs         float64
	P50Ms         float64
	P95Ms         float64
	P99Ms         float64
	P999Ms        float64
}

// The final line: "N records sent, X records/sec (Y MB/sec), A ms avg latency, M ms max latency,
// P ms 50th, P ms 95th, P ms 99th, P ms 99.9th." Interim lines lack the percentiles.
var producerLine = regexp.MustCompile(`(\d+) records sent, ([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency, ([\d.]+) ms max latency, (\d+) ms 50th, (\d+) ms 95th, (\d+) ms 99th, (\d+) ms 99\.9th`)

// ParseProducer returns the final summary of a kafka-producer-perf-test run.
func ParseProducer(out string) (Producer, error) {
	m := producerLine.FindAllStringSubmatch(out, -1)
	if len(m) == 0 {
		return Producer{}, fmt.Errorf("no final summary line in producer output")
	}
	f := m[len(m)-1]
	n, _ := strconv.ParseInt(f[1], 10, 64)
	num := func(i int) float64 { v, _ := strconv.ParseFloat(f[i], 64); return v }
	return Producer{Records: n, RecordsPerSec: num(2), MBPerSec: num(3), AvgMs: num(4), MaxMs: num(5),
		P50Ms: num(6), P95Ms: num(7), P99Ms: num(8), P999Ms: num(9)}, nil
}

// Consumer is the result row of kafka-consumer-perf-test.
type Consumer struct {
	Records            int64
	MB                 float64
	MBPerSec           float64
	RecordsPerSec      float64
	FetchMBPerSec      float64 // throughput excluding the group join (rebalance time)
	FetchRecordsPerSec float64
}

// ParseConsumer reads kafka-consumer-perf-test's CSV: a header starting "start.time" and a value
// row. Columns are found by name, so the tool may add columns.
func ParseConsumer(out string) (Consumer, error) {
	lines := strings.Split(out, "\n")
	for i, l := range lines {
		if !strings.HasPrefix(strings.TrimSpace(l), "start.time") || i+1 >= len(lines) {
			continue
		}
		head := splitCSV(l)
		vals := splitCSV(lines[i+1])
		if len(vals) != len(head) {
			return Consumer{}, fmt.Errorf("consumer output: %d columns in the header, %d in the values", len(head), len(vals))
		}
		col := map[string]string{}
		for j, h := range head {
			col[h] = vals[j]
		}
		get := func(k string) (float64, error) {
			v, ok := col[k]
			if !ok {
				return 0, fmt.Errorf("consumer output: no column %s", k)
			}
			return strconv.ParseFloat(v, 64)
		}
		var c Consumer
		var err error
		var n float64
		for _, x := range []struct {
			k string
			p *float64
		}{{"data.consumed.in.MB", &c.MB}, {"MB.sec", &c.MBPerSec}, {"data.consumed.in.nMsg", &n}, {"nMsg.sec", &c.RecordsPerSec}, {"fetch.MB.sec", &c.FetchMBPerSec}, {"fetch.nMsg.sec", &c.FetchRecordsPerSec}} {
			if *x.p, err = get(x.k); err != nil {
				return Consumer{}, err
			}
		}
		c.Records = int64(n)
		return c, nil
	}
	return Consumer{}, fmt.Errorf("no result row in consumer output")
}

func splitCSV(l string) []string {
	parts := strings.Split(strings.TrimSpace(l), ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// Median of xs (the middle value; the lower middle for an even count). 0 for none.
func Median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[(len(s)-1)/2]
}
