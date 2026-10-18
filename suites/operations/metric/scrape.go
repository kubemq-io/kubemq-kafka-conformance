// Package metric holds the operations track's metric-row verifier (Track G).
//
// A metric row maps one Apache Kafka operator metric (a JMX MBean attribute) to the KubeMQ
// Prometheus series an operator would alert on instead. The track scrapes both brokers before
// and after the same scripted workload, and each leg is judged against the row's expectation:
// the series exists, a counter never goes backwards, and the change (or the gauge value) matches
// what the workload must have caused. The oracle leg must meet the expectation, or the row is
// VOID (the mapping or the workload is wrong, not KubeMQ).
//
// Both brokers' scrapes are normalised to the same Series form, so one verifier serves both:
//   - KubeMQ: the Prometheus text exposition of its /metrics endpoint (ParsePrometheus).
//   - Kafka:  the CSV that Kafka's own kafka-jmx.sh (JmxTool) prints (ParseJmxCSV). The MBean
//     domain, type and name plus the attribute become the series name,
//     "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec:Count"; every other ObjectName
//     key (topic, partition, request, listener, ...) becomes a label.
package metric

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Series is one sample: a metric name, its labels and its value.
type Series struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// Scrape is everything one broker exposed at one moment.
type Scrape []Series

// Key is the canonical identity of a series: name{k="v",...} with sorted label keys.
func (s Series) Key() string {
	if len(s.Labels) == 0 {
		return s.Name
	}
	keys := make([]string, 0, len(s.Labels))
	for k := range s.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%q", k, s.Labels[k])
	}
	return s.Name + "{" + strings.Join(parts, ",") + "}"
}

// Selector picks series by exact name and by equality on the listed labels. Labels not listed
// are not constrained, so a selector without labels sums a metric across topics or partitions.
type Selector struct {
	Name   string            `yaml:"name"`
	Labels map[string]string `yaml:"labels,omitempty"`
}

// Matches reports whether s is selected.
func (sel Selector) Matches(s Series) bool {
	if s.Name != sel.Name {
		return false
	}
	for k, v := range sel.Labels {
		if s.Labels[k] != v {
			return false
		}
	}
	return true
}

// Bind replaces ${var} in the selector's label values with the workload's names (topic, group).
func (sel Selector) Bind(vars map[string]string) Selector {
	out := Selector{Name: sel.Name, Labels: map[string]string{}}
	for k, v := range sel.Labels {
		for name, val := range vars {
			v = strings.ReplaceAll(v, "${"+name+"}", val)
		}
		out.Labels[k] = v
	}
	return out
}

// Select returns the selected series keyed by Series.Key.
func (sc Scrape) Select(sel Selector) map[string]float64 {
	out := map[string]float64{}
	for _, s := range sc {
		if sel.Matches(s) {
			out[s.Key()] += s.Value
		}
	}
	return out
}

// ParsePrometheus reads the Prometheus text exposition format (version 0.0.4). Comment and
// TYPE/HELP lines are skipped; an optional trailing timestamp is ignored.
func ParsePrometheus(r io.Reader) (Scrape, error) {
	var out Scrape
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		s, err := parsePromLine(text)
		if err != nil {
			return nil, fmt.Errorf("prometheus line %d: %w", line, err)
		}
		out = append(out, s)
	}
	return out, sc.Err()
}

func parsePromLine(text string) (Series, error) {
	s := Series{Labels: map[string]string{}}
	i := strings.IndexAny(text, "{ \t")
	if i <= 0 {
		return s, fmt.Errorf("no value in %q", text)
	}
	s.Name = text[:i]
	rest := text[i:]
	if rest[0] == '{' {
		n, err := parsePromLabels(rest, s.Labels)
		if err != nil {
			return s, err
		}
		rest = rest[n:]
	}
	fields := strings.Fields(rest)
	if len(fields) < 1 || len(fields) > 2 {
		return s, fmt.Errorf("want value [timestamp] after %s, got %q", s.Name, rest)
	}
	v, err := parsePromValue(fields[0])
	if err != nil {
		return s, err
	}
	s.Value = v
	return s, nil
}

// parsePromLabels parses {k="v",...} at the start of text into labels and returns the number of
// bytes consumed, including the closing brace.
func parsePromLabels(text string, labels map[string]string) (int, error) {
	i := 1
	for {
		for i < len(text) && (text[i] == ' ' || text[i] == ',') {
			i++
		}
		if i >= len(text) {
			return 0, fmt.Errorf("unterminated label set in %q", text)
		}
		if text[i] == '}' {
			return i + 1, nil
		}
		eq := strings.IndexByte(text[i:], '=')
		if eq < 0 {
			return 0, fmt.Errorf("label without value in %q", text)
		}
		key := strings.TrimSpace(text[i : i+eq])
		i += eq + 1
		if i >= len(text) || text[i] != '"' {
			return 0, fmt.Errorf("label %s value not quoted in %q", key, text)
		}
		i++
		var b strings.Builder
		for {
			if i >= len(text) {
				return 0, fmt.Errorf("unterminated label value for %s in %q", key, text)
			}
			c := text[i]
			if c == '\\' && i+1 < len(text) {
				switch text[i+1] {
				case 'n':
					b.WriteByte('\n')
				default:
					b.WriteByte(text[i+1])
				}
				i += 2
				continue
			}
			if c == '"' {
				i++
				break
			}
			b.WriteByte(c)
			i++
		}
		labels[key] = b.String()
	}
}

func parsePromValue(v string) (float64, error) {
	switch v {
	case "+Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	case "NaN":
		return math.NaN(), nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("bad sample value %q", v)
	}
	return f, nil
}

// ParseJmxCSV reads the output of `kafka-jmx.sh --report-format csv --one-time true` as Kafka
// 4.3.1 prints it (captured in testdata/kafka-jmx-4.3.1.csv): one line per attribute,
//
//	time,"1791358569840"
//	kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=t1:Count,"50"
//
// The key is not quoted and contains commas, so a line splits at its last `,"`. Lines that are
// not of that shape (the tool's "Trying to connect" banner, blank lines) and non-numeric values
// (EventType, RateUnit, Arch) are skipped. If an attribute appears twice, the last value wins.
func ParseJmxCSV(r io.Reader) (Scrape, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	sawTime := false
	byKey := map[string]int{}
	var out Scrape
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		i := strings.LastIndex(line, ",\"")
		if i <= 0 || !strings.HasSuffix(line, "\"") || len(line) < i+3 {
			continue
		}
		key, raw := line[:i], line[i+2:len(line)-1]
		if key == "time" {
			sawTime = true
			continue
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			continue
		}
		s, err := jmxSeries(key)
		if err != nil {
			return nil, err
		}
		s.Value = v
		if j, ok := byKey[s.Key()]; ok {
			out[j] = s
			continue
		}
		byKey[s.Key()] = len(out)
		out = append(out, s)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if !sawTime {
		return nil, fmt.Errorf("jmx csv: no time line; not kafka-jmx.sh --report-format csv output")
	}
	return out, nil
}

// jmxSeries turns "kafka.server:type=X,name=Y,topic=t:Count" into Series name
// "kafka.server:type=X,name=Y:Count" with label topic=t.
func jmxSeries(col string) (Series, error) {
	c := strings.IndexByte(col, ':')
	a := strings.LastIndexByte(col, ':')
	if c <= 0 || a <= c {
		return Series{}, fmt.Errorf("jmx csv: column %q is not <ObjectName>:<attribute>", col)
	}
	domain, props, attr := col[:c], col[c+1:a], col[a+1:]
	s := Series{Labels: map[string]string{}}
	var typ, name string
	for _, kv := range strings.Split(props, ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return Series{}, fmt.Errorf("jmx csv: column %q: bad key property %q", col, kv)
		}
		switch k {
		case "type":
			typ = v
		case "name":
			name = v
		default:
			s.Labels[k] = v
		}
	}
	s.Name = domain + ":type=" + typ
	if name != "" {
		s.Name += ",name=" + name
	}
	s.Name += ":" + attr
	return s, nil
}
