// Package continuity judges whether KubeMQ's operator metrics stay usable while a 3-node cluster
// is upgraded one pod at a time under load (Track G, the rolling-upgrade row the cluster track
// runs). The cluster track scrapes every KubeMQ node's /metrics on a fixed interval during the
// upgrade and records each pod's start time; this package reads those scrapes and reports what an
// operator watching dashboards would have seen break. It needs no broker.
//
// The cluster track records a failed scrape by writing no file for it. Checks, per scrape round
// (one interval; one scrape per node per round, the last wins):
//   - every node answers every round, except around its own restart (at most MaxGap consecutive
//     rounds without an answer otherwise), and at least one scrape after its restart;
//   - kubemq_process_start_time_seconds changes on each restarted node, and on no other node;
//   - kubemq_kafka_messages_total{op="produced"} never goes backwards on a node that did not
//     restart in between (a reset at a node's own restart is reported as a note);
//   - kubemq_kafka_consumer_group_lag is exported by some node in every round once it first
//     appeared (at most MaxGap consecutive rounds without it, while the group coordinator moves);
//   - kubemq_cluster_leader sums to 1 over the nodes in every round (at most MaxGap consecutive
//     rounds otherwise, during a leader election).
package continuity

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/operations/metric"
)

// Metric names the checks read.
const (
	StartTime = "kubemq_process_start_time_seconds"
	Messages  = "kubemq_kafka_messages_total"
	Lag       = "kubemq_kafka_consumer_group_lag"
	Leader    = "kubemq_cluster_leader"
)

// MaxGap is how many consecutive rounds a cluster-wide series may be missing or inconsistent.
const MaxGap = 2

// Scrape is one node's /metrics at one moment.
type Scrape struct {
	At     time.Time
	Series metric.Scrape
}

// Restart is a pod (re)start: the pod's start time.
type Restart struct {
	Node string
	At   time.Time
}

// Result is the judgement. Problems empty means the metrics stayed continuous.
type Result struct {
	Problems []string
	Notes    []string
}

// LoadDir reads <node>-<unix seconds>.prom files (Prometheus text format) into scrapes per node,
// sorted by time. Node names may contain dashes; the timestamp is the part after the last one.
func LoadDir(dir string) (map[string][]Scrape, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.prom"))
	if err != nil {
		return nil, err
	}
	out := map[string][]Scrape{}
	for _, f := range files {
		base := strings.TrimSuffix(filepath.Base(f), ".prom")
		i := strings.LastIndexByte(base, '-')
		if i <= 0 {
			return nil, fmt.Errorf("%s: want <node>-<unix seconds>.prom", f)
		}
		sec, err := strconv.ParseInt(base[i+1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: want <node>-<unix seconds>.prom", f)
		}
		r, err := os.Open(f)
		if err != nil {
			return nil, err
		}
		sc, err := metric.ParsePrometheus(r)
		r.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		node := base[:i]
		out[node] = append(out[node], Scrape{At: time.Unix(sec, 0).UTC(), Series: sc})
	}
	for n := range out {
		sort.Slice(out[n], func(a, b int) bool { return out[n][a].At.Before(out[n][b].At) })
	}
	return out, nil
}

func value(sc metric.Scrape, sel metric.Selector) (float64, bool) {
	m := sc.Select(sel)
	if len(m) == 0 {
		return 0, false
	}
	var v float64
	for _, x := range m {
		v += x
	}
	return v, true
}

// Judge applies every check. interval is the scrape interval (rounds are interval wide).
func Judge(scrapes map[string][]Scrape, restarts []Restart, interval time.Duration) Result {
	var r Result
	bad := func(format string, a ...any) { r.Problems = append(r.Problems, fmt.Sprintf(format, a...)) }
	note := func(format string, a ...any) { r.Notes = append(r.Notes, fmt.Sprintf(format, a...)) }
	// Work on time-sorted copies: callers need not sort.
	sorted := map[string][]Scrape{}
	for n, ss := range scrapes {
		c := append([]Scrape(nil), ss...)
		sort.SliceStable(c, func(i, j int) bool { return c[i].At.Before(c[j].At) })
		sorted[n] = c
	}
	scrapes = sorted
	nodes := make([]string, 0, len(scrapes))
	for n := range scrapes {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	if len(nodes) == 0 {
		bad("no scrapes")
		return r
	}
	restartsOf := map[string][]time.Time{}
	for _, x := range restarts {
		restartsOf[x.Node] = append(restartsOf[x.Node], x.At)
	}
	for n := range restartsOf {
		if _, ok := scrapes[n]; !ok {
			bad("node %s restarted but was never scraped", n)
		}
	}
	restartedBetween := func(node string, a, b time.Time) bool {
		for _, t := range restartsOf[node] {
			if t.After(a) && !t.After(b) {
				return true
			}
		}
		return false
	}

	// Per node: answered after its restart, start time changes only with a restart, the produced
	// counter never goes backwards without a restart.
	for _, n := range nodes {
		ss := scrapes[n]
		for _, t := range restartsOf[n] {
			if !ss[len(ss)-1].At.After(t) {
				bad("node %s: no scrape answered after its restart at %s", n, t.Format(time.RFC3339))
			}
		}
		var prevStart, prevMsg float64
		var prevStartAt, prevMsgAt time.Time
		var havePrevStart, havePrevMsg bool
		changed := 0
		for i, s := range ss {
			// Each comparison is against the last scrape that carried the series, so a series
			// missing for a while does not hide a restart in between.
			restarted := i > 0 && restartedBetween(n, ss[i-1].At, s.At)
			if st, ok := value(s.Series, metric.Selector{Name: StartTime}); ok {
				if havePrevStart && st != prevStart {
					changed++
					if !restartedBetween(n, prevStartAt, s.At) {
						bad("node %s: process start time changed at %s without a recorded restart", n, s.At.Format(time.RFC3339))
					}
				}
				prevStart, prevStartAt, havePrevStart = st, s.At, true
			} else {
				bad("node %s: %s missing at %s", n, StartTime, s.At.Format(time.RFC3339))
			}
			if v, ok := value(s.Series, metric.Selector{Name: Messages, Labels: map[string]string{"op": "produced"}}); ok {
				if havePrevMsg && v < prevMsg {
					if restartedBetween(n, prevMsgAt, s.At) {
						note("node %s: %s{op=\"produced\"} reset from %.0f to %.0f at its restart", n, Messages, prevMsg, v)
					} else {
						bad("node %s: %s{op=\"produced\"} went backwards from %.0f to %.0f at %s without a restart", n, Messages, prevMsg, v, s.At.Format(time.RFC3339))
					}
				}
				prevMsg, prevMsgAt, havePrevMsg = v, s.At, true
			} else if restarted {
				// A counter may be created on first use after a fresh process start.
				note("node %s: %s{op=\"produced\"} not yet exported in the first scrape after its restart", n, Messages)
			} else {
				bad("node %s: %s{op=\"produced\"} missing at %s", n, Messages, s.At.Format(time.RFC3339))
			}
		}
		if len(restartsOf[n]) > 0 && changed == 0 {
			bad("node %s: restart recorded but its process start time never changed in the scrapes", n)
		}
	}

	// Rounds: one scrape per node per interval (the last one wins if a node was scraped twice).
	// A failed scrape leaves no file, so a missing round is a node the operator could not see.
	start := scrapes[nodes[0]][0].At
	for _, n := range nodes {
		if scrapes[n][0].At.Before(start) {
			start = scrapes[n][0].At
		}
	}
	roundOf := func(t time.Time) int64 { return int64(t.Sub(start) / interval) }
	byNode := map[string]map[int64]Scrape{}
	var last int64
	for _, n := range nodes {
		byNode[n] = map[int64]Scrape{}
		for _, s := range scrapes[n] {
			k := roundOf(s.At)
			byNode[n][k] = s
			if k > last {
				last = k
			}
		}
	}
	for _, n := range nodes {
		restartRounds := map[int64]bool{}
		for _, t := range restartsOf[n] {
			k := roundOf(t)
			for j := k - 1; j <= k+MaxGap; j++ {
				restartRounds[j] = true
			}
		}
		missing := 0
		for k := int64(0); k <= last; k++ {
			if _, ok := byNode[n][k]; ok || restartRounds[k] {
				missing = 0
				continue
			}
			missing++
			if missing == MaxGap+1 {
				bad("node %s answered no metrics scrape for more than %d consecutive rounds (from round %d) outside its own restart", n, MaxGap, k-MaxGap)
			}
		}
	}
	type round struct {
		lag    bool
		leader float64
		seen   bool
	}
	rounds := map[int64]*round{}
	for _, n := range nodes {
		for k, s := range byNode[n] {
			rd := rounds[k]
			if rd == nil {
				rd = &round{}
				rounds[k] = rd
			}
			rd.seen = true
			if _, ok := value(s.Series, metric.Selector{Name: Lag}); ok {
				rd.lag = true
			}
			if v, ok := value(s.Series, metric.Selector{Name: Leader}); ok {
				rd.leader += v
			}
		}
	}
	lagSeen, lagGap, leaderGap := false, 0, 0
	for k := int64(0); k <= last; k++ {
		rd := rounds[k]
		if rd == nil || !rd.seen {
			continue
		}
		if rd.lag {
			lagSeen, lagGap = true, 0
		} else if lagSeen {
			lagGap++
			if lagGap == MaxGap+1 {
				bad("%s exported by no node for more than %d consecutive rounds (from round %d)", Lag, MaxGap, k-MaxGap)
			}
		}
		if math.Abs(rd.leader-1) < 1e-9 {
			leaderGap = 0
		} else {
			leaderGap++
			if leaderGap == MaxGap+1 {
				bad("%s summed to %.0f over the nodes for more than %d consecutive rounds (from round %d)", Leader, rd.leader, MaxGap, k-MaxGap)
			}
		}
	}
	if !lagSeen {
		note("%s never appeared (no consumer group with committed offsets during the upgrade)", Lag)
	}
	return r
}
