package continuity

// Mutation self-tests for the rolling-upgrade continuity checker: each injects one defect an
// operator would see on a dashboard into a clean three-node upgrade and proves it is reported.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/operations/metric"
)

var t0 = time.Date(2026, 10, 8, 10, 0, 0, 0, time.UTC)

const iv = 15 * time.Second

type state struct {
	start, msgs, leader float64
	lag                 bool
}

// clean builds 10 rounds for nodes a, b, c; b restarts between rounds 4 and 5 (new start time,
// its produced counter carries on); a leads and exports the lag gauge.
func clean() (map[string]map[int]*state, []Restart) {
	nodes := map[string]map[int]*state{}
	for _, n := range []string{"a", "b", "c"} {
		nodes[n] = map[int]*state{}
		for k := 0; k < 10; k++ {
			st := &state{start: 1000, msgs: float64(100 * (k + 1))}
			if n == "a" {
				st.leader, st.lag = 1, true
			}
			if n == "b" && k >= 5 {
				st.start = 2000
			}
			nodes[n][k] = st
		}
	}
	return nodes, []Restart{{Node: "b", At: t0.Add(4*iv + 7*time.Second)}}
}

func build(nodes map[string]map[int]*state) map[string][]Scrape {
	out := map[string][]Scrape{}
	for n, ks := range nodes {
		for k := 0; k < 10; k++ {
			st, ok := ks[k]
			if !ok {
				continue
			}
			var lines []string
			if st.start >= 0 {
				lines = append(lines, fmt.Sprintf("%s %v", StartTime, st.start))
			}
			if st.msgs >= 0 {
				lines = append(lines, fmt.Sprintf(`%s{op="produced"} %v`, Messages, st.msgs))
			}
			lines = append(lines, fmt.Sprintf("%s %v", Leader, st.leader))
			if st.lag {
				lines = append(lines, fmt.Sprintf(`%s{group="g",topic="t",partition="0"} 5`, Lag))
			}
			sc, err := metric.ParsePrometheus(strings.NewReader(strings.Join(lines, "\n")))
			if err != nil {
				panic(err)
			}
			out[n] = append(out[n], Scrape{At: t0.Add(time.Duration(k)*iv + time.Second), Series: sc})
		}
	}
	return out
}

func TestMutation_ContinuityDetectsEveryDefect(t *testing.T) {
	nodes, restarts := clean()
	if r := Judge(build(nodes), restarts, iv); len(r.Problems) != 0 {
		t.Fatalf("control: clean upgrade must pass: %v", r.Problems)
	}
	muts := map[string]func(map[string]map[int]*state) []Restart{
		"counter backwards without a restart": func(m map[string]map[int]*state) []Restart { m["c"][6].msgs = 50; return restarts },
		"start time changed without a restart": func(m map[string]map[int]*state) []Restart {
			for k := 7; k < 10; k++ {
				m["c"][k].start = 3000
			}
			return restarts
		},
		"restarted node never answers afterwards": func(m map[string]map[int]*state) []Restart {
			for k := 5; k < 10; k++ {
				delete(m["b"], k)
			}
			return restarts
		},
		"restart recorded, process never restarted": func(m map[string]map[int]*state) []Restart {
			for k := 5; k < 10; k++ {
				m["b"][k].start = 1000
			}
			return restarts
		},
		"lag gauge gone for 3 rounds": func(m map[string]map[int]*state) []Restart {
			for k := 3; k < 6; k++ {
				m["a"][k].lag = false
			}
			return restarts
		},
		"no leader for 3 rounds": func(m map[string]map[int]*state) []Restart {
			for k := 3; k < 6; k++ {
				m["a"][k].leader = 0
			}
			return restarts
		},
		"two leaders for 3 rounds": func(m map[string]map[int]*state) []Restart {
			for k := 3; k < 6; k++ {
				m["c"][k].leader = 1
			}
			return restarts
		},
		"produced counter missing on a running node": func(m map[string]map[int]*state) []Restart { m["c"][3].msgs = -1; return restarts },
		"start time series missing":                  func(m map[string]map[int]*state) []Restart { m["a"][2].start = -1; return restarts },
		"every node silent for 4 rounds": func(m map[string]map[int]*state) []Restart {
			for _, n := range []string{"a", "b", "c"} {
				for k := 6; k < 10; k++ {
					delete(m[n], k)
				}
			}
			// keep one late scrape so the rounds after the outage exist
			m["a"][9] = &state{start: 1000, msgs: 1000, leader: 1, lag: true}
			return restarts
		},
		"node that never restarted silent for 3 rounds": func(m map[string]map[int]*state) []Restart {
			for k := 1; k < 4; k++ {
				delete(m["c"], k)
			}
			return restarts
		},
		"restart of a node that was never scraped": func(m map[string]map[int]*state) []Restart {
			return append(restarts, Restart{Node: "d", At: t0.Add(5 * iv)})
		},
	}
	for name, mut := range muts {
		m, _ := clean()
		rs := mut(m)
		if r := Judge(build(m), rs, iv); len(r.Problems) == 0 {
			t.Errorf("checker stayed green under %q", name)
		}
	}
}

func TestToleratedTransitions(t *testing.T) {
	// Two rounds without lag or leader, a counter reset at the node's own restart, and a counter
	// created only after the first post-restart scrape are tolerated (noted, not problems).
	m, rs := clean()
	for k := 3; k < 5; k++ {
		m["a"][k].lag, m["a"][k].leader = false, 0
	}
	m["b"][5].msgs = -1
	m["b"][6].msgs = 10
	r := Judge(build(m), rs, iv)
	if len(r.Problems) != 0 {
		t.Fatalf("tolerated transitions reported as problems: %v", r.Problems)
	}
	if len(r.Notes) < 2 {
		t.Fatalf("reset and late counter must be noted: %v", r.Notes)
	}
}

// Two scrapes of one node in the same round count once (a healthy leader must not read as 2).
func TestDuplicateScrapeCountsOnce(t *testing.T) {
	m, rs := clean()
	sc := build(m)
	dup := sc["a"][3]
	dup.At = dup.At.Add(5 * time.Second)
	sc["a"] = append(sc["a"], dup)
	if r := Judge(sc, rs, iv); len(r.Problems) != 0 {
		t.Fatalf("%v", r.Problems)
	}
}

func TestLoadDir(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("kubemq-cluster-1-1791400000.prom", StartTime+" 1\n")
	write("kubemq-cluster-1-1791399985.prom", StartTime+" 1\n")
	write("kubemq-cluster-2-1791400000.prom", StartTime+" 1\n")
	got, err := LoadDir(dir)
	if err != nil || len(got) != 2 || len(got["kubemq-cluster-1"]) != 2 || !got["kubemq-cluster-1"][0].At.Before(got["kubemq-cluster-1"][1].At) {
		t.Fatalf("%v %v", got, err)
	}
	write("garbage.prom", "x 1\n")
	if _, err := LoadDir(dir); err == nil {
		t.Fatal("a file without a timestamp was accepted")
	}
}
