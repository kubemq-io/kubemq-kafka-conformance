package verify

// Mutation cases added after the independent review: each allowance path (offsets unknown,
// headers per run, commit on close) still names loss, duplicates, reordering, wrong positions and
// a broker that ignores the requested partition; timestamps and the stock readiness barrier are
// checked; the verifiable converter catches log-side defects too.

import (
	"strings"
	"testing"
)

func joined(p []string) string { return strings.Join(p, " | ") }

// unknownOffsets turns a faithful leg into one whose client reports no offsets.
func unknownOffsets(evs []Event) []Event {
	out := clone(evs)
	out[0].AckOffsetsUnknown = true
	for i := range out {
		if out[i].Ev == "ack" {
			out[i].Partition, out[i].Offset = -1, -1
		}
	}
	return out
}

func allowanceSpec() Spec {
	s := spec(scenarioMode)
	s.Limits = []string{LimitAckOffsetsUnknown, LimitHeadersPerRun, LimitCommitOnClose}
	return s
}

func TestAllowancePathsStillNameEveryDefect(t *testing.T) {
	s := allowanceSpec()
	base, view := faithful(s)
	base = unknownOffsets(base)
	var noCommits []Event
	for _, e := range base {
		if e.Ev != "commit" {
			noCommits = append(noCommits, e)
		}
	}
	cleanObs, problems := Check(s, Ledger{Events: noCommits}, view)
	if len(problems) > 0 {
		t.Fatalf("faithful leg under every allowance reported problems: %v", problems)
	}
	cases := []struct {
		name, want string
		mut        func(e []Event, v View) ([]Event, View)
	}{
		{"duplicate delivery", "delivered twice", func(e []Event, v View) ([]Event, View) {
			i := find(e, "recv", 1, 2)
			return append(e[:i+1:i+1], append([]Event{e[i]}, e[i+1:]...)...), v
		}},
		{"reorder", "out of offset order", func(e []Event, v View) ([]Event, View) {
			i, j := find(e, "recv", 1, 0), find(e, "recv", 1, 1)
			e[i], e[j] = e[j], e[i]
			return e, v
		}},
		{"wrong delivered offset", "delivered at another position", func(e []Event, v View) ([]Event, View) {
			i := find(e, "recv", 1, 4)
			e[i].Offset += 50
			return e, v
		}},
		{"loss in the log", "never acknowledged", func(e []Event, v View) ([]Event, View) {
			// with offsets unknown, a record missing from the log has no position: it is reported
			// as delivered but never stored, and the delivered-position check names it
			v.Records = v.Records[1:]
			return e, v
		}},
		{"duplicate in the log", "stored twice", func(e []Event, v View) ([]Event, View) {
			v.Records = append(v.Records, v.Records[3])
			return e, v
		}},
		{"key split across partitions in the log", "spread over more than one partition", func(e []Event, v View) ([]Event, View) {
			// seq 0 (key-00) stored on partition 2 while key-00's other records are on partition 0
			v.Records[0].Partition = 2
			for i := range e {
				if e[i].Ev == "recv" && e[i].Seq != nil && *e[i].Seq == 0 {
					e[i].Partition = 2
				}
			}
			return e, v
		}},
		{"committed behind the log end (commit on close)", "committed offsets differ", func(e []Event, v View) ([]Event, View) {
			v.Committed[0]--
			return e, v
		}},
	}
	for _, cs := range cases {
		e, v := cs.mut(clone(noCommits), cloneView(view))
		if _, problems := Check(s, Ledger{Events: e}, v); !strings.Contains(joined(problems), cs.want) {
			if cs.name == "loss in the log" && strings.Contains(joined(problems), "missing from the log") {
				continue
			}
			t.Errorf("%s: not named (%q); problems: %s", cs.name, cs.want, joined(problems))
		}
	}
	// A broker that ignores the requested partition and appends every record to partition 0:
	// content, offsets and commits are self-consistent, so the observation must differ.
	moved := cloneView(view)
	next := int64(0)
	for i := range moved.Records {
		moved.Records[i].Partition, moved.Records[i].Offset = 0, next
		next++
	}
	moved.HighWatermarks = map[int32]int64{0: next, 1: 0, 2: 0}
	moved.Committed = map[int32]int64{0: next}
	var e []Event
	for _, ev := range noCommits {
		if ev.Ev == "recv" {
			ev.Partition = 0
		}
		e = append(e, ev)
	}
	// renumber deliveries to the new stored offsets, in delivery order
	stored := map[int]int64{}
	for _, r := range moved.Records {
		stored[s.SeqOf(r.Value)] = r.Offset
	}
	var parts []Event
	for _, ev := range e {
		if ev.Ev == "recv" {
			ev.Offset = stored[*ev.Seq]
		}
		if ev.Ev == "assigned" {
			ev.Partitions = []int32{0, 1, 2}
		}
		parts = append(parts, ev)
	}
	obs, _ := Check(s, Ledger{Events: parts}, moved)
	if _, same := Diff(cleanObs, obs); same {
		t.Fatalf("all records on partition 0 left the observation unchanged: %v", obs)
	}
}

func TestCommitOnCloseOnlyWhereAllowed(t *testing.T) {
	s := spec(scenarioMode)
	evs, view := faithful(s)
	var noCommits []Event
	for _, e := range evs {
		if e.Ev != "commit" {
			noCommits = append(noCommits, e)
		}
	}
	if _, problems := Check(s, Ledger{Events: noCommits}, view); !strings.Contains(joined(problems), "no commit reported") {
		t.Fatalf("missing explicit commits accepted without the limit: %v", problems)
	}
}

func TestTimestampsMustBeProducerCreateTimeInsideTheLeg(t *testing.T) {
	s := spec(scenarioMode)
	evs, view := faithful(s)
	view.LegStartMS, view.LegEndMS = 1_000_000, 1_010_000
	for i := range view.Records {
		view.Records[i].CreateTS, view.Records[i].Timestamp = true, 1_005_000
	}
	obs, problems := Check(s, Ledger{Events: evs}, view)
	if len(problems) > 0 || obs["log_timestamps"] != "create-time" {
		t.Fatalf("faithful timestamps: %v %v", problems, obs["log_timestamps"])
	}
	for name, mut := range map[string]func(v *View){
		"timestamp -1":    func(v *View) { v.Records[3].Timestamp = -1 },
		"log-append time": func(v *View) { v.Records[5].CreateTS = false },
		"far future":      func(v *View) { v.Records[7].Timestamp = 9_000_000 },
	} {
		v := cloneView(view)
		mut(&v)
		if _, problems := Check(s, Ledger{Events: evs}, v); !strings.Contains(joined(problems), "create time inside the run") {
			t.Errorf("%s: not named: %v", name, problems)
		}
	}
}

func TestStockBarrierPositionsAreChecked(t *testing.T) {
	s := spec(stockMode)
	evs, view := faithful(s)
	at := find(evs, "assigned", 1, 0) + 1
	withPos := func(off int64) []Event {
		e := append(clone(evs[:at]), Event{Ev: "position", Phase: 1, Partition: 0, Offset: off})
		return append(e, evs[at:]...)
	}
	if _, problems := Check(s, Ledger{Events: withPos(0)}, view); len(problems) > 0 {
		t.Fatalf("barrier at the first record reported problems: %v", problems)
	}
	for name, off := range map[string]int64{"after the first record": 3, "unresolved": -1} {
		if _, problems := Check(s, Ledger{Events: withPos(off)}, view); !strings.Contains(joined(problems), "readiness barrier failed") {
			t.Errorf("%s: not named: %v", name, problems)
		}
	}
}

func TestMutation_VerifiableLogSideDefects(t *testing.T) {
	s := Spec{Mode: verifiableMode, Client: "kafka-clients", Version: "4.3.1", Partitions: 3, Records: 30, Keys: 5}
	for name, c := range map[string]struct {
		want string
		mut  func(p, c []string, v *View) ([]string, []string)
	}{
		"lost in the log": {"missing from the log", func(p, c []string, v *View) ([]string, []string) {
			v.Records = v.Records[1:]
			return p, c
		}},
		"duplicate in the log": {"stored twice", func(p, c []string, v *View) ([]string, []string) {
			v.Records = append(v.Records, v.Records[2])
			return p, c
		}},
		"wrong delivered offset": {"delivered at another position", func(p, c []string, v *View) ([]string, []string) {
			for i, l := range c {
				if strings.Contains(l, `"name":"record_data"`) {
					c[i] = strings.Replace(l, `"offset":`, `"offset":7`, 1)
					break
				}
			}
			return p, c
		}},
	} {
		p, cons, v := verifiableRun(30, 5)
		p, cons = c.mut(p, cons, &v)
		if _, problems := Check(s, FromVerifiable(join(p), join(cons)), v); !strings.Contains(joined(problems), c.want) {
			t.Errorf("%s: not named (%q): %s", name, c.want, joined(problems))
		}
	}
}

func TestScenarioResumePositionsAreNotABarrierFailure(t *testing.T) {
	s := spec(scenarioMode)
	evs, view := faithful(s)
	at := find(evs, "assigned", 2, 0) + 1
	e := append(clone(evs[:at]), Event{Ev: "position", Phase: 2, Partition: 0, Offset: 10})
	e = append(e, evs[at:]...)
	if _, problems := Check(s, Ledger{Events: e}, view); len(problems) > 0 {
		t.Fatalf("a phase-2 member resuming at its committed offset was faulted: %v", problems)
	}
}
