package verify

// Verifier mutation self-tests (docs/methodology.md): a faithful synthetic leg must be clean,
// and each injected defect (loss, duplicate, reorder, wrong offset, wrong error code, and the
// scenario-specific ones) must produce a named problem.

import (
	"encoding/json"
	"strings"
	"testing"
)

func spec(mode string) Spec {
	if mode == stockMode {
		return Spec{Mode: stockMode, Client: "franz-go", Version: "v1.22.0", Records: 10, Admin: true}
	}
	return Spec{Mode: scenarioMode, Client: "franz-go", Version: "v1.22.0", Partitions: 3, Records: 30, Records2: 12, Keys: 5, Admin: true}
}

func intp(i int) *int { return &i }

// faithful builds the ledger and fresh-reader view of a correct run.
func faithful(s Spec) ([]Event, View) {
	parts := s.Partitions
	if parts == 0 {
		parts = 1
	}
	evs := []Event{{Ev: "start", Client: s.Client, Version: s.Version, Mode: s.Mode, GroupProtocol: "classic"}}
	if s.Admin {
		evs = append(evs, Event{Ev: "create", Topic: "t", NumPartitions: s.Partitions})
	}
	next := map[int32]int64{}
	acks := map[int]pos{}
	view := View{Partitions: parts, HighWatermarks: map[int32]int64{}, Committed: map[int32]int64{}}
	produce := func(ph, lo, hi int) {
		for seq := lo; seq < hi; seq++ {
			p := int32(0)
			if s.Keys > 0 {
				p = int32((seq % s.Keys) % int(parts))
			}
			o := next[p]
			next[p]++
			acks[seq] = pos{p, o}
			evs = append(evs, Event{Ev: "ack", Phase: ph, Seq: intp(seq), Partition: p, Offset: o})
			r := s.Expected(seq)
			r.Partition, r.Offset = p, o
			view.Records = append(view.Records, r)
		}
	}
	all := make([]int32, 0, parts)
	for p := int32(0); p < parts; p++ {
		all = append(all, p)
	}
	consume := func(ph, lo, hi int) {
		evs = append(evs, Event{Ev: "assigned", Phase: ph, Partitions: all})
		// deliver partition by partition, offsets ascending
		for p := int32(0); p < parts; p++ {
			for seq := lo; seq < hi; seq++ {
				if acks[seq].p != p {
					continue
				}
				r := s.Expected(seq)
				evs = append(evs, Event{Ev: "recv", Phase: ph, Seq: intp(seq), Partition: p, Offset: acks[seq].o, Key: r.Key, Value: r.Value, Headers: r.Headers})
			}
		}
		if s.Mode == scenarioMode {
			evs = append(evs, Event{Ev: "commit", Phase: ph})
		}
	}
	if s.Mode == stockMode {
		evs = append(evs, Event{Ev: "assigned", Phase: 1, Partitions: all})
		produce(1, 0, s.Records)
		for seq := 0; seq < s.Records; seq++ {
			r := s.Expected(seq)
			evs = append(evs, Event{Ev: "recv", Phase: 1, Seq: intp(seq), Partition: acks[seq].p, Offset: acks[seq].o, Value: r.Value})
		}
	} else {
		produce(1, 0, s.Records)
		consume(1, 0, s.Records)
		produce(2, s.Records, s.total())
		consume(2, s.Records, s.total())
	}
	evs = append(evs, Event{Ev: "done"})
	for p := int32(0); p < parts; p++ {
		view.HighWatermarks[p] = next[p]
		view.Committed[p] = next[p]
	}
	return evs, view
}

func clone(evs []Event) []Event { return append([]Event(nil), evs...) }

func cloneView(v View) View {
	out := v
	out.Records = append([]LogRecord(nil), v.Records...)
	out.HighWatermarks, out.Committed = map[int32]int64{}, map[int32]int64{}
	for k, x := range v.HighWatermarks {
		out.HighWatermarks[k] = x
	}
	for k, x := range v.Committed {
		out.Committed[k] = x
	}
	return out
}

func find(evs []Event, ev string, phase, nth int) int {
	n := 0
	for i, e := range evs {
		if e.Ev == ev && (phase == 0 || e.Phase == phase) {
			if n == nth {
				return i
			}
			n++
		}
	}
	panic("no such event")
}

func TestFaithfulLegsAreClean(t *testing.T) {
	for _, mode := range []string{scenarioMode, stockMode} {
		s := spec(mode)
		evs, view := faithful(s)
		obs, problems := Check(s, Ledger{Events: evs}, view)
		if len(problems) > 0 {
			t.Fatalf("%s: faithful leg reported problems: %v", mode, problems)
		}
		if obs["records_acked"] != s.total() || obs["phase1_delivered"] != s.Records || obs["committed_at_end"] != true {
			t.Fatalf("%s: unexpected observation %v", mode, obs)
		}
	}
}

type mutation struct {
	name string
	want string // substring of the problem that must appear
	mut  func(s Spec, evs []Event, v View) ([]Event, View)
}

var scenarioMutations = []mutation{
	{"loss in consume phase 1", "phase 1: records never delivered", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "recv", 1, 3)
		return append(e[:i:i], e[i+1:]...), v
	}},
	{"loss in the log", "missing from the log", func(s Spec, e []Event, v View) ([]Event, View) {
		v.Records = append(v.Records[:4:4], v.Records[5:]...)
		return e, v
	}},
	{"duplicate delivery", "delivered twice", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "recv", 1, 2)
		return append(e[:i+1:i+1], append([]Event{e[i]}, e[i+1:]...)...), v
	}},
	{"duplicate in the log", "stored twice", func(s Spec, e []Event, v View) ([]Event, View) {
		v.Records = append(v.Records, v.Records[7])
		return e, v
	}},
	{"reorder within a partition", "out of offset order", func(s Spec, e []Event, v View) ([]Event, View) {
		i, j := find(e, "recv", 1, 0), find(e, "recv", 1, 1)
		e[i], e[j] = e[j], e[i]
		return e, v
	}},
	{"wrong offset in an acknowledgement", "another position than acknowledged", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "ack", 1, 5)
		e[i].Offset += 100
		return e, v
	}},
	{"wrong offset in a delivery", "delivered at another position", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "recv", 2, 1)
		e[i].Offset += 100
		return e, v
	}},
	{"wrong error code on produce", "acknowledgements with an error", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "ack", 1, 0)
		e[i].ErrorCode = 6
		return e, v
	}},
	{"wrong error code on commit", "commit failed: error code 22", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "commit", 2, 0)
		e[i].ErrorCode = 22
		return e, v
	}},
	{"wrong error code on create", "create topic: error code 36", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "create", 0, 0)
		e[i].ErrorCode = 36
		return e, v
	}},
	{"phase 2 resumed from the start", "another phase delivered", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "recv", 1, 0)
		x := e[i]
		x.Phase = 2
		j := find(e, "recv", 2, 0)
		return append(e[:j:j], append([]Event{x}, e[j:]...)...), v
	}},
	{"wrong value delivered", "wrong content", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "recv", 1, 4)
		k := "key-99"
		e[i].Key = &k
		return e, v
	}},
	{"header lost in the log", "stored with wrong content", func(s Spec, e []Event, v View) ([]Event, View) {
		v.Records[2].Headers = v.Records[2].Headers[:1]
		return e, v
	}},
	{"record from an unassigned partition", "not assigned to the member", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "assigned", 1, 0)
		e[i].Partitions = []int32{1, 2}
		return e, v
	}},
	{"key split over partitions", "spread over more than one partition", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "ack", 1, 0) // seq 0, key-00
		e[i].Partition, e[i].Offset = 2, 999
		return e, v
	}},
	{"offset handed out twice", "acknowledged for two records", func(s Spec, e []Event, v View) ([]Event, View) {
		a, b := find(e, "ack", 1, 0), find(e, "ack", 1, 3) // seq 0 and 3 are on partition 0
		e[b].Offset = e[a].Offset
		return e, v
	}},
	{"committed offset behind the log end", "committed offsets differ", func(s Spec, e []Event, v View) ([]Event, View) {
		v.Committed[1]--
		return e, v
	}},
	{"wrong partition count", "partitions, 3 were asked for", func(s Spec, e []Event, v View) ([]Event, View) {
		v.Partitions = 1
		return e, v
	}},
	{"driver crashed", "does not end with a done event", func(s Spec, e []Event, v View) ([]Event, View) {
		return e[:len(e)-1], v
	}},
	{"other client version loaded", "the row pins", func(s Spec, e []Event, v View) ([]Event, View) {
		e[0].Version = "v1.21.0"
		return e, v
	}},
	{"driver error event", "driver step \"consume\" failed", func(s Spec, e []Event, v View) ([]Event, View) {
		x := Event{Ev: "error", Step: "consume", ErrorCode: -1, Error: "boom"}
		return append(e[:len(e)-1:len(e)-1], x, e[len(e)-1]), v
	}},
	{"fresh reader failed", "fresh reader could not read", func(s Spec, e []Event, v View) ([]Event, View) {
		v.ReadError = "UNKNOWN_TOPIC_OR_PARTITION"
		return e, v
	}},
	{"never acknowledged", "records never acknowledged", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "ack", 2, 3)
		return append(e[:i:i], e[i+1:]...), v
	}},
}

var stockMutations = []mutation{
	{"loss", "records never delivered", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "recv", 1, 9)
		return append(e[:i:i], e[i+1:]...), v
	}},
	{"duplicate", "delivered twice", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "recv", 1, 0)
		return append(e[:i+1:i+1], append([]Event{e[i]}, e[i+1:]...)...), v
	}},
	{"reorder", "out of offset order", func(s Spec, e []Event, v View) ([]Event, View) {
		i, j := find(e, "recv", 1, 0), find(e, "recv", 1, 1)
		e[i], e[j] = e[j], e[i]
		return e, v
	}},
	{"wrong offset", "another position than acknowledged", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "ack", 1, 2)
		e[i].Offset = 77
		return e, v
	}},
	{"wrong error code", "error code 3", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "ack", 1, 2)
		e[i].ErrorCode = 3
		return e, v
	}},
	{"key added", "wrong content", func(s Spec, e []Event, v View) ([]Event, View) {
		k := "k"
		v.Records[0].Key = &k
		return e, v
	}},
	{"no assignment reported", "no partition assignment", func(s Spec, e []Event, v View) ([]Event, View) {
		i := find(e, "assigned", 1, 0)
		return append(e[:i:i], e[i+1:]...), v
	}},
}

func TestMutation_EveryDefectIsNamed(t *testing.T) {
	for mode, muts := range map[string][]mutation{scenarioMode: scenarioMutations, stockMode: stockMutations} {
		s := spec(mode)
		for _, m := range muts {
			evs, view := faithful(s)
			e, v := m.mut(s, clone(evs), cloneView(view))
			_, problems := Check(s, Ledger{Events: e}, v)
			joined := strings.Join(problems, " | ")
			if !strings.Contains(joined, m.want) {
				t.Errorf("%s / %s: verifier did not report %q; problems: %s", mode, m.name, m.want, joined)
			}
		}
	}
}

func TestParseLedger(t *testing.T) {
	in := `{"ev":"start","client":"franz-go","version":"v1.22.0","mode":"scenario","group_protocol":"classic"}
{"ev":"create","topic":"t","partitions":3,"error_code":0}

{"ev":"assigned","phase":1,"partitions":[0,2]}
not json
{"ev":"recv","phase":1,"seq":4,"partition":2,"offset":9,"key":null,"value":"value-000004","headers":[["seq","4"]]}
{"ev":"done"}
`
	l := ParseLedger(strings.NewReader(in))
	if len(l.Events) != 5 || len(l.Problems) != 1 {
		t.Fatalf("events %d problems %v", len(l.Events), l.Problems)
	}
	if l.Events[1].NumPartitions != 3 || len(l.Events[2].Partitions) != 2 || l.Events[3].Key != nil || *l.Events[3].Value != "value-000004" {
		b, _ := json.Marshal(l.Events)
		t.Fatalf("decoded wrongly: %s", b)
	}
}

func TestMutation_DiffNamesWrongErrorCode(t *testing.T) {
	diff, same := Diff(Observation{"create_error_code": 0, "records_acked": 400}, Observation{"create_error_code": 37, "records_acked": 400})
	if same || diff != "create_error_code: Kafka 0, KubeMQ 37" {
		t.Fatalf("got %q %v", diff, same)
	}
	if _, same := Diff(Observation{"a": 1}, Observation{"a": 1}); !same {
		t.Fatal("identical observations reported different")
	}
}

func TestHiddenAssignmentSkipsOnlyThatCheck(t *testing.T) {
	s := spec(scenarioMode)
	s.Limits = []string{LimitAssignmentHidden}
	evs, view := faithful(s)
	evs[0].AssignmentHidden = true
	var kept []Event
	for _, e := range evs {
		if e.Ev != "assigned" {
			kept = append(kept, e)
		}
	}
	if _, problems := Check(s, Ledger{Events: kept}, view); len(problems) > 0 {
		t.Fatalf("a driver that declares its assignment hidden must not be faulted for it: %v", problems)
	}
	// Every other check still applies: a lost delivery is still named.
	i := find(kept, "recv", 1, 2)
	lost := append(append([]Event(nil), kept[:i]...), kept[i+1:]...)
	if _, problems := Check(s, Ledger{Events: lost}, view); !strings.Contains(strings.Join(problems, "|"), "records never delivered") {
		t.Fatalf("loss not reported with hidden assignment: %v", problems)
	}
}

func TestAckOffsetsUnknownOnlyWhereTheRowAllowsIt(t *testing.T) {
	s := spec(stockMode)
	s.Limits = []string{LimitAckOffsetsUnknown}
	evs, view := faithful(s)
	evs[0].AckOffsetsUnknown = true
	for i := range evs {
		if evs[i].Ev == "ack" {
			evs[i].Partition, evs[i].Offset = 0, -1
		}
	}
	if _, problems := Check(s, Ledger{Events: evs}, view); len(problems) > 0 {
		t.Fatalf("acks=0 stock run must be judged by content: %v", problems)
	}
	// Loss in the log is still caught.
	v := cloneView(view)
	v.Records = v.Records[1:]
	if _, problems := Check(s, Ledger{Events: evs}, v); !strings.Contains(strings.Join(problems, "|"), "missing from the log") {
		t.Fatalf("loss not reported: %v", problems)
	}
	// A row that does not allow it rejects the declaration, and so does hidden assignment.
	sc := spec(scenarioMode)
	e2, v2 := faithful(sc)
	e2[0].AckOffsetsUnknown, e2[0].AssignmentHidden = true, true
	j := ""
	if _, problems := Check(sc, Ledger{Events: e2}, v2); true {
		j = strings.Join(problems, "|")
	}
	if !strings.Contains(j, "limit ack-offsets-unknown") || !strings.Contains(j, "limit assignment-hidden") {
		t.Fatalf("undeclared limits accepted: %s", j)
	}
}

func TestHeadersPerRunExpectsOnlyTheClientHeader(t *testing.T) {
	s := spec(scenarioMode)
	s.Limits = []string{LimitHeadersPerRun}
	if h := s.Expected(3).Headers; len(h) != 1 || h[0] != [2]string{"client", "franz-go"} {
		t.Fatalf("headers-per-run row expects %v", h)
	}
	evs, view := faithful(s)
	if _, problems := Check(s, Ledger{Events: evs}, view); len(problems) > 0 {
		t.Fatalf("faithful headers-per-run run reported problems: %v", problems)
	}
	// A record that still carries the per-record header is wrong content for this row.
	v := cloneView(view)
	v.Records[0].Headers = [][2]string{{"seq", "0"}, {"client", "franz-go"}}
	if _, problems := Check(s, Ledger{Events: evs}, v); !strings.Contains(strings.Join(problems, "|"), "wrong content") {
		t.Fatalf("header mismatch not reported: %v", problems)
	}
}
