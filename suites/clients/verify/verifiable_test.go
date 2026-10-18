package verify

import (
	"fmt"
	"strings"
	"testing"
)

// verifiableRun is faithful tool output: n records, keys repeating mod k over 3 partitions.
func verifiableRun(n, k int) (producer, consumer []string, view View) {
	view = View{Partitions: 3, HighWatermarks: map[int32]int64{}, Committed: map[int32]int64{}}
	producer = []string{`{"name":"kcert_version","version":"4.3.1"}`, `{"timestamp":1,"name":"startup_complete"}`}
	next := map[int32]int64{}
	type rec struct {
		p   int32
		o   int64
		seq int
	}
	var recs []rec
	for seq := 0; seq < n; seq++ {
		p := int32((seq % k) % 3)
		o := next[p]
		next[p]++
		recs = append(recs, rec{p, o, seq})
		producer = append(producer, fmt.Sprintf(`{"timestamp":1,"name":"producer_send_success","key":"%d","value":"%d","offset":%d,"partition":%d,"topic":"t"}`, seq%k, seq, o, p))
		key, val := fmt.Sprint(seq%k), fmt.Sprint(seq)
		view.Records = append(view.Records, LogRecord{Partition: p, Offset: o, Key: &key, Value: &val})
	}
	producer = append(producer, `{"timestamp":1,"name":"shutdown_complete"}`, `{"timestamp":1,"name":"tool_data","sent":1,"acked":1}`)
	consumer = []string{`{"timestamp":1,"name":"startup_complete"}`,
		`{"timestamp":1,"name":"partitions_assigned","partitions":[{"topic":"t","partition":0},{"topic":"t","partition":1},{"topic":"t","partition":2}]}`}
	for p := int32(0); p < 3; p++ {
		for _, r := range recs {
			if r.p == p {
				consumer = append(consumer, fmt.Sprintf(`{"timestamp":1,"name":"record_data","key":"%d","value":"%d","topic":"t","partition":%d,"offset":%d}`, r.seq%k, r.seq, r.p, r.o))
			}
		}
	}
	consumer = append(consumer, `{"timestamp":1,"name":"offsets_committed","offsets":[],"success":true}`,
		`{"timestamp":1,"name":"partitions_revoked","partitions":[]}`, `{"timestamp":1,"name":"shutdown_complete"}`)
	for p := int32(0); p < 3; p++ {
		view.HighWatermarks[p], view.Committed[p] = next[p], next[p]
	}
	return producer, consumer, view
}

func join(lines []string) []byte { return []byte(strings.Join(lines, "\n") + "\n") }

func TestVerifiableFaithfulIsClean(t *testing.T) {
	s := Spec{Mode: verifiableMode, Client: "kafka-clients", Version: "4.3.1", Partitions: 3, Records: 30, Keys: 5}
	p, c, v := verifiableRun(30, 5)
	obs, problems := Check(s, FromVerifiable(join(p), join(c)), v)
	if len(problems) > 0 {
		t.Fatalf("faithful verifiable run reported problems: %v", problems)
	}
	if obs["phase1_delivered"] != 30 || obs["committed_at_end"] != true {
		t.Fatalf("observation %v", obs)
	}
}

func TestMutation_VerifiableDefectsAreNamed(t *testing.T) {
	s := Spec{Mode: verifiableMode, Client: "kafka-clients", Version: "4.3.1", Partitions: 3, Records: 30, Keys: 5}
	idx := func(lines []string, name string, nth int) int {
		n := 0
		for i, l := range lines {
			if strings.Contains(l, `"name":"`+name+`"`) {
				if n == nth {
					return i
				}
				n++
			}
		}
		panic(name)
	}
	cases := []struct {
		name, want string
		mut        func(p, c []string, v *View) ([]string, []string)
	}{
		{"loss", "records never delivered", func(p, c []string, v *View) ([]string, []string) {
			i := idx(c, "record_data", 3)
			return p, append(c[:i:i], c[i+1:]...)
		}},
		{"duplicate", "delivered twice", func(p, c []string, v *View) ([]string, []string) {
			i := idx(c, "record_data", 3)
			return p, append(c[:i+1:i+1], append([]string{c[i]}, c[i+1:]...)...)
		}},
		{"reorder", "out of offset order", func(p, c []string, v *View) ([]string, []string) {
			i, j := idx(c, "record_data", 0), idx(c, "record_data", 1)
			c[i], c[j] = c[j], c[i]
			return p, c
		}},
		{"wrong offset", "another position than acknowledged", func(p, c []string, v *View) ([]string, []string) {
			i := idx(p, "producer_send_success", 2)
			p[i] = strings.Replace(p[i], `"offset":`, `"offset":10`, 1)
			return p, c
		}},
		{"wrong error code (send error)", "acknowledgements with an error", func(p, c []string, v *View) ([]string, []string) {
			i := idx(p, "producer_send_success", 2)
			p[i] = strings.Replace(p[i], "producer_send_success", "producer_send_error", 1)
			return p, c
		}},
		{"failed commit", "commit failed", func(p, c []string, v *View) ([]string, []string) {
			i := idx(c, "offsets_committed", 0)
			c[i] = strings.Replace(c[i], `"success":true`, `"success":false,"error":"REBALANCE_IN_PROGRESS"`, 1)
			return p, c
		}},
		{"consumer crashed", "did not reach shutdown_complete", func(p, c []string, v *View) ([]string, []string) {
			return p, c[:len(c)-1]
		}},
		{"other client version", "the row pins", func(p, c []string, v *View) ([]string, []string) {
			p[0] = `{"name":"kcert_version","version":"4.2.0"}`
			return p, c
		}},
		{"commit behind the log end", "committed offsets differ", func(p, c []string, v *View) ([]string, []string) {
			v.Committed[2]--
			return p, c
		}},
	}
	for _, cs := range cases {
		p, c, v := verifiableRun(30, 5)
		p, c = cs.mut(p, c, &v)
		_, problems := Check(s, FromVerifiable(join(p), join(c)), v)
		if j := strings.Join(problems, " | "); !strings.Contains(j, cs.want) {
			t.Errorf("%s: verifier did not report %q; problems: %s", cs.name, cs.want, j)
		}
	}
}
