package probe

// Verifier mutation tests for the tools track (docs/methodology.md): each injects one defect into
// a known-good input and proves the verifier reports it.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
)

func goodConsole() ([]ConsoleSent, []kout.ConsoleRecord) {
	var sent []ConsoleSent
	var got []kout.ConsoleRecord
	next := map[int]int64{}
	for i := 0; i < 24; i++ {
		s := ConsoleSent{Key: fmt.Sprintf("k%d", i%6), Value: fmt.Sprintf("v-%04d", i), Headers: fmt.Sprintf("seq:%d,src:cli", i)}
		sent = append(sent, s)
		p := i % 6 % 3
		got = append(got, kout.ConsoleRecord{Partition: p, Offset: next[p], Headers: s.Headers, Key: s.Key, Value: s.Value})
		next[p]++
	}
	return sent, got
}

func TestMutation_ConsoleDetectsEveryDefect(t *testing.T) {
	sent, got := goodConsole()
	clean := AnalyzeConsole(sent, got)
	if clean != (ConsoleCheck{Produced: 24, Consumed: 24, OffsetsFromZero: true, KeysSinglePartiton: true, ContentMatches: true}) {
		t.Fatalf("control: faithful round trip must be clean: %+v", clean)
	}
	clone := func() []kout.ConsoleRecord { return append([]kout.ConsoleRecord(nil), got...) }
	muts := map[string]struct {
		got   func() []kout.ConsoleRecord
		check func(ConsoleCheck) bool
	}{
		"loss":      {func() []kout.ConsoleRecord { g := clone(); return append(g[:5:5], g[6:]...) }, func(c ConsoleCheck) bool { return c.Missing == 1 }},
		"duplicate": {func() []kout.ConsoleRecord { g := clone(); return append(g, g[3]) }, func(c ConsoleCheck) bool { return c.Duplicates == 1 }},
		"reorder": {func() []kout.ConsoleRecord {
			g := clone() // records 0 and 6 share key k0 and partition 0: swap their contents, keep offsets
			g[0].Value, g[0].Headers, g[6].Value, g[6].Headers = g[6].Value, g[6].Headers, g[0].Value, g[0].Headers
			return g
		}, func(c ConsoleCheck) bool { return c.OutOfOrder > 0 }},
		"wrong offset":   {func() []kout.ConsoleRecord { g := clone(); g[7].Offset += 2; return g }, func(c ConsoleCheck) bool { return !c.OffsetsFromZero }},
		"wrong key":      {func() []kout.ConsoleRecord { g := clone(); g[2].Key = "kx"; return g }, func(c ConsoleCheck) bool { return !c.ContentMatches }},
		"wrong header":   {func() []kout.ConsoleRecord { g := clone(); g[2].Headers = "seq:2"; return g }, func(c ConsoleCheck) bool { return !c.ContentMatches }},
		"foreign record": {func() []kout.ConsoleRecord { g := clone(); g[4].Value = "v-9999"; return g }, func(c ConsoleCheck) bool { return c.Unexpected == 1 && c.Missing == 1 }},
		"split key":      {func() []kout.ConsoleRecord { g := clone(); g[6].Partition = 1; return g }, func(c ConsoleCheck) bool { return !c.KeysSinglePartiton }},
	}
	for name, m := range muts {
		if c := AnalyzeConsole(sent, m.got()); !m.check(c) {
			t.Errorf("%s: not detected: %+v", name, c)
		}
	}
}

func goodVerifiable(n, parts int) (prod, cons []kout.VerifiableEvent) {
	per := map[int]int64{}
	for i := 0; i < n; i++ {
		p := i % parts
		prod = append(prod, kout.VerifiableEvent{Name: "producer_send_success", Value: fmt.Sprint(i), Partition: p, Offset: per[p]})
		per[p]++
	}
	prod = append(prod, kout.VerifiableEvent{Name: "tool_data", Sent: n, Acked: n})
	rc := kout.VerifiableEvent{Name: "records_consumed", Count: n}
	ok := true
	oc := kout.VerifiableEvent{Name: "offsets_committed", Success: &ok}
	for p := 0; p < parts; p++ {
		rc.Partitions = append(rc.Partitions, struct {
			Partition int   `json:"partition"`
			Count     int   `json:"count"`
			MinOffset int64 `json:"minOffset"`
			MaxOffset int64 `json:"maxOffset"`
			Offset    int64 `json:"offset"`
		}{Partition: p, Count: int(per[p]), MinOffset: 0, MaxOffset: per[p] - 1})
		oc.Offsets = append(oc.Offsets, struct {
			Partition int   `json:"partition"`
			Offset    int64 `json:"offset"`
		}{Partition: p, Offset: per[p]})
	}
	cons = []kout.VerifiableEvent{rc, oc}
	for p := 0; p < parts; p++ {
		for o := int64(0); o < per[p]; o++ {
			cons = append(cons, kout.VerifiableEvent{Name: "record_data", Partition: p, Offset: o})
		}
	}
	return prod, cons
}

func TestMutation_VerifiableDetectsEveryDefect(t *testing.T) {
	prod, cons := goodVerifiable(30, 3)
	clean := AnalyzeVerifiable(prod, cons)
	want := VerifiableCheck{ProducerAcked: 30, SuccessEvents: 30, DistinctAckOffsets: true, AckOffsetsDense: true, Consumed: 30, ConsumedCoversAcks: true, CommitsSucceeded: true, FinalCommitAtEnd: true, RecordsExactlyAcked: true}
	if clean != want {
		t.Fatalf("control: %+v", clean)
	}
	type mut func() ([]kout.VerifiableEvent, []kout.VerifiableEvent)
	cp := func(e []kout.VerifiableEvent) []kout.VerifiableEvent {
		return append([]kout.VerifiableEvent(nil), e...)
	}
	muts := map[string]struct {
		m     mut
		check func(VerifiableCheck) bool
	}{
		"consumer loss (count short)": {func() ([]kout.VerifiableEvent, []kout.VerifiableEvent) {
			c := cp(cons)
			rc := c[0]
			rc.Partitions = append(rc.Partitions[:0:0], rc.Partitions...)
			rc.Partitions[1].Count--
			rc.Partitions[1].MaxOffset--
			rc.Count--
			c[0] = rc
			return prod, c
		}, func(c VerifiableCheck) bool { return !c.ConsumedCoversAcks && c.Consumed == 29 }},
		"consumer duplicate": {func() ([]kout.VerifiableEvent, []kout.VerifiableEvent) {
			c := cp(cons)
			return prod, append(c, c[0])
		}, func(c VerifiableCheck) bool { return !c.ConsumedCoversAcks }},
		"duplicate ack offset": {func() ([]kout.VerifiableEvent, []kout.VerifiableEvent) {
			p := cp(prod)
			p[4].Offset = p[1].Offset
			return p, cons
		}, func(c VerifiableCheck) bool { return !c.DistinctAckOffsets }},
		"wrong ack offset (gap)": {func() ([]kout.VerifiableEvent, []kout.VerifiableEvent) {
			p := cp(prod)
			p[27].Offset += 5
			return p, cons
		}, func(c VerifiableCheck) bool { return !c.AckOffsetsDense }},
		"failed commit (wrong error)": {func() ([]kout.VerifiableEvent, []kout.VerifiableEvent) {
			c := cp(cons)
			bad := false
			oc := c[1]
			oc.Success = &bad
			c[1] = oc
			return prod, c
		}, func(c VerifiableCheck) bool { return !c.CommitsSucceeded }},
		"commit short of the end": {func() ([]kout.VerifiableEvent, []kout.VerifiableEvent) {
			c := cp(cons)
			oc := c[1]
			oc.Offsets = append(oc.Offsets[:0:0], oc.Offsets...)
			oc.Offsets[0].Offset--
			c[1] = oc
			return prod, c
		}, func(c VerifiableCheck) bool { return !c.FinalCommitAtEnd }},
		"duplicate plus loss in one partition (range unchanged)": {func() ([]kout.VerifiableEvent, []kout.VerifiableEvent) {
			c := cp(cons)
			for i := range c {
				if c[i].Name == "record_data" && c[i].Partition == 1 && c[i].Offset == 2 {
					c[i].Offset = 1 // offset 2 lost, offset 1 consumed twice; count, min and max stay
				}
			}
			return prod, c
		}, func(c VerifiableCheck) bool { return !c.RecordsExactlyAcked && c.ConsumedCoversAcks }},
		"no record events": {func() ([]kout.VerifiableEvent, []kout.VerifiableEvent) {
			return prod, cp(cons)[:2]
		}, func(c VerifiableCheck) bool { return !c.RecordsExactlyAcked }},
		"producer loss": {func() ([]kout.VerifiableEvent, []kout.VerifiableEvent) {
			p := cp(prod)
			return append(p[:10:10], p[11:]...), cons
		}, func(c VerifiableCheck) bool { return c.SuccessEvents == 29 && !c.ConsumedCoversAcks }},
	}
	for name, m := range muts {
		p, c := m.m()
		if got := AnalyzeVerifiable(p, c); !m.check(got) {
			t.Errorf("%s: not detected: %+v", name, got)
		}
	}
}

func TestPutFlattensForFieldDiff(t *testing.T) {
	obs := Observation{}
	put(obs, "rt", ConsoleCheck{Produced: 3, ContentMatches: true})
	if obs["rt_produced"] != float64(3) || obs["rt_content_matches"] != true {
		t.Fatalf("%v", obs)
	}
}

func TestMutation_LinesDetectEveryDefect(t *testing.T) {
	var exp []string
	for i := 0; i < 20; i++ {
		exp = append(exp, fmt.Sprintf("g-%06d k=v;x=y payload-%d", i, i*7919%100003))
	}
	clean := AnalyzeLines(exp, append([]string(nil), exp...))
	if clean != (LineCheck{Expected: 20, Delivered: 20, Identical: true}) {
		t.Fatalf("control: %+v", clean)
	}
	cp := func() []string { return append([]string(nil), exp...) }
	muts := map[string]struct {
		got   func() []string
		check func(LineCheck) bool
	}{
		"loss":      {func() []string { g := cp(); return append(g[:9:9], g[10:]...) }, func(c LineCheck) bool { return c.Missing == 1 && !c.Identical }},
		"duplicate": {func() []string { g := cp(); return append(g[:11:11], g[10:]...) }, func(c LineCheck) bool { return c.Duplicates == 1 && !c.Identical }},
		"reorder":   {func() []string { g := cp(); g[3], g[4] = g[4], g[3]; return g }, func(c LineCheck) bool { return c.OrderViolations == 1 && !c.Identical }},
		"foreign":   {func() []string { g := cp(); g[7] = "torn line"; return g }, func(c LineCheck) bool { return c.Unexpected == 1 && c.Missing == 1 }},
		"truncated": {func() []string { return cp()[:19] }, func(c LineCheck) bool { return c.Missing == 1 && !c.Identical }},
	}
	for name, m := range muts {
		if c := AnalyzeLines(exp, m.got()); !m.check(c) {
			t.Errorf("%s: not detected: %+v", name, c)
		}
	}
	if !SortedEqual([]string{"a", "b", "b"}, []string{"b", "a", "b"}) || SortedEqual([]string{"a", "b"}, []string{"a", "a"}) || SortedEqual([]string{"a"}, []string{"a", "a"}) {
		t.Error("SortedEqual")
	}
}

func TestMutation_WordCountsDetectEveryDefect(t *testing.T) {
	exp := map[string]int64{"word01": 3, "word02": 2}
	good := []WordUpdate{{"word01", 1}, {"word02", 1}, {"word01", 2}, {"word02", 2}, {"word01", 3}}
	if c := AnalyzeWordCounts(exp, good); c != (WordCountCheck{Words: 2}) {
		t.Fatalf("control: %+v", c)
	}
	cp := func() []WordUpdate { return append([]WordUpdate(nil), good...) }
	muts := map[string]struct {
		got   func() []WordUpdate
		check func(WordCountCheck) bool
	}{
		"duplicate update": {func() []WordUpdate {
			g := cp()
			return append(g[:3:3], append([]WordUpdate{{"word01", 2}}, g[3:]...)...)
		}, func(c WordCountCheck) bool { return c.SequenceViolations == 1 }},
		"lost update":  {func() []WordUpdate { g := cp(); return append(g[:2:2], g[3:]...) }, func(c WordCountCheck) bool { return c.SequenceViolations == 1 }},
		"short final":  {func() []WordUpdate { return cp()[:4] }, func(c WordCountCheck) bool { return c.WrongFinal == 1 }},
		"foreign word": {func() []WordUpdate { return append(cp(), WordUpdate{"word99", 1}) }, func(c WordCountCheck) bool { return c.UnexpectedWords == 1 }},
		"reordered":    {func() []WordUpdate { g := cp(); g[0], g[2] = g[2], g[0]; return g }, func(c WordCountCheck) bool { return c.SequenceViolations > 0 }},
	}
	for name, m := range muts {
		if c := AnalyzeWordCounts(exp, m.got()); !m.check(c) {
			t.Errorf("%s: not detected: %+v", name, c)
		}
	}
	if streamsWords(7) != streamsWords(7) || len(strings.Fields(streamsWords(12))) != 6 {
		t.Error("word generator must be deterministic, six words per line")
	}
}
