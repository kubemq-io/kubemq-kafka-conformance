package conformance

// Verifier mutation tests (docs/methodology.md, "Verifier mutation tests").
//
// Each test takes a known-good input, injects exactly one defect — a lost record, a duplicated
// record, a reordered record, a wrong offset, a wrong error code — and proves the verifier
// reports a failure. A verifier that stays green under any of these mutations is broken.
// These tests need no broker and run on every pull request.
//
// Verifiers covered:
//   - the full-transcript diff (CompareTranscripts), on real oracle transcripts captured from
//     Apache Kafka 4.3.1 for the core, multi-partition and transactions scenarios
//     (testdata/mutation/*.oracle.json);
//   - the scenario invariants (InvariantProblems);
//   - the smoke round-trip verifiers of sarama and segmentio, and segmentio's group-resume check;
//   - the wrong error code on the wire, end to end: each driver runs against franz-go's kfake
//     broker with one Produce answered with an error code, and the row must not read PASS.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func loadOracleFixture(t *testing.T, name string) Transcript {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "mutation", name+".oracle.json"))
	if err != nil {
		t.Fatal(err)
	}
	var tr Transcript
	if err := json.Unmarshal(b, &tr); err != nil {
		t.Fatal(err)
	}
	if len(tr.Events) == 0 {
		t.Fatalf("fixture %s has no events", name)
	}
	return tr
}

func cloneTranscript(tr Transcript) Transcript {
	out := tr
	out.Events = append([]Event(nil), tr.Events...)
	return out
}

// fetchIndexes returns the indexes of successful offset-bearing FetchFrom events.
func fetchIndexes(tr Transcript) []int {
	var idx []int
	for i, e := range tr.Events {
		if e.Action == string(ActionFetchFrom) && e.ErrCode == 0 && e.RelOffset >= 0 {
			idx = append(idx, i)
		}
	}
	return idx
}

type transcriptMutation struct {
	name   string
	mutate func(t *testing.T, tr Transcript) Transcript
}

var transcriptMutations = []transcriptMutation{
	{"loss: one fetched record dropped", func(t *testing.T, tr Transcript) Transcript {
		i := fetchIndexes(tr)[len(fetchIndexes(tr))/2]
		tr.Events = append(tr.Events[:i:i], tr.Events[i+1:]...)
		return tr
	}},
	{"duplicate: one fetched record delivered twice", func(t *testing.T, tr Transcript) Transcript {
		i := fetchIndexes(tr)[len(fetchIndexes(tr))/2]
		ev := append([]Event(nil), tr.Events[:i+1]...)
		ev = append(ev, tr.Events[i])
		tr.Events = append(ev, tr.Events[i+1:]...)
		return tr
	}},
	{"reorder: two fetched records swapped", func(t *testing.T, tr Transcript) Transcript {
		f := fetchIndexes(tr)
		for k := 0; k+1 < len(f); k++ {
			a, b := tr.Events[f[k]], tr.Events[f[k+1]]
			if a.ValueSHA != b.ValueSHA || string(a.Key) != string(b.Key) {
				tr.Events[f[k]], tr.Events[f[k+1]] = b, a
				return tr
			}
		}
		t.Fatal("no two distinct consecutive fetch events to swap")
		return tr
	}},
	{"wrong offset: one fetched record one offset off", func(t *testing.T, tr Transcript) Transcript {
		f := fetchIndexes(tr)
		tr.Events[f[len(f)-1]].RelOffset++
		return tr
	}},
	{"wrong offset: a produce ack one offset off", func(t *testing.T, tr Transcript) Transcript {
		for i := len(tr.Events) - 1; i >= 0; i-- {
			if tr.Events[i].Action == string(ActionProduce) && tr.Events[i].ErrCode == 0 && tr.Events[i].RelOffset > 0 {
				tr.Events[i].RelOffset++
				return tr
			}
		}
		t.Fatal("no produce ack to mutate")
		return tr
	}},
	{"wrong offset: every offset shifted (fresh topic not starting at 0)", func(t *testing.T, tr Transcript) Transcript {
		for i := range tr.Events {
			if (tr.Events[i].Action == string(ActionProduce) || tr.Events[i].Action == string(ActionFetchFrom)) && tr.Events[i].ErrCode == 0 && tr.Events[i].RelOffset >= 0 {
				tr.Events[i].RelOffset++
			}
		}
		return tr
	}},
	{"wrong error code: a produce answered with INVALID_RECORD", func(t *testing.T, tr Transcript) Transcript {
		for i := range tr.Events {
			if tr.Events[i].Action == string(ActionProduce) {
				tr.Events[i].ErrCode = 87
				return tr
			}
		}
		t.Fatal("no produce event")
		return tr
	}},
	{"wrong error code: a fetch answered with NOT_LEADER_OR_FOLLOWER", func(t *testing.T, tr Transcript) Transcript {
		f := fetchIndexes(tr)
		tr.Events[f[0]].ErrCode = 6
		return tr
	}},
	{"wrong value: one fetched record's value differs", func(t *testing.T, tr Transcript) Transcript {
		f := fetchIndexes(tr)
		tr.Events[f[0]].ValueSHA = HashValue([]byte("mutated"))
		return tr
	}},
}

func TestMutation_TranscriptDiffDetectsEveryDefect(t *testing.T) {
	for _, scenario := range []string{"core", "multipartition", "transactions"} {
		oracle := loadOracleFixture(t, scenario)
		if r := CompareTranscripts(oracle, cloneTranscript(oracle)); r.Verdict != VerdictIdentical {
			t.Fatalf("%s: control failed, an unmutated copy must read identical:\n%s", scenario, r.Diff)
		}
		for _, m := range transcriptMutations {
			t.Run(scenario+"/"+m.name, func(t *testing.T) {
				mutated := m.mutate(t, cloneTranscript(oracle))
				r := CompareTranscripts(oracle, mutated)
				if r.Verdict == VerdictIdentical {
					t.Fatalf("verifier stayed green under mutation %q", m.name)
				}
			})
		}
	}
}

func TestMutation_InvariantsDetectDefects(t *testing.T) {
	core := loadOracleFixture(t, "core")
	if p := InvariantProblems(ScenarioCore().Name, core); len(p) != 0 {
		t.Fatalf("control: core fixture must satisfy its invariants: %v", p)
	}
	shifted := cloneTranscript(core)
	for i := range shifted.Events {
		if shifted.Events[i].Action == string(ActionFetchFrom) || shifted.Events[i].Action == string(ActionProduce) {
			shifted.Events[i].RelOffset++
		}
	}
	if p := InvariantProblems(ScenarioCore().Name, shifted); len(p) == 0 {
		t.Error("fresh topic starting at offset 1 must break the base-offset invariant")
	}

	mp := loadOracleFixture(t, "multipartition")
	if p := InvariantProblems(ScenarioMultiPartition().Name, mp); len(p) != 0 {
		t.Fatalf("control: multi-partition fixture must satisfy its invariants: %v", p)
	}
	reordered := cloneTranscript(mp)
	swapped := false
	f := fetchIndexes(reordered)
	for k := 0; k+1 < len(f) && !swapped; k++ {
		a, b := reordered.Events[f[k]], reordered.Events[f[k+1]]
		// The broker hands back two records in the wrong order at ascending offsets: offsets stay,
		// the records behind them swap.
		if a.Partition == b.Partition && string(a.Key) != string(b.Key) {
			reordered.Events[f[k]].Key, reordered.Events[f[k+1]].Key = b.Key, a.Key
			swapped = true
		}
	}
	if !swapped {
		t.Fatal("no same-partition pair to reorder")
	}
	if p := InvariantProblems(ScenarioMultiPartition().Name, reordered); len(p) == 0 {
		t.Error("per-key reorder within a partition must break the key-order invariant")
	}

	txn := loadOracleFixture(t, "transactions")
	if p := InvariantProblems(ScenarioTransactions().Name, txn); len(p) != 0 {
		t.Fatalf("control: transactions fixture must satisfy its invariants: %v", p)
	}
	leaked := cloneTranscript(txn)
	f = fetchIndexes(leaked)
	leaked.Events[f[0]].Key = abortedTxnKey(0)
	if p := InvariantProblems(ScenarioTransactions().Name, leaked); len(p) == 0 {
		t.Error("an aborted record delivered under read_committed must break the client-filtering invariant")
	}
	lost := cloneTranscript(txn)
	i := f[0]
	lost.Events = append(lost.Events[:i:i], lost.Events[i+1:]...)
	if p := InvariantProblems(ScenarioTransactions().Name, lost); len(p) == 0 {
		t.Error("a committed record lost from the read_committed read-back must break the invariant")
	}
}

// smokeRecords builds a produced batch and its faithful consumption.
func smokeRecords(n int) ([]Record, []saramaConsumedRecord, []segmentioConsumedRecord) {
	var produced []Record
	var sa []saramaConsumedRecord
	var sg []segmentioConsumedRecord
	for i := 0; i < n; i++ {
		r := Record{Key: []byte(fmt.Sprintf("k-%d", i)), Value: []byte(fmt.Sprintf("v-%d", i)), Headers: []RecordHeader{{Key: "h", Value: []byte{byte(i)}}}}
		produced = append(produced, r)
		sa = append(sa, saramaConsumedRecord{Key: r.Key, Value: r.Value, Headers: r.Headers, Offset: int64(i)})
		sg = append(sg, segmentioConsumedRecord{Key: r.Key, Value: r.Value, Headers: r.Headers, Offset: int64(i)})
	}
	return produced, sa, sg
}

func TestMutation_SmokeVerifiersDetectEveryDefect(t *testing.T) {
	produced, sa, sg := smokeRecords(6)
	if _, err := saramaConsumedToEvents(sa, produced, "t"); err != nil {
		t.Fatalf("control: sarama faithful round-trip must pass: %v", err)
	}
	if _, err := segmentioConsumedToEvents(sg, produced, "t"); err != nil {
		t.Fatalf("control: segmentio faithful round-trip must pass: %v", err)
	}
	type mut struct {
		name string
		sa   func([]saramaConsumedRecord) []saramaConsumedRecord
		sg   func([]segmentioConsumedRecord) []segmentioConsumedRecord
	}
	muts := []mut{
		{"loss", func(c []saramaConsumedRecord) []saramaConsumedRecord { return append(c[:2:2], c[3:]...) },
			func(c []segmentioConsumedRecord) []segmentioConsumedRecord { return append(c[:2:2], c[3:]...) }},
		{"duplicate", func(c []saramaConsumedRecord) []saramaConsumedRecord {
			return append(append(c[:3:3], c[2]), c[3:len(c)-1]...)
		}, func(c []segmentioConsumedRecord) []segmentioConsumedRecord {
			return append(append(c[:3:3], c[2]), c[3:len(c)-1]...)
		}},
		{"reorder", func(c []saramaConsumedRecord) []saramaConsumedRecord {
			c = append([]saramaConsumedRecord(nil), c...)
			c[1], c[2] = c[2], c[1]
			return c
		}, func(c []segmentioConsumedRecord) []segmentioConsumedRecord {
			c = append([]segmentioConsumedRecord(nil), c...)
			c[1], c[2] = c[2], c[1]
			return c
		}},
		{"wrong offset", func(c []saramaConsumedRecord) []saramaConsumedRecord {
			c = append([]saramaConsumedRecord(nil), c...)
			c[4].Offset = 7
			return c
		}, func(c []segmentioConsumedRecord) []segmentioConsumedRecord {
			c = append([]segmentioConsumedRecord(nil), c...)
			c[4].Offset = 7
			return c
		}},
		{"wrong header", func(c []saramaConsumedRecord) []saramaConsumedRecord {
			c = append([]saramaConsumedRecord(nil), c...)
			c[0].Headers = nil
			return c
		}, func(c []segmentioConsumedRecord) []segmentioConsumedRecord {
			c = append([]segmentioConsumedRecord(nil), c...)
			c[0].Headers = nil
			return c
		}},
	}
	for _, m := range muts {
		if _, err := saramaConsumedToEvents(m.sa(sa), produced, "t"); err == nil {
			t.Errorf("sarama verifier stayed green under %s", m.name)
		}
		if _, err := segmentioConsumedToEvents(m.sg(sg), produced, "t"); err == nil {
			t.Errorf("segmentio verifier stayed green under %s", m.name)
		}
	}

	// Group resume after a commit at offset 3.
	rest := append([]segmentioConsumedRecord(nil), sg[3:]...)
	if err := segmentioCheckResume(rest, produced, 3); err != nil {
		t.Fatalf("control: faithful resume must pass: %v", err)
	}
	for name, bad := range map[string][]segmentioConsumedRecord{
		"resume one too early (duplicate)": sg[2:],
		"resume one too late (loss)":       sg[4:],
		"reordered after resume":           {sg[3], sg[5], sg[4]},
		"wrong offset after resume":        {sg[3], sg[4], {Key: sg[5].Key, Value: sg[5].Value, Headers: sg[5].Headers, Offset: 9}},
	} {
		if err := segmentioCheckResume(bad, produced, 3); err == nil {
			t.Errorf("segmentio resume check stayed green under %s", name)
		}
	}
}

func TestMutation_WrongErrorCodeOnTheWire(t *testing.T) {
	const invalidRecord = 10 // MESSAGE_TOO_LARGE: non-retriable in every client
	// The franz-go driver's per-step bound is shrunk so the record that never arrives is noticed
	// in seconds, not after the production bound of a minute.
	for _, d := range []Driver{&FranzGoDriver{stepTimeout: 5 * time.Second}, NewSaramaDriver(), NewSegmentioDriver()} {
		t.Run(d.Name(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			clean, err := NewKfakeTarget()
			if err != nil {
				t.Fatal(err)
			}
			defer clean.Close()
			want, err := runDriver(ctx, d, ScenarioCore(), clean)
			if err != nil {
				t.Fatalf("control: clean kfake run must succeed: %v", err)
			}

			bad, err := NewKfakeTarget()
			if err != nil {
				t.Fatal(err)
			}
			defer bad.Close()
			bad.InjectProduceError(invalidRecord)
			got, err := runDriver(ctx, d, ScenarioCore(), bad)
			if err != nil {
				return // the leg fails: the row is FAIL, never PASS
			}
			if d.Kind() == DriverKindSmoke {
				t.Fatalf("smoke driver %s succeeded although the broker answered a Produce with error %d", d.Name(), invalidRecord)
			}
			if r := CompareTranscripts(want, got); r.Verdict == VerdictIdentical {
				t.Fatalf("full transcript stayed identical although the broker answered a Produce with error %d", invalidRecord)
			}
		})
	}
}
