package probe

// Verifier mutation tests for the features track (docs/methodology.md): each injects one
// defect into a known-good input and proves the verifier reports it.

import (
	"strings"
	"testing"
)

func goodRoundTrip() ([]Sent, []Got) {
	sent := spread(12, 3)
	sent[0].Headers = [][2][]byte{{[]byte("a"), []byte("1")}, {[]byte("n"), nil}}
	var got []Got
	pos := map[int32]int64{}
	for _, s := range sent {
		got = append(got, Got{Partition: s.Partition, Offset: pos[s.Partition], Key: s.Key, Value: s.Value, Headers: s.Headers})
		pos[s.Partition]++
	}
	return sent, got
}

func TestMutation_RoundTripDetectsEveryDefect(t *testing.T) {
	sent, got := goodRoundTrip()
	clean := AnalyzeRoundTrip(sent, got)
	if clean.Missing != 0 || clean.Duplicates != 0 || clean.OutOfOrder != 0 || !clean.OffsetsFromZero || !clean.ContentMatches || clean.Consumed != 12 {
		t.Fatalf("control: faithful round trip must be clean: %+v", clean)
	}
	clone := func() []Got { return append([]Got(nil), got...) }
	muts := map[string]func() []Got{
		"loss":      func() []Got { g := clone(); return append(g[:4:4], g[5:]...) },
		"duplicate": func() []Got { g := clone(); return append(g, g[4]) },
		"reorder": func() []Got {
			g := clone()
			// records 0 and 3 are both in partition 0: swap their content, keep offsets
			g[0].Key, g[0].Value, g[0].Headers, g[3].Key, g[3].Value, g[3].Headers = g[3].Key, g[3].Value, g[3].Headers, g[0].Key, g[0].Value, g[0].Headers
			return g
		},
		"wrong offset": func() []Got { g := clone(); g[6].Offset += 1; return g },
		"wrong value":  func() []Got { g := clone(); g[2].Value = []byte("x"); return g },
		"null header as empty": func() []Got {
			g := clone()
			g[0].Headers = [][2][]byte{{[]byte("a"), []byte("1")}, {[]byte("n"), []byte{}}}
			return g
		},
	}
	for name, m := range muts {
		rt := AnalyzeRoundTrip(sent, m())
		if _, same := Diff(clean.Observation(), rt.Observation()); same {
			t.Errorf("round-trip verifier stayed green under %s: %+v", name, rt)
		}
	}
}

func TestMutation_DiffDetectsWrongErrorCode(t *testing.T) {
	oracle := Observation{"limited_20KiB_error_code": 10, "default_1200KiB_error_code": 10}
	kubemq := Observation{"limited_20KiB_error_code": 0, "default_1200KiB_error_code": 10}
	diff, same := Diff(oracle, kubemq)
	if same || diff != "limited_20KiB_error_code: Kafka 10, KubeMQ 0" {
		t.Fatalf("got %q, %v", diff, same)
	}
	if _, same := Diff(oracle, Observation{"limited_20KiB_error_code": 10}); same {
		t.Fatal("a missing field must differ")
	}
	if _, same := Diff(oracle, Observation{"limited_20KiB_error_code": 10, "default_1200KiB_error_code": 10}); !same {
		t.Fatal("control: equal observations must be identical")
	}
}

func TestDiffIsDeterministic(t *testing.T) {
	a := Observation{"b": 1, "a": []string{"x"}, "c": true}
	b := Observation{"c": false, "a": []string{"y"}, "b": 2}
	first, _ := Diff(a, b)
	for i := 0; i < 20; i++ {
		if d, _ := Diff(a, b); d != first {
			t.Fatalf("diff text must be stable for gap binding: %q vs %q", d, first)
		}
	}
	if !strings.HasPrefix(first, "a: ") {
		t.Fatalf("fields must be sorted: %q", first)
	}
}

// Test vectors from Apache Kafka's UtilsTest.testMurmur2.
func TestMurmur2MatchesKafka(t *testing.T) {
	cases := map[string]int32{
		"21": -973932308, "foobar": -790332482, "a-little-bit-long-string": -985981536,
		"a-little-bit-longer-string": -1486304829, "lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8": -58897971,
		"abc": 479470107,
	}
	for in, want := range cases {
		if got := int32(murmur2([]byte(in))); got != want {
			t.Errorf("murmur2(%q) = %d, want %d", in, got, want)
		}
	}
}

// The false PASS found while building the codec rows: records sent one at a time were never
// compressed, so both brokers returned "none" and the rows matched. The precondition must turn
// that into an oracle failure (VOID), never a PASS.
func TestMutation_UncompressedCodecRunIsNotAPass(t *testing.T) {
	for _, codec := range []string{"gzip", "snappy", "lz4", "zstd"} {
		uncompressed := Observation{"stored_codecs": []string{"none"}}
		if err := CodecPrecondition("real-kafka", uncompressed, codec); err == nil {
			t.Errorf("%s: an oracle run that never compressed must fail the precondition", codec)
		}
		mixed := Observation{"stored_codecs": []string{"none", codec}}
		if err := CodecPrecondition("real-kafka", mixed, codec); err == nil {
			t.Errorf("%s: partly uncompressed oracle run must fail the precondition", codec)
		}
		if err := CodecPrecondition("real-kafka", Observation{"stored_codecs": []string{codec}}, codec); err != nil {
			t.Errorf("%s: control: compressed oracle run must pass the precondition: %v", codec, err)
		}
		if err := CodecPrecondition("kubemq", uncompressed, codec); err != nil {
			t.Errorf("%s: the precondition binds the oracle only; KubeMQ differences are verdicts: %v", codec, err)
		}
	}
}
