package conformance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleTranscript(target string, offsets []int64) Transcript {
	events := make([]Event, 0, len(offsets))
	for _, off := range offsets {
		events = append(events, Event{
			Action:    string(ActionFetchFrom),
			Topic:     "run-" + target + "-core",
			Partition: 0,
			Key:       []byte("k"),
			ValueSHA:  HashValue([]byte("v")),
			RelOffset: off,
		})
	}
	return Transcript{
		Driver:       "kgo",
		DriverKind:   DriverKindFullTranscript,
		Target:       target,
		LogicalTopic: "core",
		Events:       events,
	}
}

func TestCompareTranscripts_Identical(t *testing.T) {
	left := sampleTranscript("real-kafka", []int64{0, 1, 2})
	right := sampleTranscript("kfake", []int64{0, 1, 2})

	got := CompareTranscripts(left, right)

	assert.Equal(t, VerdictIdentical, got.Verdict)
	assert.Empty(t, got.Diff)
	assert.Empty(t, got.InvariantErrors)
	assert.Equal(t, "real-kafka", got.Left)
	assert.Equal(t, "kfake", got.Right)
}

func TestCompareTranscripts_IdenticalAfterBaseRelativeNormalization(t *testing.T) {
	// Two targets that assign different absolute starting offsets for the same logical
	// sequence must still compare identical once normalized. Uses ActionProduce (not
	// FetchFrom) so the separate absolute baseOffset==0 invariant — which legitimately DOES
	// require every FetchFrom stream to start at 0 — is not in play here; that invariant is
	// covered by its own tests below.
	left := Transcript{Driver: "kgo", Target: "real-kafka", LogicalTopic: "core", Events: []Event{
		{Action: string(ActionProduce), Topic: "run-a-core", Partition: 0, Key: []byte("k"), ValueSHA: HashValue([]byte("v")), RelOffset: 0},
		{Action: string(ActionProduce), Topic: "run-a-core", Partition: 0, Key: []byte("k"), ValueSHA: HashValue([]byte("v")), RelOffset: 1},
		{Action: string(ActionProduce), Topic: "run-a-core", Partition: 0, Key: []byte("k"), ValueSHA: HashValue([]byte("v")), RelOffset: 2},
	}}
	right := Transcript{Driver: "kgo", Target: "kfake", LogicalTopic: "core", Events: []Event{
		{Action: string(ActionProduce), Topic: "run-b-core", Partition: 0, Key: []byte("k"), ValueSHA: HashValue([]byte("v")), RelOffset: 500},
		{Action: string(ActionProduce), Topic: "run-b-core", Partition: 0, Key: []byte("k"), ValueSHA: HashValue([]byte("v")), RelOffset: 501},
		{Action: string(ActionProduce), Topic: "run-b-core", Partition: 0, Key: []byte("k"), ValueSHA: HashValue([]byte("v")), RelOffset: 502},
	}}

	got := CompareTranscripts(left, right)

	assert.Equal(t, VerdictIdentical, got.Verdict)
	assert.Empty(t, got.InvariantErrors)
}

func TestCompareTranscripts_DiffOnMismatchedEvents(t *testing.T) {
	left := sampleTranscript("real-kafka", []int64{0, 1, 2})
	right := sampleTranscript("kfake", []int64{0, 1, 3}) // gap differs

	got := CompareTranscripts(left, right)

	assert.Equal(t, VerdictDiff, got.Verdict)
	assert.NotEmpty(t, got.Diff)
}

func TestCompareTranscripts_DiffOnDifferentEventCount(t *testing.T) {
	left := sampleTranscript("real-kafka", []int64{0, 1, 2})
	right := sampleTranscript("kfake", []int64{0, 1})

	got := CompareTranscripts(left, right)

	assert.Equal(t, VerdictDiff, got.Verdict)
	assert.NotEmpty(t, got.Diff)
}

func TestCompareTranscripts_InvariantViolationForcesDiff(t *testing.T) {
	// Both sides normalize identically to each other (same relative offsets), but the LEFT
	// side leaked a 1-based absolute start — the invariant check (on raw offsets) must catch
	// this even though the cross-transcript diff alone would not.
	left := sampleTranscript("leaky-target", []int64{1, 2, 3})
	right := sampleTranscript("kfake", []int64{0, 1, 2})

	got := CompareTranscripts(left, right)

	assert.Equal(t, VerdictDiff, got.Verdict)
	require.NotEmpty(t, got.InvariantErrors)
	assert.True(t, strings.Contains(got.InvariantErrors[0], "leaky-target"))
}

func TestCompareTranscripts_InvariantViolationOnRightSideAlsoDetected(t *testing.T) {
	// Mirror of the left-side case: the RIGHT transcript's invariant check must run too, not
	// just the left's.
	left := sampleTranscript("real-kafka", []int64{0, 1, 2})
	right := sampleTranscript("leaky-target", []int64{1, 2, 3})

	got := CompareTranscripts(left, right)

	assert.Equal(t, VerdictDiff, got.Verdict)
	require.NotEmpty(t, got.InvariantErrors)
	assert.True(t, strings.Contains(got.InvariantErrors[0], "leaky-target"))
}

func TestCompareTranscripts_BothSidesViolateInvariant(t *testing.T) {
	left := sampleTranscript("leaky-left", []int64{1, 2, 3})
	right := sampleTranscript("leaky-right", []int64{2, 3, 4})

	got := CompareTranscripts(left, right)

	assert.Equal(t, VerdictDiff, got.Verdict)
	require.Len(t, got.InvariantErrors, 2)
}

func TestCompareTranscripts_NoFetchEventsDoesNotFalselyViolateInvariant(t *testing.T) {
	// A transcript with only Produce events (no FetchFrom) must not be treated as an invariant
	// violation by CompareTranscripts (ErrNoEvents is explicitly not a violation) — only an
	// actual nonzero base offset is.
	left := Transcript{Driver: "kgo", Target: "real-kafka", LogicalTopic: "core", Events: []Event{
		{Action: string(ActionProduce), Topic: "run-a-core", Partition: 0, RelOffset: 0},
	}}
	right := Transcript{Driver: "kgo", Target: "kfake", LogicalTopic: "core", Events: []Event{
		{Action: string(ActionProduce), Topic: "run-b-core", Partition: 0, RelOffset: 0},
	}}

	got := CompareTranscripts(left, right)

	assert.Empty(t, got.InvariantErrors)
	assert.Equal(t, VerdictIdentical, got.Verdict)
}

func TestAggregateVerdict(t *testing.T) {
	tests := []struct {
		name        string
		comparisons []ComparisonReport
		smoke       []SmokeReport
		want        Verdict
	}{
		{
			name: "empty report is infra, never identical",
			want: VerdictInfra,
		},
		{
			name:        "all identical",
			comparisons: []ComparisonReport{{Verdict: VerdictIdentical}},
			smoke:       []SmokeReport{{Verdict: VerdictIdentical}},
			want:        VerdictIdentical,
		},
		{
			name:        "one diff among comparisons wins over identical smoke",
			comparisons: []ComparisonReport{{Verdict: VerdictIdentical}, {Verdict: VerdictDiff}},
			smoke:       []SmokeReport{{Verdict: VerdictIdentical}},
			want:        VerdictDiff,
		},
		{
			name:        "infra beats diff",
			comparisons: []ComparisonReport{{Verdict: VerdictDiff}},
			smoke:       []SmokeReport{{Verdict: VerdictInfra}},
			want:        VerdictInfra,
		},
		{
			name:        "infra in comparisons beats diff in smoke",
			comparisons: []ComparisonReport{{Verdict: VerdictInfra}},
			smoke:       []SmokeReport{{Verdict: VerdictDiff}},
			want:        VerdictInfra,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := aggregateVerdict(tt.comparisons, tt.smoke)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBuildReport(t *testing.T) {
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	comparisons := []ComparisonReport{{Left: "real-kafka", Right: "kfake", Verdict: VerdictIdentical}}
	smoke := []SmokeReport{{Driver: "kcat", Target: "real-kafka", Verdict: VerdictIdentical}}

	report := BuildReport("core", comparisons, smoke, now)

	assert.Equal(t, "core", report.Scenario)
	assert.Equal(t, VerdictIdentical, report.Verdict)
	assert.Equal(t, "2026-07-02T12:00:00Z", report.GeneratedAt)
	assert.Equal(t, comparisons, report.Comparisons)
	assert.Equal(t, smoke, report.Smoke)
}

func TestToComparisonReport(t *testing.T) {
	d := DiffResult{
		Verdict:         VerdictDiff,
		Left:            "real-kafka",
		Right:           "kfake",
		Diff:            "some diff text",
		InvariantErrors: []string{"violation 1"},
	}

	got := ToComparisonReport(d)

	assert.Equal(t, "real-kafka", got.Left)
	assert.Equal(t, "kfake", got.Right)
	assert.Equal(t, VerdictDiff, got.Verdict)
	assert.Equal(t, "some diff text", got.Diff)
	assert.Equal(t, []string{"violation 1"}, got.InvariantErrors)
}

func TestWriteReportJSON_RoundTrips(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out", "report.json")

	report := BuildReport("core",
		[]ComparisonReport{{Left: "real-kafka", Right: "kfake", Verdict: VerdictIdentical}},
		nil,
		time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC),
	)

	err := WriteReportJSON(path, report)
	require.NoError(t, err)

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var got Report
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, report.Verdict, got.Verdict)
	assert.Equal(t, report.Scenario, got.Scenario)
	require.Len(t, got.Comparisons, 1)
	assert.Equal(t, "real-kafka", got.Comparisons[0].Left)
}

func TestWriteReportJSON_VerdictFieldPresentInRawJSON(t *testing.T) {
	// Guard the exact contract run.sh depends on: report.json must carry a top-level
	// "verdict" field readable without decoding the whole struct.
	dir := t.TempDir()
	path := filepath.Join(dir, "report.json")

	report := BuildReport("core", []ComparisonReport{{Verdict: VerdictDiff}}, nil, time.Now())
	require.NoError(t, WriteReportJSON(path, report))

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var raw map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, string(VerdictDiff), raw["verdict"])
}

func TestWriteReportJSON_CreatesParentDirs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "deeper", "report.json")

	report := BuildReport("core", nil, []SmokeReport{{Verdict: VerdictIdentical}}, time.Now())
	require.NoError(t, WriteReportJSON(path, report))

	_, err := os.Stat(path)
	assert.NoError(t, err)
}

func TestWriteReportJSON_ErrorsWhenParentDirCannotBeCreated(t *testing.T) {
	// Make a regular file, then try to write a report "inside" it (as if it were a directory)
	// — MkdirAll must fail, and WriteReportJSON must surface that error rather than panicking
	// or silently succeeding.
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o644))

	path := filepath.Join(blocker, "sub", "report.json")
	report := BuildReport("core", nil, []SmokeReport{{Verdict: VerdictIdentical}}, time.Now())

	err := WriteReportJSON(path, report)

	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "create report dir"))
}

func TestWriteReportJSON_ErrorsWhenFileCannotBeWritten(t *testing.T) {
	// Point path AT a directory (not a file inside it) — os.WriteFile must fail because the
	// target is a directory, and WriteReportJSON must surface that error.
	dir := t.TempDir()
	asDir := filepath.Join(dir, "report.json")
	require.NoError(t, os.Mkdir(asDir, 0o755))

	report := BuildReport("core", nil, []SmokeReport{{Verdict: VerdictIdentical}}, time.Now())

	err := WriteReportJSON(asDir, report)

	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "write report json"))
}

func TestWriteReportMarkdown_ErrorsWhenParentDirCannotBeCreated(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o644))

	path := filepath.Join(blocker, "sub", "report.md")
	report := BuildReport("core", nil, []SmokeReport{{Verdict: VerdictIdentical}}, time.Now())

	err := WriteReportMarkdown(path, report)

	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "create report dir"))
}

func TestWriteReportMarkdown_ErrorsWhenFileCannotBeWritten(t *testing.T) {
	dir := t.TempDir()
	asDir := filepath.Join(dir, "report.md")
	require.NoError(t, os.Mkdir(asDir, 0o755))

	report := BuildReport("core", nil, []SmokeReport{{Verdict: VerdictIdentical}}, time.Now())

	err := WriteReportMarkdown(asDir, report)

	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "write report md"))
}

func TestRenderReportMarkdown_ContainsVerdictAndScenario(t *testing.T) {
	report := BuildReport("core",
		[]ComparisonReport{{Left: "real-kafka", Right: "kfake", Verdict: VerdictDiff, Diff: "- a\n+ b"}},
		[]SmokeReport{{Driver: "kcat", Target: "real-kafka", Verdict: VerdictIdentical, Detail: "produce+consume ok"}},
		time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC),
	)

	md := RenderReportMarkdown(report)

	assert.True(t, strings.Contains(md, "core"))
	assert.True(t, strings.Contains(md, string(VerdictDiff)))
	assert.True(t, strings.Contains(md, "real-kafka vs kfake"))
	assert.True(t, strings.Contains(md, "- a\n+ b"))
	assert.True(t, strings.Contains(md, "kcat"))
	assert.True(t, strings.Contains(md, "produce+consume ok"))
}

func TestRenderReportMarkdown_RendersInvariantViolations(t *testing.T) {
	report := BuildReport("core",
		[]ComparisonReport{{
			Left:            "leaky-target",
			Right:           "kfake",
			Verdict:         VerdictDiff,
			InvariantErrors: []string{"target=leaky-target topic=core partition=0: first fetched baseOffset=1, want 0"},
		}},
		nil,
		time.Now(),
	)

	md := RenderReportMarkdown(report)

	assert.True(t, strings.Contains(md, "**Invariant violations:**"))
	assert.True(t, strings.Contains(md, "first fetched baseOffset=1, want 0"))
}

func TestRenderReportMarkdown_EmptySectionsNoteNoneRecorded(t *testing.T) {
	report := BuildReport("core", nil, nil, time.Now())

	md := RenderReportMarkdown(report)

	assert.True(t, strings.Contains(md, "_none recorded_"))
}

func TestWriteReportMarkdown_WritesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out", "report.md")

	report := BuildReport("core", []ComparisonReport{{Verdict: VerdictIdentical}}, nil, time.Now())
	require.NoError(t, WriteReportMarkdown(path, report))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.True(t, strings.Contains(string(data), "# Kafka Conformance Report"))
}

func TestVerdictSeverity_Ordering(t *testing.T) {
	assert.True(t, verdictSeverity(VerdictInfra) > verdictSeverity(VerdictDiff))
	assert.True(t, verdictSeverity(VerdictDiff) > verdictSeverity(VerdictIdentical))
}

// TestWithInvariantProblems: an identical comparison with a per-target invariant failure must
// become a diff in the report (two identically wrong oracles are not "identical"), extra problems
// are appended not replacing, infra is never lowered, and no problems is a no-op.
func TestWithInvariantProblems(t *testing.T) {
	c := ComparisonReport{Left: "real-kafka", Right: "kfake", Verdict: VerdictIdentical}
	assert.Equal(t, c, WithInvariantProblems(c, nil))

	got := WithInvariantProblems(c, []string{"read_committed delivered a-00 on real-kafka"})
	assert.Equal(t, VerdictDiff, got.Verdict)
	assert.Equal(t, []string{"read_committed delivered a-00 on real-kafka"}, got.InvariantErrors)
	assert.Equal(t, VerdictDiff, BuildReport("txn", []ComparisonReport{got}, nil, time.Now()).Verdict,
		"the report file must not say identical when a per-target invariant failed")
	assert.Empty(t, c.InvariantErrors, "the input must not be mutated")

	prior := ComparisonReport{Verdict: VerdictDiff, InvariantErrors: []string{"x"}}
	assert.Equal(t, []string{"x", "y"}, WithInvariantProblems(prior, []string{"y"}).InvariantErrors)

	infra := ComparisonReport{Verdict: VerdictInfra}
	assert.Equal(t, VerdictInfra, WithInvariantProblems(infra, []string{"y"}).Verdict)
}
