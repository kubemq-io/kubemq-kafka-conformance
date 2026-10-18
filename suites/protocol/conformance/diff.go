package conformance

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
)

// Verdict is the machine-diffable classification report.json carries. run.sh reads
// report.json.verdict — NOT the go test exit code, which is only
// pass/fail — to decide its own exit code:
//
//	identical -> exit 0
//	diff      -> exit 1
//	infra     -> exit 2
//
// A driver/client error that is actually a broker divergence (the broker sent bytes the client
// rejects) is a Diff, never Infra — the classifier keys on WHERE the failure happened
// (container/network/podman tooling = Infra; a client-observed, broker-caused rejection =
// Diff), not on which layer surfaced it.
type Verdict string

const (
	// VerdictIdentical means the two normalized transcripts compared equal and the absolute
	// baseOffset invariant held on every target checked.
	VerdictIdentical Verdict = "identical"
	// VerdictDiff means the normalized transcripts differ, or the absolute baseOffset
	// invariant was violated, or a driver observed a broker-caused rejection.
	VerdictDiff Verdict = "diff"
	// VerdictInfra means the run could not produce a comparable result for reasons outside the
	// protocol itself (container/network/tooling failure, a target unreachable, etc.).
	VerdictInfra Verdict = "infra"
)

// DiffResult is the outcome of comparing two normalized transcripts (typically the primary
// oracle-parity diff: kgo-vs-real-Kafka against kgo-vs-kfake).
type DiffResult struct {
	// Verdict classifies the outcome; see Verdict.
	Verdict Verdict
	// Left/Right name the two sides being compared (e.g. Transcript.Target values) for
	// reporting.
	Left  string
	Right string
	// Diff is the human-readable go-cmp diff text; empty when Verdict is VerdictIdentical.
	Diff string
	// InvariantErrors holds any absolute baseOffset==0 invariant violations discovered on
	// either side (see CheckFreshTopicBaseOffsetZero). Non-empty forces Verdict to
	// VerdictDiff even if the normalized transcripts otherwise compare equal.
	InvariantErrors []string
}

// CompareTranscripts normalizes both transcripts, diffs their Events with go-cmp, and checks
// the absolute baseOffset==0 invariant on BOTH raw (pre-normalization) transcripts — per the
// normalize.go file doc, the invariant check must run on raw offsets, not the base-relative
// normalized ones.
//
// left/right should be a matched pair recording the SAME Scenario run against two different
// Targets by the same (FullTranscript) Driver — e.g. kgo-vs-real-Kafka and kgo-vs-kfake, the
// primary oracle-parity diff.
func CompareTranscripts(left, right Transcript) DiffResult {
	result := DiffResult{
		Verdict: VerdictIdentical,
		Left:    left.Target,
		Right:   right.Target,
	}

	var invariantErrs []string
	if err := CheckFreshTopicBaseOffsetZero(left); err != nil && !errors.Is(err, ErrNoEvents) {
		invariantErrs = append(invariantErrs, err.Error())
	}
	if err := CheckFreshTopicBaseOffsetZero(right); err != nil && !errors.Is(err, ErrNoEvents) {
		invariantErrs = append(invariantErrs, err.Error())
	}
	result.InvariantErrors = invariantErrs

	normLeft := Normalize(left)
	normRight := Normalize(right)

	diffText := cmp.Diff(normLeft.Events, normRight.Events)
	result.Diff = diffText

	if diffText != "" || len(invariantErrs) > 0 {
		result.Verdict = VerdictDiff
	}

	return result
}

// Report is the top-level structure serialized as report.json. Its Verdict field is the
// classification run.sh reads (see Verdict doc).
type Report struct {
	// GeneratedAt is when the report was produced, RFC3339 UTC.
	GeneratedAt string `json:"generated_at"`
	// Scenario is the Scenario.Name this report covers.
	Scenario string `json:"scenario"`
	// Verdict is the overall classification across every comparison/check in Comparisons and
	// Smoke. It is the WORST verdict observed (infra > diff > identical, in severity order) —
	// see aggregateVerdict.
	Verdict Verdict `json:"verdict"`
	// Comparisons holds one entry per full-transcript oracle-parity comparison performed
	// (kgo-vs-real-Kafka vs kgo-vs-kfake).
	Comparisons []ComparisonReport `json:"comparisons"`
	// Smoke holds one entry per Smoke-kind driver run (kcat, kafka-cli).
	Smoke []SmokeReport `json:"smoke"`
	// Oracle identifies the real Apache Kafka broker the oracle side ran against (image, version,
	// and its measured ApiVersions ceiling) — nil when no real Kafka took part. Two runs against
	// different Kafka versions write the same file names; this field is what tells them apart.
	Oracle *OracleIdentity `json:"oracle,omitempty"`
}

// OracleIdentity records WHICH real Apache Kafka broker an oracle leg ran against, and how that
// was established. See oracle_identity_test.go (resolveOracleIdentity) for how each field is
// obtained.
type OracleIdentity struct {
	// Addr is the KAFKA_ADDR the leg dialled.
	Addr string `json:"addr"`
	// Image is the broker image reference, e.g. "docker.io/apache/kafka:4.3.1".
	Image string `json:"image"`
	// ImageSource says where Image came from: "broker-container" (read from the running
	// container named by KAFKA_CONFORMANCE_BROKER_CONTAINER — measured), "KAFKA_IMAGE" (declared
	// by the caller) or "contract-default" (neither was given).
	ImageSource string `json:"image_source"`
	// DeclaredImage is KAFKA_IMAGE when it was set (cross-checked against the container's image).
	DeclaredImage string `json:"declared_image,omitempty"`
	// Container, ContainerImageID and ContainerImageDigest describe the broker container when one
	// was named.
	Container            string `json:"container,omitempty"`
	ContainerImageID     string `json:"container_image_id,omitempty"`
	ContainerImageDigest string `json:"container_image_digest,omitempty"`
	// Version is Image's tag when it is a version ("4.3.1", "4.4.0-rc1"), else "".
	Version string `json:"version"`
	// ApiVersionsMax is the broker's ApiVersions (key 18) ceiling MEASURED on Addr with a raw v3
	// round-trip; -1 when the measurement failed.
	ApiVersionsMax int16 `json:"apiversions_max"`
	// ExpectedApiVersionsMax is the ceiling Version must advertise (4.3.x: 4, 4.4.x: 5); -1 when
	// Version is not in the pinned table (then the measured value is recorded but not checked).
	ExpectedApiVersionsMax int16 `json:"expected_apiversions_max"`
	// Problems lists every reason the identity is not proven (image mismatch, wrong ceiling,
	// unreachable container, unpublished port). A leg whose oracle identity has problems must fail.
	Problems []string `json:"problems,omitempty"`
}

// WithOracleIdentity attaches id to r. When id has problems the report can no longer say which
// Kafka it compared against, so an "identical" verdict is raised to infra; a diff stays a diff (a
// real divergence is never masked).
func WithOracleIdentity(r Report, id *OracleIdentity) Report {
	r.Oracle = id
	if id != nil && len(id.Problems) > 0 && r.Verdict == VerdictIdentical {
		r.Verdict = VerdictInfra
	}
	return r
}

// ComparisonReport is the report.json shape for one DiffResult.
type ComparisonReport struct {
	Left            string   `json:"left"`
	Right           string   `json:"right"`
	Verdict         Verdict  `json:"verdict"`
	Diff            string   `json:"diff,omitempty"`
	InvariantErrors []string `json:"invariant_errors,omitempty"`
}

// SmokeReport is the report.json shape for one Smoke-driver result.
type SmokeReport struct {
	Driver  string  `json:"driver"`
	Target  string  `json:"target"`
	Verdict Verdict `json:"verdict"`
	Detail  string  `json:"detail,omitempty"`
	// ClientVersion is the client library version the driver proved it ran (Transcript.ClientVersion),
	// e.g. "kcat 1.7.1, librdkafka 2.14.1 (...)" or "github.com/IBM/sarama v1.50.3".
	ClientVersion string `json:"client_version,omitempty"`
}

// verdictSeverity ranks verdicts for aggregation: infra is worst (something outside the
// protocol broke and later results may be meaningless), diff is next, identical is best.
func verdictSeverity(v Verdict) int {
	switch v {
	case VerdictInfra:
		return 2
	case VerdictDiff:
		return 1
	default:
		return 0
	}
}

// aggregateVerdict returns the worst (highest-severity) verdict among the given comparison and
// smoke results. An empty report (no comparisons, no smoke entries) aggregates to VerdictInfra
// — a report claiming nothing was checked must never present as VerdictIdentical.
func aggregateVerdict(comparisons []ComparisonReport, smoke []SmokeReport) Verdict {
	if len(comparisons) == 0 && len(smoke) == 0 {
		return VerdictInfra
	}
	worst := VerdictIdentical
	for _, c := range comparisons {
		if verdictSeverity(c.Verdict) > verdictSeverity(worst) {
			worst = c.Verdict
		}
	}
	for _, s := range smoke {
		if verdictSeverity(s.Verdict) > verdictSeverity(worst) {
			worst = s.Verdict
		}
	}
	return worst
}

// BuildReport aggregates comparison and smoke results into a Report with the correct overall
// Verdict (see aggregateVerdict). now is injected for deterministic unit testing.
func BuildReport(scenario string, comparisons []ComparisonReport, smoke []SmokeReport, now time.Time) Report {
	return Report{
		GeneratedAt: now.UTC().Format(time.RFC3339),
		Scenario:    scenario,
		Verdict:     aggregateVerdict(comparisons, smoke),
		Comparisons: comparisons,
		Smoke:       smoke,
	}
}

// WithInvariantProblems returns c with problems appended to its InvariantErrors and, when there is
// at least one, its Verdict raised to VerdictDiff (never lowered: an infra verdict stays infra).
// Tests that check extra per-target invariants (per-key order, read_committed filtering, the LSO
// tripwire) on top of CompareTranscripts must fold them in with this before building the report —
// otherwise two transcripts that are identically WRONG (both oracles broken the same way) produce a
// report file that says "identical" while the test itself failed, and anything that keys on the
// report file reads a pass.
func WithInvariantProblems(c ComparisonReport, problems []string) ComparisonReport {
	if len(problems) == 0 {
		return c
	}
	c.InvariantErrors = append(append([]string(nil), c.InvariantErrors...), problems...)
	if verdictSeverity(c.Verdict) < verdictSeverity(VerdictDiff) {
		c.Verdict = VerdictDiff
	}
	return c
}

// ToComparisonReport converts a DiffResult into its report.json entry shape.
func ToComparisonReport(d DiffResult) ComparisonReport {
	return ComparisonReport{
		Left:            d.Left,
		Right:           d.Right,
		Verdict:         d.Verdict,
		Diff:            d.Diff,
		InvariantErrors: d.InvariantErrors,
	}
}

// WriteReportJSON marshals the Report as indented JSON and writes it to path, creating parent
// directories as needed.
func WriteReportJSON(path string, report Report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("kafkaconformance: create report dir: %w", err)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("kafkaconformance: marshal report: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("kafkaconformance: write report json: %w", err)
	}
	return nil
}

// RenderReportMarkdown renders a human-readable report.md body for the given Report.
func RenderReportMarkdown(report Report) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# Kafka Conformance Report\n\n")
	fmt.Fprintf(&b, "- Scenario: `%s`\n", report.Scenario)
	fmt.Fprintf(&b, "- Generated: %s\n", report.GeneratedAt)
	fmt.Fprintf(&b, "- Verdict: **%s**\n", report.Verdict)
	if o := report.Oracle; o != nil {
		fmt.Fprintf(&b, "- Oracle: `%s` at %s (version %q, image from %s; ApiVersions max measured %d, expected %d)\n",
			o.Image, o.Addr, o.Version, o.ImageSource, o.ApiVersionsMax, o.ExpectedApiVersionsMax)
		for _, p := range o.Problems {
			fmt.Fprintf(&b, "  - oracle identity problem: %s\n", p)
		}
	}
	fmt.Fprintf(&b, "\n")

	fmt.Fprintf(&b, "## Oracle-parity comparisons\n\n")
	if len(report.Comparisons) == 0 {
		fmt.Fprintf(&b, "_none recorded_\n\n")
	}
	for _, c := range report.Comparisons {
		fmt.Fprintf(&b, "### %s vs %s — %s\n\n", c.Left, c.Right, c.Verdict)
		if len(c.InvariantErrors) > 0 {
			fmt.Fprintf(&b, "**Invariant violations:**\n\n")
			for _, ie := range c.InvariantErrors {
				fmt.Fprintf(&b, "- %s\n", ie)
			}
			fmt.Fprintf(&b, "\n")
		}
		if c.Diff != "" {
			fmt.Fprintf(&b, "```diff\n%s\n```\n\n", c.Diff)
		}
	}

	fmt.Fprintf(&b, "## Smoke drivers\n\n")
	if len(report.Smoke) == 0 {
		fmt.Fprintf(&b, "_none recorded_\n\n")
	}
	for _, s := range report.Smoke {
		fmt.Fprintf(&b, "- `%s` vs `%s`: **%s**", s.Driver, s.Target, s.Verdict)
		if s.ClientVersion != "" {
			fmt.Fprintf(&b, " (client: %s)", s.ClientVersion)
		}
		if s.Detail != "" {
			fmt.Fprintf(&b, " — %s", s.Detail)
		}
		fmt.Fprintf(&b, "\n")
	}

	return b.String()
}

// WriteReportMarkdown renders and writes report.md to path, creating parent directories as
// needed.
func WriteReportMarkdown(path string, report Report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("kafkaconformance: create report dir: %w", err)
	}
	if err := os.WriteFile(path, []byte(RenderReportMarkdown(report)), 0o644); err != nil {
		return fmt.Errorf("kafkaconformance: write report md: %w", err)
	}
	return nil
}
