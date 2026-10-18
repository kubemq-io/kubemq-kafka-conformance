// Package kafkaconformance is the multi-client / oracle conformance harness for the KubeMQ
// Kafka drop-in connector.
//
// This file defines the core, pure data types shared by every later file in the package:
// the scripted Scenario/Step model, the recorded Event/Transcript model, and the Target/Driver
// seams that later waves implement (real-Kafka, kfake, KubeMQ targets; kgo/kcat/kafka-cli
// drivers).
//
// Building this package without -tags conformance fails with "build constraints exclude all Go
// files" — that is expected; see tests/kafka-conformance/README.md.
package conformance

import (
	"context"
	"errors"
	"runtime/debug"
)

// ErrInfra marks a container/network/tooling failure — a failure OUTSIDE the Kafka protocol
// itself (podman run/exec failed, a required broker container/address precondition was unmet, a
// container image bootstrap like `apk add` failed, a target was unreachable). It is deliberately
// distinct from a genuine protocol/round-trip divergence (a broker-caused, client-observed
// rejection, or a produce/consume count/content MISMATCH), which must classify as a DIFF, not
// infra. Drivers wrap infra-class failures with %w and this sentinel so the harness's classifier
// (see conformance_test.go's runSmokeDriver and diff.go's Verdict doc) can map errors.Is(err,
// ErrInfra) to VerdictInfra (exit 2) while leaving MISMATCH errors plain (=> VerdictDiff, exit 1).
var ErrInfra = errors.New("kafkaconformance: infra failure")

// StepAction identifies the kind of operation a Step performs against a Target.
//
// NOTE: no consumer-group actions exist yet. Group actions are added when the coordinator is
// decomposed, per the rolling-wave planning rule.
type StepAction string

const (
	// ActionCreateTopic creates a topic (admin API) before any produce/fetch against it.
	ActionCreateTopic StepAction = "CreateTopic"
	// ActionProduce appends one or more records to a topic-partition.
	ActionProduce StepAction = "Produce"
	// ActionFetchFrom fetches records starting at a given offset, count-terminated (never
	// timeout-terminated).
	ActionFetchFrom StepAction = "FetchFrom"
	// ActionListOffsets queries offset boundaries (earliest/latest/by-timestamp).
	ActionListOffsets StepAction = "ListOffsets"
	// ActionMetadata queries topic/partition metadata.
	ActionMetadata StepAction = "Metadata"
	// ActionBeginTxn begins a transaction on the scenario's transactional producer
	// (kgo.Client.BeginTransaction). Valid only when Scenario.TransactionalID != "".
	ActionBeginTxn StepAction = "BeginTxn"
	// ActionCommitTxn ends the open transaction with a COMMIT (EndTransaction(ctx, kgo.TryCommit)),
	// appending a COMMIT control marker; committed records become visible under read_committed.
	ActionCommitTxn StepAction = "CommitTxn"
	// ActionAbortTxn ends the open transaction with an ABORT (EndTransaction(ctx, kgo.TryAbort)),
	// appending an ABORT control marker; the records stay in the log (served under read_uncommitted)
	// but a read_committed client filters them via AbortedTransactions.
	ActionAbortTxn StepAction = "AbortTxn"
)

// Record is one logical key/value(+headers) pair a Step may produce.
type Record struct {
	// Key is the record key. Nil means "no key" (unkeyed record).
	Key []byte
	// Value is the record value. Note: at the normalized Event identity level, a nil value and
	// an empty (non-nil) slice are NOT distinguishable — HashValue (normalize.go) maps both to
	// sha256(zero bytes), the same digest, mirroring Kafka's own lack of a wire distinction
	// between a null value (tombstone-style) and "". Callers that must distinguish
	// null/tombstone from empty need a side channel (out of scope for this harness's scenario,
	// which does not produce null values).
	Value []byte
	// Headers are the record's Kafka headers, in wire order. Their digest is part of the
	// normalized Event identity (see normalize.go) so header fidelity is actually verified.
	Headers []RecordHeader
}

// RecordHeader is a single Kafka record header (key/value pair).
type RecordHeader struct {
	Key   string
	Value []byte
}

// Step is one scripted operation in a Scenario, executed in order against a Target.
type Step struct {
	// Action selects the operation this Step performs.
	Action StepAction
	// Topic is the LOGICAL topic name this step targets. Drivers map this to a physical,
	// per-run-unique topic name on the Target; normalize.go maps it back to LogicalTopic.
	Topic string
	// Partition is the partition this step targets. Scenarios currently pin 1-partition topics
	// (see Scenario doc below), so this is always 0, but the field exists for forward
	// compatibility with N-partition scenarios.
	Partition int32
	// NumPartitions is the partition count an ActionCreateTopic step requests. Zero-value (0)
	// means "use the driver's 1-partition default" (driver_franzgo.go's runCreateTopic maps
	// n<=0 to n=1) — so every existing Scenario literal (ScenarioCore's implicit
	// NumPartitions:0) is unaffected by this field's addition. Unused on every Step kind other
	// than ActionCreateTopic.
	NumPartitions int32
	// Records is the payload for ActionProduce steps. Unused otherwise.
	Records []Record
	// FetchOffset is the starting offset for ActionFetchFrom steps (absolute, as returned by
	// the target — e.g. earliest/latest sentinel handling is the driver's responsibility).
	FetchOffset int64
	// FetchCount is the number of records an ActionFetchFrom step must read before it is
	// considered complete. Count-terminated, never timeout-terminated.
	FetchCount int
	// ListOffsetsSpec selects which boundary(ies) an ActionListOffsets step queries.
	ListOffsetsSpec ListOffsetsSpec
	// ReadCommitted makes an ActionFetchFrom step consume with kgo.FetchIsolationLevel(kgo.ReadCommitted())
	// — the client filters aborted records and never delivers past LSO. Zero-value false ==
	// read_uncommitted (byte-unchanged). Unused on non-FetchFrom steps.
	ReadCommitted bool
}

// ListOffsetsSpec selects which offset-boundary queries an ActionListOffsets Step performs.
type ListOffsetsSpec struct {
	Earliest bool
	Latest   bool
	// ReadCommitted makes the Latest boundary query use read_committed isolation
	// (kadm.ListCommittedOffsets => ListOffsets(latest, IsolationLevel=1) => the LSO) instead of
	// read_uncommitted (kadm.ListEndOffsets => the HWM) — the ListOffsets(latest,read_committed)
	// ==LSO tripwire. Earliest is isolation-independent.
	ReadCommitted bool
}

// Scenario is an ordered, named script of Steps run against exactly one Target by exactly one
// Driver invocation. The same Scenario value is run against every oracle Target so their
// resulting Transcripts are comparable after normalization.
type Scenario struct {
	// Name identifies the scenario (e.g. "core" for scenario_core.go's ScenarioCore).
	Name string
	// Steps is the ordered list of operations to perform.
	Steps []Step
	// TransactionalID, when non-empty, makes the FullTranscript driver build its producer with
	// kgo.TransactionalID(TransactionalID); ActionBeginTxn/ActionProduce/ActionCommitTxn|ActionAbortTxn
	// then drive InitProducerId + AddPartitionsToTxn + EndTxn on the wire. Empty (every pre-9.4
	// scenario's zero-value) == today's non-transactional client, byte-unchanged.
	TransactionalID string
}

// Event is one observed, normalizable outcome recorded by a Driver while running a Scenario
// against a Target. Event is the ONLY thing diff.go compares (after normalization) — Driver,
// Target and LogicalTopic live on Transcript, never on Event, per the plan's key contracts.
type Event struct {
	// Action mirrors the StepAction that produced this Event, as a string (e.g. "Produce",
	// "FetchFrom", "ListOffsets"). Kept as a plain string (not StepAction) so a Driver may also
	// record synthetic/derived events (e.g. a per-record fetch event) without action-type
	// friction.
	Action string
	// Topic is the PHYSICAL topic name as observed on the wire/API by the driver. normalize.go
	// maps this back to the Scenario's LogicalTopic name for cross-run comparison.
	Topic string
	// Partition is the partition this event pertains to.
	Partition int32
	// Key is the record key (nil for unkeyed records, distinct from an empty key).
	Key []byte
	// ValueSHA is hex-encoded sha256(value) — never the raw value, so large payloads stay out
	// of transcripts and diffs are stable/cheap.
	ValueSHA string
	// HeadersSHA is hex-encoded sha256(canonical header encoding); see normalize.go for the
	// canonical encoding rule. Empty string means "no headers" (distinct from a hash of zero
	// headers, which normalize.go also defines as the empty string — see there).
	HeadersSHA string
	// RelOffset is the offset of this event, base-relative to the topic-partition's first
	// observed offset in THIS transcript (see normalize.go). Driver code may record the
	// absolute offset here; normalize.go performs the base-relative rewrite.
	RelOffset int64
	// ErrCode is the Kafka error code observed for this event/step (0 = no error). Comparable
	// 1:1 across targets/oracles when the driver exposes it (kgo); smoke drivers that only
	// observe success/failure record 0 on success and a non-zero sentinel on failure.
	ErrCode int16

	// The following five fields are the "infra half": they widen the capture
	// surface so a Driver CAN record what a target reports for high-watermark/log-start/leader
	// epoch/timestamp, once a Driver is able to observe them. They are deliberately CAPTURED but
	// NOT YET COMPARED: scrubEvent (normalize.go) does not copy them into the value Normalize()
	// diffs, so cmp.Diff (diff.go's CompareTranscripts) stays blind to them for now — asserting
	// on them before wire Produce lands (which is what would let a KubeMQ transcript
	// actually populate them) would produce false diffs against zero-valued fields on drivers/
	// targets that don't yet surface them. The produce-dependent half (driving these
	// fields through a full transcript diff) comes later.

	// HighWatermark is the partition high-watermark (the offset one past the last committed
	// record) as reported alongside this event, when the driver/target surfaces it (e.g. kgo's
	// FetchPartition.HighWatermark on a FetchFrom event). Zero-value (0) when not observed.
	HighWatermark int64
	// LogStartOffset is the partition's low-watermark (earliest available offset) as reported
	// alongside this event, when the driver/target surfaces it (e.g. kgo's
	// FetchPartition.LogStartOffset on a FetchFrom event). Zero-value (0) when not observed.
	LogStartOffset int64
	// LeaderEpoch is the leader epoch the target attached to this event's record/response, when
	// the driver surfaces it (e.g. kgo's Record.LeaderEpoch on a fetched record — the
	// monotonic-leader-epoch requirement, which this field exists to eventually verify on the
	// wire). Zero-value (0) when not observed; note 0 is also a legitimate first-epoch value, so
	// this field alone cannot yet distinguish "unset" from "epoch 0" — acceptable for the
	// capture-only infra half (see file doc above).
	LeaderEpoch int32
	// Timestamp is the record timestamp (Unix milliseconds, matching the Kafka wire's
	// millisecond epoch convention and kcat -J's "ts" field) as reported alongside this event,
	// when the driver surfaces it. Zero-value (0) when not observed.
	Timestamp int64
	// TimestampType describes how Timestamp was determined: "create" (client-set CreateTime,
	// Kafka wire attrs bit 0) or "logappend" (broker-set LogAppendTime, attrs bit 1), mirroring
	// kcat -J's "tstype" string values. Empty string when not observed.
	TimestampType string
}

// DriverKind classifies how much of a Scenario a Driver actually verifies: only ONE
// driver (franz-go/kgo) is FullTranscript; kcat and the Java CLI are Smoke drivers that prove
// client connectivity/round-trip on a produce+consume subset, not full-transcript diffed.
type DriverKind string

const (
	// DriverKindFullTranscript marks a driver that runs the ENTIRE Scenario (admin,
	// produce-with-acks, fetch, ListOffsets, headers) and produces a Transcript suitable for
	// the primary oracle-parity diff. Only the kgo driver (driver_franzgo.go) is this kind.
	DriverKindFullTranscript DriverKind = "FullTranscript"
	// DriverKindSmoke marks a driver that runs a produce+consume SUBSET of a Scenario against
	// a single, real-Kafka-coupled Target and is checked for success only — never
	// whole-transcript diffed. kcat and the Java kafka-*.sh CLI are this kind.
	DriverKindSmoke DriverKind = "Smoke"
)

// Transcript is the full, ordered record of what a Driver observed running a Scenario against
// a Target. Driver, Target and LogicalTopic are TRANSCRIPT-level metadata (not per-Event) per
// the plan's key contracts — every event in a Transcript shares the same driver/target/topic
// mapping context.
type Transcript struct {
	// Driver is the Name() of the Driver that produced this transcript (e.g. "kgo", "kcat",
	// "kafka-cli").
	Driver string
	// DriverKind mirrors the Driver's Kind() at the time this transcript was produced.
	DriverKind DriverKind
	// Target is the Name() of the Target this transcript was recorded against (e.g.
	// "real-kafka", "kfake").
	Target string
	// LogicalTopic is the scenario-level logical topic name (e.g. "core") that every physical,
	// per-run-unique topic name in this transcript's Events maps back to. A Transcript may
	// cover more than one logical topic in a future scenario; the current core scenario uses
	// exactly one, so this single field is sufficient.
	LogicalTopic string
	// Events is the ordered list of observed outcomes, in the order the Driver recorded them.
	Events []Event
	// ClientVersion is the client library version the driver PROVED it ran (read from the binary's
	// build info, or from the client tool's own version output), for the report. Empty when a
	// driver does not record it.
	ClientVersion string `json:",omitempty"`
	// Notes are target-specific observations a Smoke driver wants in the report without failing
	// the run (e.g. an allowance applied only on the real-Kafka oracle). They are appended to the
	// smoke report's detail.
	Notes []string `json:",omitempty"`
}

// Target is a Kafka-protocol-speaking endpoint a Driver runs a Scenario against: real Apache
// Kafka, franz-go's in-process kfake, or KubeMQ itself. This interface is the
// seam a later target_kubemq.go extends — a KubeMQ target is not yet implemented here.
type Target interface {
	// BootstrapAddr returns the host:port a client should use as bootstrap.servers.
	BootstrapAddr() string
	// Name identifies the target for Transcript.Target and report output (e.g. "real-kafka",
	// "kfake").
	Name() string
	// Close releases any resources the Target holds (in-process brokers, client handles). It
	// does NOT stop externally-managed infrastructure (e.g. a podman container run.sh started)
	// — see target_kafka.go's "no self-start" rule.
	Close() error
}

// Driver runs a Scenario against a Target and returns the resulting Transcript. Implementations
// range from a full in-process client (kgo) to a subprocess/container wrapper (kcat, the Java
// CLI).
type Driver interface {
	// Run executes the Scenario against the Target and returns the observed Transcript. A
	// Smoke-kind driver may run only a subset of the Scenario appropriate to what it can
	// verify (see DriverKindSmoke).
	Run(scenario Scenario, target Target) (Transcript, error)
	// Name identifies the driver for Transcript.Driver and report output (e.g. "kgo", "kcat",
	// "kafka-cli").
	Name() string
	// Kind reports whether this driver produces a full-transcript diffable result or only a
	// smoke (success-only) result. See DriverKind.
	Kind() DriverKind
}

// ContextDriver is a Driver that can also run bounded by a caller's context. Driver.Run takes no
// context, so a caller's deadline never reached a driver that dials the broker in-process: the
// kgo driver ran every step under context.Background(), and a transactional produce waiting on a
// transaction coordinator that never came up hung for the full go test timeout (30 minutes)
// under a 5-minute test context. Every in-process driver (kgo, sarama, segmentio) implements it;
// callers holding a deadline run drivers through runDriver.
type ContextDriver interface {
	Driver
	RunContext(ctx context.Context, scenario Scenario, target Target) (Transcript, error)
}

// runDriver runs d against target under ctx when d is a ContextDriver, and through plain Run
// otherwise. The drivers that are not ContextDrivers are the subprocess drivers (kcat, the Kafka
// CLI, the Java runners): ctx does not reach them, and they are bounded only by their own fixed
// per-command timeouts (exec.CommandContext under context.WithTimeout(context.Background(), ...)).
func runDriver(ctx context.Context, d Driver, scenario Scenario, target Target) (Transcript, error) {
	if cd, ok := d.(ContextDriver); ok {
		return cd.RunContext(ctx, scenario, target)
	}
	return d.Run(scenario, target)
}

// goModuleVersion reports the version of Go module path linked into this binary, read from its
// build info ("unknown" when the build info does not list it). A replaced module reports
// "<version>=><replacement version>". Drivers use it to record, in the report, exactly which
// client library version they ran.
func goModuleVersion(path string) string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range bi.Deps {
		if dep.Path == path {
			if dep.Replace != nil {
				return dep.Version + "=>" + dep.Replace.Version
			}
			return dep.Version
		}
	}
	return "unknown"
}
