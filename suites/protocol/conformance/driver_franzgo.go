package conformance

// driver_franzgo.go — the ONE full-transcript driver.
//
// franz-go (kgo/kadm) is in-process, binary-safe, and is the only driver in this package that
// can perform every Step kind a Scenario needs: admin (CreateTopic via kadm), produce-with-acks
// (capturing the real assigned offset + Kafka error code per record), a count-terminated fetch,
// ListOffsets earliest/latest, and header round-tripping. Because of that completeness, it is
// the driver that produces the canonical Transcript used for the primary diff (oracle
// parity: kgo vs real-Kafka === kgo vs kfake — see normalize.go/diff.go and conformance_test.go).
//
// kcat and the Java kafka-*.sh CLI (driver_kcat.go / driver_kafkacli.go, later waves) are Smoke
// drivers by contrast: neither exposes an admin API or per-record ack/offset/error information
// over their CLI surface, so they cannot produce a comparable Transcript.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// franzGoStepTimeout bounds every single Step of a Run. It is a hang ceiling, never a termination
// condition: a Step that hits it fails the Run with an error naming the Step (a count-terminated
// fetch that runs out of time is a failure, never a short success). Generous for a cold real-Kafka
// broker — the first InitProducerId on a fresh broker creates __transaction_state and the first
// fetch waits for the partition leader — yet short enough that a broken broker fails the leg in
// about a minute instead of hanging until go test's own timeout.
const franzGoStepTimeout = 60 * time.Second

// franzGoProducerIDRetryPause is the pause between InitProducerId attempts while the transaction
// coordinator answers a retriable error (franz-go already retries each attempt internally; this
// only keeps a fast-failing transport error from spinning).
const franzGoProducerIDRetryPause = 250 * time.Millisecond

// FranzGoDriver is the full-transcript kgo/kadm Driver.
type FranzGoDriver struct {
	// stepTimeout overrides franzGoStepTimeout when positive (unit tests shrink it).
	stepTimeout time.Duration
}

// NewFranzGoDriver constructs the franz-go full-transcript Driver.
func NewFranzGoDriver() *FranzGoDriver { return &FranzGoDriver{} }

// NewFranzGoDriverWithStepBound returns a driver whose per-step bound is d (tests only).
func NewFranzGoDriverWithStepBound(d time.Duration) *FranzGoDriver {
	return &FranzGoDriver{stepTimeout: d}
}

// stepBound is the per-Step ceiling this driver applies (see franzGoStepTimeout).
func (d *FranzGoDriver) stepBound() time.Duration {
	if d.stepTimeout > 0 {
		return d.stepTimeout
	}
	return franzGoStepTimeout
}

// Name implements Driver.
func (d *FranzGoDriver) Name() string { return "kgo" }

// Kind implements Driver. franz-go is the only DriverKindFullTranscript driver in this package
// (see harness.go's DriverKind doc).
func (d *FranzGoDriver) Kind() DriverKind { return DriverKindFullTranscript }

// Run implements Driver: executes every Step of scenario against target using a fresh kgo
// client (and a kadm wrapper for admin ops), recording one or more Events per Step in order, and
// returns the resulting Transcript. The physical topic name used on the wire is derived from the
// scenario's logical topic name (see physicalTopicName) so repeated runs against the same
// long-lived Target (e.g. a real-Kafka broker reused across a test session) never collide.
//
// Run has no caller context, so it is RunContext under context.Background(): every Step is still
// bounded by the per-Step ceiling (franzGoStepTimeout).
func (d *FranzGoDriver) Run(scenario Scenario, target Target) (Transcript, error) {
	return d.RunContext(context.Background(), scenario, target)
}

// RunContext is Run bounded by ctx as well as by the per-Step ceiling: each Step runs under
// context.WithTimeout(ctx, stepBound()), and a Step that runs out of time — or whose ctx the
// caller canceled — fails the Run with an error naming the Step, its bound and the last thing the
// broker answered, even when the Step's handler returned events. Callers holding a deadline (the conformance tests' overall timeout) use this
// instead of Run so that deadline actually reaches the wire.
func (d *FranzGoDriver) RunContext(ctx context.Context, scenario Scenario, target Target) (Transcript, error) {
	physicalTopic := physicalTopicName(scenario.Name, target.Name())

	cl, err := kgo.NewClient(franzGoClientOpts(scenario, target.BootstrapAddr())...)
	if err != nil {
		return Transcript{}, fmt.Errorf("kafkaconformance: build kgo client for target %q: %w", target.Name(), err)
	}
	defer cl.Close()

	admin := kadm.NewClient(cl)

	out := Transcript{
		Driver:       d.Name(),
		DriverKind:   d.Kind(),
		Target:       target.Name(),
		LogicalTopic: scenario.Name,
	}

	bound := d.stepBound()
	for i, step := range scenario.Steps {
		stepCtx, cancel := context.WithTimeout(ctx, bound)
		events, err := d.runStep(stepCtx, cl, admin, target.BootstrapAddr(), physicalTopic, step)
		stepErr := stepCtx.Err()
		cancel()
		switch {
		case stepErr != nil:
			// A Step whose context ended never stands, even when its handler returned events: a
			// client call cut short by the context can come back as an ordinary error answer
			// (kgo's EndTransaction reports OPERATION_NOT_ATTEMPTED when the context ends while it
			// reloads the producer id), which the handler would record as if the broker sent it.
			cause := err
			if cause == nil {
				cause = stepErr
			}
			return Transcript{}, fmt.Errorf("kafkaconformance: step %d (%s) against target %q did not complete within its %s bound (or the caller's deadline): %w",
				i, step.Action, target.Name(), bound, cause)
		case err != nil:
			return Transcript{}, fmt.Errorf("kafkaconformance: step %d (%s) against target %q: %w", i, step.Action, target.Name(), err)
		}
		out.Events = append(out.Events, events...)
	}

	return out, nil
}

// franzGoClientOpts builds the option set for the main (producer/admin) kgo client, extracted from
// Run so the transactional-vs-non-transactional decision is directly unit-testable without a broker
// (mirroring resolveCreateTopicPartitionCount's extraction for the same reason). The base opts are
// byte-identical to what Run built before transactional support was added; the ONLY conditional
// addition is kgo.TransactionalID, appended solely when scenario.TransactionalID != "".
//
// A transactional scenario makes the SAME client transactional — InitProducerId +
// AddPartitionsToTxn + EndTxn resolve on it, and every ActionProduce between an ActionBeginTxn and
// its ActionCommitTxn/ActionAbortTxn is a transactional produce. Empty TransactionalID (every
// pre-transactional scenario's zero-value) leaves the client byte-identical to today (no txn opt
// appended), which the driver_franzgo_test.go byte-unchanged regression pins. CreateTopic (kadm),
// Metadata, and the separate short-lived fetch client (runFetchFrom) are unaffected; the scenario
// never issues a non-transactional produce on a txn client.
func franzGoClientOpts(scenario Scenario, bootstrapAddr string) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(bootstrapAddr),
		kgo.AllowAutoTopicCreation(),
		kgo.ClientID("kafka-conformance-kgo"),
		// kgo.ManualPartitioner() (the harness-bug fix): without this, the client falls back to
		// franz-go's default UniformBytesPartitioner (a key-hash partitioner) which IGNORES
		// Record.Partition entirely for keyed records — runProduce's `Partition: step.Partition`
		// field below was DEAD CODE until this fix, invisible only because every scenario before
		// this fix was 1-partition (n=1 always resolves to partition 0 regardless of which
		// partitioner runs). A no-op for every existing 1-partition scenario; the fix that makes an
		// explicit Record.Partition assignment finally do what it claims.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}
	if scenario.TransactionalID != "" {
		opts = append(opts, kgo.TransactionalID(scenario.TransactionalID))
	}
	return opts
}

// runStep dispatches one Step to its Action-specific handler. bootstrapAddr is threaded through
// (rather than derived from cl via reflection-ish option introspection) so runFetchFrom can dial
// its own short-lived, offset-seeded consumer client against the same target.
func (d *FranzGoDriver) runStep(ctx context.Context, cl *kgo.Client, admin *kadm.Client, bootstrapAddr, physicalTopic string, step Step) ([]Event, error) {
	switch step.Action {
	case ActionCreateTopic:
		return d.runCreateTopic(ctx, admin, physicalTopic, step.NumPartitions)
	case ActionProduce:
		return d.runProduce(ctx, cl, physicalTopic, step)
	case ActionFetchFrom:
		return d.runFetchFrom(ctx, bootstrapAddr, physicalTopic, step)
	case ActionListOffsets:
		return d.runListOffsets(ctx, admin, physicalTopic, step)
	case ActionMetadata:
		return d.runMetadata(ctx, admin, physicalTopic)
	case ActionBeginTxn:
		return d.runBeginTxn(ctx, cl)
	case ActionCommitTxn:
		return d.runEndTxn(ctx, cl, physicalTopic, kgo.TryCommit)
	case ActionAbortTxn:
		return d.runEndTxn(ctx, cl, physicalTopic, kgo.TryAbort)
	default:
		return nil, fmt.Errorf("unknown step action %q", step.Action)
	}
}

// resolveCreateTopicPartitionCount is runCreateTopic's pure default-resolution rule, extracted so
// it is directly unit-testable ("Step.NumPartitions zero-value back-compat"): numPartitions<=0
// (covers both the harness.go Step.NumPartitions zero-value AND any explicit non-positive value)
// resolves to 1 — scenario_core.go's original 1-partition pin — so every pre-multipartition
// Scenario literal (whose Step.NumPartitions zero-value was added alongside multi-partition
// support) creates a 1-partition topic exactly as before; a multi-partition scenario
// (scenario_multipartition.go) passes a positive NumPartitions explicitly and gets it back
// verbatim.
func resolveCreateTopicPartitionCount(numPartitions int32) int32 {
	if numPartitions <= 0 {
		return 1
	}
	return numPartitions
}

// runCreateTopic issues a kadm CreateTopic for a 1x replication-factor topic, recording a single
// Event carrying the resulting Kafka error code (0 on success). See
// resolveCreateTopicPartitionCount for the numPartitions default-resolution rule.
func (d *FranzGoDriver) runCreateTopic(ctx context.Context, admin *kadm.Client, physicalTopic string, numPartitions int32) ([]Event, error) {
	n := resolveCreateTopicPartitionCount(numPartitions)
	resp, err := admin.CreateTopic(ctx, n, 1, nil, physicalTopic)
	errCode := int16(0)
	if resp.Err != nil {
		errCode = kafkaErrCode(resp.Err)
	}
	// A transport-level failure (request never got a response) is an infra problem the caller
	// must see — return it rather than encoding a fabricated Event.
	if err != nil {
		return nil, fmt.Errorf("CreateTopic request for %q: %w", physicalTopic, err)
	}
	return []Event{{
		Action:    string(ActionCreateTopic),
		Topic:     physicalTopic,
		Partition: 0,
		ErrCode:   errCode,
	}}, nil
}

// runProduce produces every Record in step.Records with acks required (so the broker's assigned
// offset and any per-record Kafka error are actually observed), recording one Event per record
// in produce order. RelOffset carries the RAW, broker-assigned absolute offset here — normalize.go
// performs the base-relative rewrite later; CheckFreshTopicBaseOffsetZero depends on this Event
// (recorded here as ActionProduce, not ActionFetchFrom) NOT being consulted for the absolute
// invariant, which only inspects ActionFetchFrom events per its doc.
func (d *FranzGoDriver) runProduce(ctx context.Context, cl *kgo.Client, physicalTopic string, step Step) ([]Event, error) {
	events := make([]Event, 0, len(step.Records))
	for _, rec := range step.Records {
		kr := &kgo.Record{
			Topic:     physicalTopic,
			Partition: step.Partition,
			Key:       rec.Key,
			Value:     rec.Value,
			Headers:   toKgoHeaders(rec.Headers),
		}
		results := cl.ProduceSync(ctx, kr)
		if len(results) != 1 {
			return nil, fmt.Errorf("produce for topic %q: expected 1 result, got %d", physicalTopic, len(results))
		}
		res := results[0]

		// A record that failed because the step ran out of time (or its caller canceled) was never
		// answered by the broker: that is a Step failure, not a Kafka error code to diff. Encoding
		// it as UNKNOWN_SERVER_ERROR and carrying on let every later record fail the same way
		// instantly and pushed the hang on to the next Step (see kafkaErrCode's contract).
		if contextEnded(res.Err) {
			return nil, fmt.Errorf("produce record %d/%d (key %q) to topic %q partition %d was never acknowledged: %w",
				len(events)+1, len(step.Records), rec.Key, physicalTopic, step.Partition, res.Err)
		}

		errCode := int16(0)
		relOffset := int64(-1)
		if res.Err != nil {
			errCode = kafkaErrCode(res.Err)
		} else if res.Record != nil {
			relOffset = res.Record.Offset
		}

		events = append(events, Event{
			Action:     string(ActionProduce),
			Topic:      physicalTopic,
			Partition:  step.Partition,
			Key:        rec.Key,
			ValueSHA:   HashValue(rec.Value),
			HeadersSHA: HashHeaders(rec.Headers),
			RelOffset:  relOffset,
			ErrCode:    errCode,
		})
	}
	return events, nil
}

// runFetchFrom performs a count-terminated fetch starting at step.FetchOffset, NEVER
// timeout-terminated: it polls until step.FetchCount records have been observed for
// (physicalTopic, step.Partition) or ctx ends — the plan's "count-terminated reads (never
// timeout-terminated)" non-negotiable. ctx carries the Step's hang ceiling (RunContext), and
// running into it FAILS the Step; a fetch never ends early as a short success. Each fetched record
// becomes one Event carrying its ABSOLUTE broker offset in RelOffset (again, base-relative
// rewriting is normalize.go's job, not the driver's).
//
// Each Event also carries the partition's HighWatermark/LogStartOffset (from kgo's
// FetchPartition, iterated via fs.EachPartition rather than the coarser fs.EachRecord so this
// partition-level context is available per record) and the record's
// LeaderEpoch/Timestamp/TimestampType (from kgo's Record/RecordAttrs). These fields are captured
// but deliberately NOT diffed yet — see harness.go's Event field docs.
func (d *FranzGoDriver) runFetchFrom(ctx context.Context, bootstrapAddr, physicalTopic string, step Step) ([]Event, error) {
	consumeOpts := []kgo.Opt{
		kgo.SeedBrokers(bootstrapAddr),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			physicalTopic: {step.Partition: kgo.NewOffset().At(step.FetchOffset)},
		}),
		kgo.ClientID("kafka-conformance-kgo-fetch"),
	}
	if step.ReadCommitted {
		// franz-go filters aborted records CLIENT-side via the broker's AbortedTransactions
		// and never surfaces a record past the LSO. EachRecord below therefore yields ONLY committed
		// records — so a count-terminated read's FetchCount is the COMMITTED count, and the aborted
		// records never become Events (the never-delivers-an-aborted-record guarantee, asserted AT
		// THE CLIENT). Zero-value false leaves the fetch client byte-identical to today
		// (read_uncommitted, which surfaces every record incl. aborted ones — proving no server-side
		// filter).
		consumeOpts = append(consumeOpts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}
	fetchCl, err := kgo.NewClient(consumeOpts...)
	if err != nil {
		return nil, fmt.Errorf("build fetch client for topic %q: %w", physicalTopic, err)
	}
	defer fetchCl.Close()

	var events []Event
	for len(events) < step.FetchCount {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("fetch from topic %q partition %d offset %d: %w (got %d/%d records)",
				physicalTopic, step.Partition, step.FetchOffset, ctx.Err(), len(events), step.FetchCount)
		default:
		}

		fs := fetchCl.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if fe.Err != nil {
					events = append(events, Event{
						Action:    string(ActionFetchFrom),
						Topic:     physicalTopic,
						Partition: fe.Partition,
						ErrCode:   kafkaErrCode(fe.Err),
					})
				}
			}
			// A fetch error is recorded as a diffable Event (client-rejected-broker-response
			// territory), not treated as fatal — keep polling for the remaining records unless
			// the context is exhausted.
			continue
		}

		fs.EachPartition(func(p kgo.FetchTopicPartition) {
			p.EachRecord(func(r *kgo.Record) {
				if len(events) >= step.FetchCount {
					return
				}
				events = append(events, Event{
					Action:         string(ActionFetchFrom),
					Topic:          physicalTopic,
					Partition:      r.Partition,
					Key:            r.Key,
					ValueSHA:       HashValue(r.Value),
					HeadersSHA:     HashHeaders(fromKgoHeaders(r.Headers)),
					RelOffset:      r.Offset,
					ErrCode:        0,
					HighWatermark:  p.HighWatermark,
					LogStartOffset: p.LogStartOffset,
					LeaderEpoch:    r.LeaderEpoch,
					Timestamp:      r.Timestamp.UnixMilli(),
					TimestampType:  timestampTypeString(r.Attrs.TimestampType()),
				})
			})
		})
	}
	return events, nil
}

// timestampTypeString renders kgo's RecordAttrs.TimestampType() int8 (0 = client-set CreateTime,
// 1 = broker-set LogAppendTime, -1 = no timestamp / pre-0.10.0 message set) as the same string
// vocabulary kcat's -J "tstype" field uses ("create"/"logappend"), so Event.TimestampType is
// comparable across the kgo and (future) kcat-populated paths. -1 and any other unrecognized
// value render as "" (kcat's own "no determinable type" convention — see KcatMessage.TimestampType
// doc in parse.go).
func timestampTypeString(t int8) string {
	switch t {
	case 0:
		return "create"
	case 1:
		return "logappend"
	default:
		return ""
	}
}

// runListOffsets queries the earliest and/or latest boundary via kadm, per step.ListOffsetsSpec.
// Each queried boundary becomes one Event ("earliest"/"latest" recorded via a synthetic Key so
// the two are distinguishable in a diff — see the Key field usage below), carrying the returned
// offset in RelOffset and the Kafka error code (0 on success).
func (d *FranzGoDriver) runListOffsets(ctx context.Context, admin *kadm.Client, physicalTopic string, step Step) ([]Event, error) {
	var events []Event

	query := func(boundary string, list func(context.Context, ...string) (kadm.ListedOffsets, error)) error {
		listed, err := list(ctx, physicalTopic)
		if err != nil {
			return fmt.Errorf("ListOffsets(%s) for topic %q: %w", boundary, physicalTopic, err)
		}
		lo, ok := listed.Lookup(physicalTopic, step.Partition)
		errCode := int16(0)
		offset := int64(-1)
		if !ok {
			errCode = kerr.UnknownTopicOrPartition.Code
		} else if lo.Err != nil {
			errCode = kafkaErrCode(lo.Err)
		} else {
			offset = lo.Offset
		}
		events = append(events, Event{
			Action:    string(ActionListOffsets),
			Topic:     physicalTopic,
			Partition: step.Partition,
			Key:       []byte(boundary),
			RelOffset: offset,
			ErrCode:   errCode,
		})
		return nil
	}

	if step.ListOffsetsSpec.Earliest {
		if err := query("earliest", admin.ListStartOffsets); err != nil {
			return nil, err
		}
	}
	if step.ListOffsetsSpec.Latest {
		// read_uncommitted latest == ListEndOffsets (the HWM); read_committed latest ==
		// ListCommittedOffsets (kadm metadata.go:478 -> listOffsets(ctx, isolation=1, timestamp=-1)
		// -> the LSO). Distinct boundary labels ("latest" vs "latest-committed") keep the two events
		// distinguishable in the transcript AND identical across oracles (each step runs the SAME
		// query on every target) — the ListOffsets(latest,read_committed)==LSO<HWM tripwire. Both
		// method values share kadm's func(context.Context, ...string)
		// (ListedOffsets, error) signature, so the query closure is reused verbatim.
		latestFn := admin.ListEndOffsets
		boundary := "latest"
		if step.ListOffsetsSpec.ReadCommitted {
			latestFn = admin.ListCommittedOffsets
			boundary = "latest-committed"
		}
		if err := query(boundary, latestFn); err != nil {
			return nil, err
		}
	}
	return events, nil
}

// runMetadata fetches topic metadata for physicalTopic via kadm.ListTopics (which surfaces a
// per-topic Err, unlike kgo's own bare ListTopics helper), recording a single Event whose
// ErrCode reflects any per-topic error the broker returned (0 on success).
func (d *FranzGoDriver) runMetadata(ctx context.Context, admin *kadm.Client, physicalTopic string) ([]Event, error) {
	details, err := admin.ListTopicsWithInternal(ctx, physicalTopic)
	if err != nil {
		return nil, fmt.Errorf("metadata for topic %q: %w", physicalTopic, err)
	}

	errCode := int16(0)
	detail, ok := details[physicalTopic]
	switch {
	case !ok:
		errCode = kerr.UnknownTopicOrPartition.Code
	case detail.Err != nil:
		errCode = kafkaErrCode(detail.Err)
	}

	return []Event{{
		Action:    string(ActionMetadata),
		Topic:     physicalTopic,
		Partition: 0,
		ErrCode:   errCode,
	}}, nil
}

// runBeginTxn begins a transaction on the (transactional) producer client — valid only when the
// scenario set Scenario.TransactionalID (which made cl transactional in Run). The FIRST
// BeginTxn drives InitProducerId (fencing) on the wire; every subsequent ActionProduce until the
// matching ActionCommitTxn/ActionAbortTxn is a transactional produce. Records one synthetic Event
// carrying the Kafka error code (0 on success) so txn-control fidelity is diffable across oracles
// exactly like every other step. A non-typed (transport) error would be mapped to
// UnknownServerError's code by kafkaErrCode — the honest "failed but unclassifiable" sentinel —
// rather than smuggled in as 0/NONE.
//
// The producer ID is loaded under ctx FIRST (awaitTxnProducerID). kgo.BeginTransaction takes no
// context: it loads the ID with context.Background(), and when the transaction coordinator answers
// a retriable error (COORDINATOR_NOT_AVAILABLE on a single broker that cannot create
// __transaction_state) it returns nil and leaves the ID flagged for reload — the next
// transactional produce then waits for an ID that never comes. Loading it here, bounded, turns
// that hang into a Step failure that names the coordinator.
func (d *FranzGoDriver) runBeginTxn(ctx context.Context, cl *kgo.Client) ([]Event, error) {
	if err := awaitTxnProducerID(ctx, cl); err != nil {
		return nil, err
	}
	errCode := int16(0)
	if err := cl.BeginTransaction(); err != nil {
		errCode = kafkaErrCode(err)
	}
	return []Event{{Action: string(ActionBeginTxn), Partition: 0, ErrCode: errCode}}, nil
}

// awaitTxnProducerID loads cl's transactional producer ID (InitProducerId via the transaction
// coordinator) under ctx. Each attempt first asks the broker for the transaction coordinator
// directly (FindCoordinator, which franz-go does not retry on an error answer): franz-go's own
// InitProducerId retries COORDINATOR_NOT_AVAILABLE silently for up to 30 s, so without the probe a
// step bound shorter than that would end with no broker answer to report. A retriable answer or a
// transport error is retried until ctx ends. A non-retriable broker answer (from either request —
// an authorization failure, say) returns nil: it is a definitive answer, and BeginTransaction
// records it as the step's error code exactly as it did before this wait existed, so it stays
// diffable across oracles. It returns an error only when ctx ends first, naming the last answer.
func awaitTxnProducerID(ctx context.Context, cl *kgo.Client) error {
	txnID, _ := cl.OptValue(kgo.TransactionalID).(string)
	admin := kadm.NewClient(cl)
	var lastErr error
	for {
		err := probeTxnCoordinator(ctx, admin, txnID)
		if err == nil {
			if _, _, err = cl.ProducerID(ctx); err == nil {
				return nil
			}
		}
		var ke *kerr.Error
		if errors.As(err, &ke) && !ke.Retriable {
			return nil
		}
		if ctx.Err() == nil || lastErr == nil {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("the transaction coordinator for transactional id %q never granted a producer id (InitProducerId): %w; last answer: %v — "+
				"is __transaction_state creatable? a single Kafka broker needs transaction.state.log.replication.factor=1 and transaction.state.log.min.isr=1",
				txnID, ctx.Err(), lastErr)
		case <-time.After(franzGoProducerIDRetryPause):
		}
	}
}

// probeTxnCoordinator asks for txnID's transaction coordinator once and returns the broker's
// answer for that key (nil when a coordinator was named).
func probeTxnCoordinator(ctx context.Context, admin *kadm.Client, txnID string) error {
	resp, ok := admin.FindTxnCoordinators(ctx, txnID)[txnID]
	if !ok {
		return fmt.Errorf("FindCoordinator(transaction %q) returned no answer for the key", txnID)
	}
	if resp.Err != nil {
		return fmt.Errorf("FindCoordinator(transaction %q): %w", txnID, resp.Err)
	}
	return nil
}

// runEndTxn ends the open transaction with a COMMIT (kgo.TryCommit) or an ABORT (kgo.TryAbort),
// driving EndTxn(26) + the in-log COMMIT/ABORT control marker on the wire. The commit
// flag selects BOTH the EndTransaction call AND the recorded Action label (ActionCommitTxn vs
// ActionAbortTxn) so a commit-vs-abort divergence between oracles surfaces as an Action diff.
// Records one synthetic Event carrying the Kafka error code (0 on success). An EndTxn cut short by
// the step's context (its bound, or the caller's deadline) was never answered by the broker: that
// fails the Step, exactly as a context-ended record does in runProduce, instead of being recorded as
// UNKNOWN_SERVER_ERROR.
func (d *FranzGoDriver) runEndTxn(ctx context.Context, cl *kgo.Client, physicalTopic string, commit kgo.TransactionEndTry) ([]Event, error) {
	action := ActionCommitTxn
	if commit == kgo.TryAbort {
		action = ActionAbortTxn
	}
	errCode := int16(0)
	if err := cl.EndTransaction(ctx, commit); err != nil {
		if contextEnded(err) {
			return nil, fmt.Errorf("%s (EndTxn) for the transaction on topic %q was never answered: %w", action, physicalTopic, err)
		}
		errCode = kafkaErrCode(err)
	}
	return []Event{{Action: string(action), Topic: physicalTopic, Partition: 0, ErrCode: errCode}}, nil
}

// contextEnded reports whether err is a request's context ending (a deadline or a cancellation)
// rather than an answer from the broker. Such an error is a Step failure; it must never reach
// kafkaErrCode, which would record it as UNKNOWN_SERVER_ERROR.
func contextEnded(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// kafkaErrCode extracts the Kafka protocol error code from err, if it (or something it wraps) is
// a *kerr.Error. Errors that are not typed Kafka errors (transport failures, context
// cancellation, etc.) are NOT silently mapped to a Kafka code — that classification (infra vs
// diff) belongs to the caller/harness, not this helper; such errors should propagate as Go
// errors from the calling Step handler instead of being smuggled into ErrCode. For the paths
// that DO call this helper, the input is always already known to be a produce/fetch/admin
// response-level failure, so a typed *kerr.Error is expected; if one isn't found, code 0
// (NONE) would be misleading, so UnknownServerError's code is used as an honest "something
// failed but we couldn't classify it" sentinel.
func kafkaErrCode(err error) int16 {
	if err == nil {
		return 0
	}
	var kerrErr *kerr.Error
	if errors.As(err, &kerrErr) {
		return kerrErr.Code
	}
	return kerr.UnknownServerError.Code
}

// toKgoHeaders converts our target-agnostic RecordHeader slice into kgo's RecordHeader slice,
// preserving wire order (Kafka headers are an ordered list, not a map — see normalize.go's
// HashHeaders doc).
func toKgoHeaders(headers []RecordHeader) []kgo.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	out := make([]kgo.RecordHeader, len(headers))
	for i, h := range headers {
		out[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
	}
	return out
}

// fromKgoHeaders converts kgo's RecordHeader slice back into our target-agnostic RecordHeader
// slice so a fetched record's headers can be hashed with the same HashHeaders used on produce,
// proving header fidelity round-tripped through the target.
func fromKgoHeaders(headers []kgo.RecordHeader) []RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	out := make([]RecordHeader, len(headers))
	for i, h := range headers {
		out[i] = RecordHeader{Key: h.Key, Value: h.Value}
	}
	return out
}

// physicalTopicName derives a per-run-unique physical topic name for a scenario/target pair, so
// re-running the harness against a long-lived, externally-managed Target (e.g. a real-Kafka
// broker started once per run.sh invocation, but under KUBEMQ_KAFKA_CONFORMANCE test iterations)
// never collides with a previous run's leftover topic. normalize.go maps this back to
// Transcript.LogicalTopic (== scenario.Name) for cross-run/cross-target comparison.
//
// The suffix is time.Now().UnixNano() (mirroring driver_kcat.go's smokePhysicalTopic, which
// already does this correctly) — NOT a hash of scenarioName+targetName. An earlier version of
// this function hashed only those two fixed strings, which is deterministic: EVERY run against
// the same target produced the identical physical topic name, so a second run against a
// long-lived broker (exactly the scenario this doc comment describes and CreateTopic on the
// franz-go path is the very first Step run) failed with TOPIC_ALREADY_EXISTS — reproduced
// directly against a real broker reused across two invocations.
func physicalTopicName(scenarioName, targetName string) string {
	return fmt.Sprintf("kconf-%s-%s-%d", scenarioName, targetName, time.Now().UnixNano())
}
