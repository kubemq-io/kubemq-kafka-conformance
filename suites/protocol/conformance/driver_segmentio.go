package conformance

// driver_segmentio.go — the SMOKE segmentio (github.com/segmentio/kafka-go) driver.
//
// Like sarama (driver_sarama.go), segmentio's kafka-go is a pure in-process Go client with no
// podman/container/subprocess layer, so this driver dials target.BootstrapAddr() directly. It
// stays Kind() == DriverKindSmoke as its baseline — a produce+consume subset, success-only.
//
// Simpler than sarama: kafka.Client/Writer/Reader each own independent connection pools (verified:
// kafka.Client has no Close() method at all — it's a stateless RoundTripper wrapper), so there is
// no shared-client-ownership trap to navigate.
//
// Scope: (1) CreateTopic via kafka.Client.CreateTopics on a per-run-unique physical topic (1
// partition / replication-factor 1); (2) Produce every record from the scenario's FIRST
// ActionProduce step (firstProduceStep, shared with driver_kcat.go) via one Writer.WriteMessages
// call; (3) Fetch/consume that same count back from the beginning via a Reader with an explicit
// SetOffset(FirstOffset) call (NOT ReaderConfig.StartOffset, which kafka-go documents as "only
// used when GroupID is set" — this driver has no GroupID), count-terminated (never
// timeout-terminated — the outer context timeout is a hang-safety-net only); (4) verify byte-equal
// key/value/headers per position. skafka.Header is a direct structural match to our RecordHeader
// (both {Key string, Value []byte}), so header round-trip fidelity is verified natively.
//
// CONSUMER-GROUP ARM (runGroupArm, run after the partition round-trip above). Before round 1 it
// waits (bounded, segmentioCoordinatorReadyTimeout) until FindCoordinator names a coordinator for
// round 1's group: a fresh real Kafka broker answers COORDINATOR_NOT_AVAILABLE until it has created
// __consumer_offsets, and a broker that can never create it (a single broker left at the default
// offsets.topic.replication.factor=3) answers it forever. The wait absorbs the first and fails the
// arm on the second with an error that says so, before any round topic is created; a wait that
// saw refusals is recorded as a Transcript note, so it is never silent. Then ten rounds, each on
// its own topic and group —
//  1. CreateTopics (1 partition) through one kafka.Client/Transport;
//  2. an IMMEDIATE Writer.WriteMessages through a SECOND, brand-new Transport with MaxAttempts 1
//     and no auto-create. A fresh Transport's first request is an all-topics Metadata, and with
//     one attempt there is no retry: if the broker still served a pre-create all-topics
//     enumeration the write fails UNKNOWN_TOPIC_OR_PARTITION on the spot (KubeMQ drops that
//     cache on CreateTopics; this is the real-client proof).
//     EXPECTED REAL-KAFKA BEHAVIOUR, and why the two targets are held to different standards here:
//     Apache Kafka (KRaft) answers CreateTopics once the controller has COMMITTED the topic record;
//     each broker's metadata cache — what Metadata and Produce read — is updated asynchronously
//     when that broker replays the record, so Kafka does not promise read-your-create and an
//     immediate Metadata/Produce may briefly see the topic as unknown (UNKNOWN_TOPIC_OR_PARTITION,
//     or LEADER_NOT_AVAILABLE while the partition leader is being set). On the real-Kafka oracle
//     ONLY, that exact answer is therefore allowed: the arm waits (bounded) until a topic-scoped
//     Metadata shows the topic with a leader, writes once more (again single-attempt), and records
//     the allowance as a Transcript note that lands in the smoke report and the test log. Any
//     other error, a topic that never appears, or a second failure still fails the round. KubeMQ
//     (a single node that invalidates the cache synchronously in CreateTopics) gets NO allowance —
//     that invalidation is the regression this step exists to catch, so the arm deliberately holds
//     KubeMQ to a stricter standard than Kafka itself promises;
//  3. a Reader{GroupID} (JoinGroup/SyncGroup/Heartbeat, StartOffset=FirstOffset for the empty
//     group) fetches every record and synchronously commits only the first k (k varies 1..5 by
//     round), then closes (LeaveGroup);
//  4. a NEW Reader in the same group must resume at EXACTLY offset k — segmentio resolves the
//     group's position with OffsetFetch v1, which it hard-codes, so this is the only real-client
//     coverage of KubeMQ's OffsetFetch v1 path — and deliver records k.. byte-equal
//     (key/value/headers);
//  5. DeleteTopics.
//
// Any deviation is a plain (diff-class) error naming the round, the step and the offsets.
//
// Design decision (mirrors driver_sarama.go's identical note): this driver does NOT wrap any error
// with ErrInfra — see driver_sarama.go's file doc for the full rationale (no container/tooling
// layer here; target.BootstrapAddr() has already been proven live by the caller before Run() is
// invoked).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	skafka "github.com/segmentio/kafka-go"
)

// segmentioConsumeTimeout bounds how long the consume side waits for the count-terminated read to
// complete — a safety net against a hang, not the termination condition itself (mirrors
// saramaConsumeTimeout / kcatConsumeTimeout's per-file const pattern).
const segmentioConsumeTimeout = 60 * time.Second

// SegmentioDriver is the SMOKE segmentio (kafka-go) driver. It implements the Driver interface
// (harness.go). It takes no config struct: it dials target.BootstrapAddr() directly.
type SegmentioDriver struct {
	// coordinatorWait overrides segmentioCoordinatorReadyTimeout when positive (unit tests shrink it).
	coordinatorWait time.Duration
}

// NewSegmentioDriver constructs the segmentio Smoke Driver.
func NewSegmentioDriver() *SegmentioDriver { return &SegmentioDriver{} }

// Name identifies this driver in Transcript.Driver / report output.
func (d *SegmentioDriver) Name() string { return "segmentio" }

// Kind reports this driver as Smoke — see harness.go's DriverKindSmoke doc.
func (d *SegmentioDriver) Kind() DriverKind { return DriverKindSmoke }

// Run executes a produce+consume SUBSET of the given Scenario against target using an in-process
// kafka-go client dialing target.BootstrapAddr() directly. See the file doc for the exact subset.
//
// Run has no caller context, so it is RunContext under context.Background(): every phase is still
// bounded by its own budget.
func (d *SegmentioDriver) Run(scenario Scenario, target Target) (Transcript, error) {
	return d.RunContext(context.Background(), scenario, target)
}

// RunContext is Run bounded by ctx as well as by the driver's own budgets: the partition round
// trip (segmentioConsumeTimeout), the wait for a group coordinator
// (segmentioCoordinatorReadyTimeout) and each group round (segmentioGroupRoundTimeout) all run
// under contexts derived from ctx, so a caller's deadline ends the run in whichever phase it is.
func (d *SegmentioDriver) RunContext(ctx context.Context, scenario Scenario, target Target) (Transcript, error) {
	step, records, err := firstProduceStep(scenario)
	if err != nil {
		return Transcript{}, err
	}
	physicalTopic := smokePhysicalTopic(d.Name(), step.Topic)

	rtCtx, cancel := context.WithTimeout(ctx, segmentioConsumeTimeout)
	defer cancel()

	addr := target.BootstrapAddr()

	if createErr := segmentioCreateTopic(rtCtx, addr, physicalTopic); createErr != nil {
		return Transcript{}, createErr
	}

	if produceErr := segmentioProduce(rtCtx, addr, physicalTopic, records); produceErr != nil {
		return Transcript{}, produceErr
	}

	consumed, err := segmentioConsume(rtCtx, addr, physicalTopic, step.Partition, len(records))
	if err != nil {
		return Transcript{}, err
	}

	events, err := segmentioConsumedToEvents(consumed, records, physicalTopic)
	if err != nil {
		return Transcript{}, err
	}

	oracle := target.Name() == segmentioOracleTargetName
	groupEvents, notes, err := d.runGroupArm(ctx, addr, step.Topic, records, oracle)
	if err != nil {
		return Transcript{}, err
	}
	events = append(events, groupEvents...)

	return Transcript{
		Driver:        d.Name(),
		DriverKind:    d.Kind(),
		Target:        target.Name(),
		LogicalTopic:  step.Topic,
		Events:        events,
		ClientVersion: "github.com/segmentio/kafka-go " + goModuleVersion("github.com/segmentio/kafka-go"),
		Notes:         notes,
	}, nil
}

// segmentioCreateTopic issues a kafka.Client.CreateTopics for a 1-partition, replication-factor-1
// topic. The response's Errors map is keyed by topic name; a non-nil entry for physicalTopic is
// surfaced as a plain error.
func segmentioCreateTopic(ctx context.Context, addr, physicalTopic string) error {
	cl := &skafka.Client{Addr: skafka.TCP(addr)}
	resp, err := cl.CreateTopics(ctx, &skafka.CreateTopicsRequest{
		Addr: skafka.TCP(addr),
		Topics: []skafka.TopicConfig{
			{Topic: physicalTopic, NumPartitions: 1, ReplicationFactor: 1},
		},
	})
	if err != nil {
		return fmt.Errorf("kafkaconformance: segmentio: CreateTopics request for %q: %w", physicalTopic, err)
	}
	if topicErr := resp.Errors[physicalTopic]; topicErr != nil {
		return fmt.Errorf("kafkaconformance: segmentio: CreateTopics %q: %w", physicalTopic, topicErr)
	}
	return nil
}

// segmentioProduce writes every record via one Writer.WriteMessages call. Message.Key/Value being
// nil vs an empty non-nil slice is preserved as-is by kafka-go (unlike sarama's Encoder interface,
// []byte nil is passed straight through) — matching harness.go's "nil Key means no key" contract
// natively, so no per-record nil-check is needed here. Message.Partition is deliberately left
// unset — kafka-go's doc states it is read-only and MUST NOT be set when writing.
//
// Note: WriteMessages does NOT populate Offset back into the caller's msgs slice (verified in
// kafka-go's writer.go source: msgs is passed by value, and WriteMessages only returns a plain
// error/WriteErrors) — so, exactly like every other Smoke driver, RelOffset can only come from the
// consume side (positional match), never the produce side.
func segmentioProduce(ctx context.Context, addr, physicalTopic string, records []Record) error {
	w := &skafka.Writer{
		Addr:         skafka.TCP(addr),
		Topic:        physicalTopic,
		RequiredAcks: skafka.RequireAll,
		Balancer:     &skafka.LeastBytes{},
	}
	defer func() { _ = w.Close() }()

	msgs := make([]skafka.Message, len(records))
	for i, rec := range records {
		msgs[i] = skafka.Message{
			Key:     rec.Key,
			Value:   rec.Value,
			Headers: toSegmentioHeaders(rec.Headers),
		}
	}

	if err := w.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("kafkaconformance: segmentio: WriteMessages to topic %q: %w", physicalTopic, err)
	}
	return nil
}

// segmentioConsumedRecord is one message this driver observed on the consume side, target-agnostic
// (mirrors saramaConsumedRecord's role for the sarama driver).
type segmentioConsumedRecord struct {
	Key     []byte
	Value   []byte
	Headers []RecordHeader
	Offset  int64
}

// segmentioConsume opens a partition Reader (no GroupID => direct partition consumption, no
// commit semantics) and explicitly SetOffset(FirstOffset) — the documented partition-reader
// read-position mechanism (ReaderConfig.StartOffset is ignored for a non-group reader per
// kafka-go's own doc) — before reading count messages via ReadMessage in a loop bounded by ctx.
//
// MaxWait is short (as in segmentioGroupFetch) because Reader.Close waits for the fetch in flight:
// at kafka-go's 10 s default, closing the reader after the last record blocked for about 9 s — past
// any caller deadline shorter than that — while the broker held an empty long poll.
func segmentioConsume(ctx context.Context, addr, physicalTopic string, partition int32, count int) ([]segmentioConsumedRecord, error) {
	r := skafka.NewReader(skafka.ReaderConfig{
		Brokers:   []string{addr},
		Topic:     physicalTopic,
		Partition: int(partition),
		MaxWait:   500 * time.Millisecond,
	})
	defer func() { _ = r.Close() }()

	if err := r.SetOffset(skafka.FirstOffset); err != nil {
		return nil, fmt.Errorf("kafkaconformance: segmentio: SetOffset(FirstOffset) on topic %q partition %d: %w", physicalTopic, partition, err)
	}

	out := make([]segmentioConsumedRecord, 0, count)
	for len(out) < count {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("kafkaconformance: segmentio: consume topic %q partition %d: %w (got %d/%d records)",
				physicalTopic, partition, err, len(out), count)
		}
		out = append(out, segmentioConsumedRecord{
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: fromSegmentioHeaders(msg.Headers),
			Offset:  msg.Offset,
		})
	}
	return out, nil
}

// toSegmentioHeaders converts our target-agnostic RecordHeader slice into kafka-go's Header slice.
// skafka.Header is a direct structural match to RecordHeader ({Key string, Value []byte}), so this
// is a trivial 1:1 field copy.
func toSegmentioHeaders(headers []RecordHeader) []skafka.Header {
	if len(headers) == 0 {
		return nil
	}
	out := make([]skafka.Header, len(headers))
	for i, h := range headers {
		out[i] = skafka.Header{Key: h.Key, Value: h.Value}
	}
	return out
}

// fromSegmentioHeaders converts kafka-go's Header slice back into our target-agnostic
// RecordHeader slice, so a consumed record's headers can be hashed with the same HashHeaders used
// elsewhere, proving header round-trip fidelity through the target.
func fromSegmentioHeaders(headers []skafka.Header) []RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	out := make([]RecordHeader, len(headers))
	for i, h := range headers {
		out[i] = RecordHeader{Key: h.Key, Value: h.Value}
	}
	return out
}

// segmentioConsumedToEvents converts kafka-go's consumed messages into normalizable Events,
// matching each consumed message back to the produced Record it round-tripped by POSITION — a
// 1-partition topic makes positional matching well-defined (see kcatConsumedToEvents'/
// saramaConsumedToEvents' identical rationale).
//
// Returns a plain (non-ErrInfra) error on a count mismatch, a position-i key/value mismatch, or
// a position-i HEADER mismatch — see driver_sarama.go's file doc for
// the shared ErrInfra design-decision rationale and the header-check rationale (HashHeaders is
// now actually ASSERTED, not merely computed-and-discarded into HeadersSHA).
func segmentioConsumedToEvents(consumed []segmentioConsumedRecord, produced []Record, physicalTopic string) ([]Event, error) {
	if len(consumed) != len(produced) {
		return nil, fmt.Errorf("kafkaconformance: segmentio round-trip count mismatch: produced %d, consumed %d", len(produced), len(consumed))
	}

	events := make([]Event, 0, len(consumed))
	for i, msg := range consumed {
		want := produced[i]
		if !bytes.Equal(msg.Key, want.Key) {
			return nil, fmt.Errorf("kafkaconformance: segmentio round-trip key mismatch at position %d: produced %q, consumed %q", i, want.Key, msg.Key)
		}
		if !bytes.Equal(msg.Value, want.Value) {
			return nil, fmt.Errorf("kafkaconformance: segmentio round-trip value mismatch at position %d: produced %q, consumed %q", i, want.Value, msg.Value)
		}
		if wantSHA, gotSHA := HashHeaders(want.Headers), HashHeaders(msg.Headers); wantSHA != gotSHA {
			return nil, fmt.Errorf("kafkaconformance: segmentio round-trip header mismatch at position %d: produced %v (sha %s), consumed %v (sha %s)", i, want.Headers, wantSHA, msg.Headers, gotSHA)
		}
		if err := checkSmokeOffset("segmentio", i, msg.Offset); err != nil {
			return nil, err
		}

		events = append(events, Event{
			Action:     string(ActionProduce),
			Topic:      physicalTopic,
			Partition:  0,
			Key:        msg.Key,
			ValueSHA:   HashValue(msg.Value),
			HeadersSHA: HashHeaders(msg.Headers),
			RelOffset:  msg.Offset,
			ErrCode:    0,
		})
	}
	return events, nil
}

// Consumer-group arm parameters (see the file doc's CONSUMER-GROUP ARM section).
const (
	// segmentioGroupRounds is how many independent create/write/commit/resume/delete rounds run.
	segmentioGroupRounds = 10
	// segmentioGroupRecords is how many records each round writes (taken from the front of the
	// scenario's first Produce step, so keys, values and headers are the scenario's own).
	segmentioGroupRecords = 6
	// segmentioGroupRoundTimeout bounds one round (two group joins plus admin calls).
	segmentioGroupRoundTimeout = 90 * time.Second
	// segmentioGroupActionResume is the Event.Action recorded per round: RelOffset is the offset
	// the second reader resumed at (== the committed offset).
	segmentioGroupActionResume = "GroupResume"
	// segmentioOracleTargetName is the Target.Name() of the real Apache Kafka oracle — the only
	// target the immediate-write metadata-lag allowance applies to (see the file doc).
	segmentioOracleTargetName = "real-kafka"
	// segmentioOracleMetadataWait bounds how long the arm waits for real Kafka's metadata to show a
	// just-created topic after the oracle answered the immediate write with a lag error.
	segmentioOracleMetadataWait = 15 * time.Second
	// segmentioCoordinatorReadyTimeout bounds the wait, before round 1, for FindCoordinator to name
	// a group coordinator (see the file doc). A fresh single broker creates __consumer_offsets in a
	// few seconds; a minute without a coordinator means it never will.
	segmentioCoordinatorReadyTimeout = 60 * time.Second
	// segmentioCoordinatorPoll is the pause between FindCoordinator attempts during that wait.
	segmentioCoordinatorPoll = 250 * time.Millisecond
)

// segmentioGroupCommitCount is how many records round i commits before handing over: 1..5, so
// the resume point differs from round to round and can never coincide with "start" or "end".
func segmentioGroupCommitCount(round int) int {
	return round%(segmentioGroupRecords-1) + 1
}

// runGroupArm runs the consumer-group rounds and returns one GroupResume event per round, plus one
// note per round in which the real-Kafka metadata-lag allowance was used (oracle only). The wait for
// a coordinator and every round run under contexts derived from ctx.
func (d *SegmentioDriver) runGroupArm(ctx context.Context, addr, logicalTopic string, records []Record, oracle bool) ([]Event, []string, error) {
	if len(records) < segmentioGroupRecords {
		return nil, nil, fmt.Errorf("kafkaconformance: segmentio group arm needs %d records from the scenario's first Produce step, got %d", segmentioGroupRecords, len(records))
	}
	recs := records[:segmentioGroupRecords]
	topics := make([]string, segmentioGroupRounds)
	for round := range topics {
		topics[round] = smokePhysicalTopic(d.Name(), fmt.Sprintf("%s-grp%02d", logicalTopic, round))
	}

	var notes []string
	wait := segmentioCoordinatorReadyTimeout
	if d.coordinatorWait > 0 {
		wait = d.coordinatorWait
	}
	firstGroup := topics[0] + "-group"
	waited, refusals, err := segmentioAwaitGroupCoordinator(ctx, addr, firstGroup, wait)
	if err != nil {
		return nil, nil, fmt.Errorf("kafkaconformance: segmentio group arm, before round 1: %w", err)
	}
	if refusals > 0 {
		notes = append(notes, fmt.Sprintf("group coordinator became ready %s after the first FindCoordinator (%d coordinator-loading answers before round 1)",
			waited.Round(time.Millisecond), refusals))
	}

	events := make([]Event, 0, segmentioGroupRounds)
	for round := 0; round < segmentioGroupRounds; round++ {
		topic := topics[round]
		group := topic + "-group"
		commit := segmentioGroupCommitCount(round)
		resumeRec, note, err := segmentioGroupRound(ctx, addr, topic, group, recs, commit, oracle)
		if err != nil {
			return nil, nil, fmt.Errorf("kafkaconformance: segmentio group arm round %d/%d (topic %q, group %q, commit %d): %w",
				round+1, segmentioGroupRounds, topic, group, commit, err)
		}
		if note != "" {
			notes = append(notes, fmt.Sprintf("group arm round %d/%d: %s", round+1, segmentioGroupRounds, note))
		}
		events = append(events, Event{
			Action:     segmentioGroupActionResume,
			Topic:      topic,
			Partition:  0,
			Key:        resumeRec.Key,
			ValueSHA:   HashValue(resumeRec.Value),
			HeadersSHA: HashHeaders(resumeRec.Headers),
			RelOffset:  resumeRec.Offset,
		})
	}
	return events, notes, nil
}

// segmentioGroupRound is one round: create, immediate single-attempt write, group consume with a
// partial commit, resume check on a new reader, delete. It returns the first record the second
// reader delivered, and a non-empty note when the real-Kafka metadata-lag allowance was used. The
// round is bounded by segmentioGroupRoundTimeout and by parent, whichever ends first.
func segmentioGroupRound(parent context.Context, addr, topic, group string, records []Record, commit int, oracle bool) (segmentioConsumedRecord, string, error) {
	ctx, cancel := context.WithTimeout(parent, segmentioGroupRoundTimeout)
	defer cancel()

	// 1. Create through its own Transport.
	adminTr := &skafka.Transport{}
	defer adminTr.CloseIdleConnections()
	admin := &skafka.Client{Addr: skafka.TCP(addr), Transport: adminTr}
	resp, err := admin.CreateTopics(ctx, &skafka.CreateTopicsRequest{
		Addr:   skafka.TCP(addr),
		Topics: []skafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	})
	if err != nil {
		return segmentioConsumedRecord{}, "", fmt.Errorf("step create: %w", err)
	}
	if topicErr := resp.Errors[topic]; topicErr != nil {
		return segmentioConsumedRecord{}, "", fmt.Errorf("step create: %w", topicErr)
	}

	// 2. Immediate write: brand-new Transport (empty metadata cache, so its first request is an
	//    all-topics Metadata), one attempt, no auto-create. On the real-Kafka oracle only, a
	//    metadata-lag answer is allowed once (see the file doc).
	var note string
	if werr := segmentioImmediateWrite(ctx, addr, topic, records); werr != nil {
		if !segmentioMetadataLagAllowed(oracle, werr) {
			return segmentioConsumedRecord{}, "", fmt.Errorf("step immediate-write (single attempt, fresh all-topics metadata right after CreateTopics): %w", werr)
		}
		waited, merr := segmentioAwaitTopicMetadata(ctx, admin, addr, topic, segmentioOracleMetadataWait)
		if merr != nil {
			return segmentioConsumedRecord{}, "", fmt.Errorf("step immediate-write: real Kafka answered %v, and the topic did not become visible in its metadata within %s: %w", werr, segmentioOracleMetadataWait, merr)
		}
		if rerr := segmentioImmediateWrite(ctx, addr, topic, records); rerr != nil {
			return segmentioConsumedRecord{}, "", fmt.Errorf("step immediate-write: second single-attempt write on real Kafka, after its metadata showed the topic, failed: %w (first attempt: %v)", rerr, werr)
		}
		note = fmt.Sprintf("real-Kafka metadata-lag allowance used: the immediate write got %v; the topic was visible in real Kafka's metadata %s later and the second single-attempt write succeeded", werr, waited.Round(time.Microsecond))
	}

	// 3. First group member: fetch everything, commit only the first `commit` records.
	first, err := segmentioGroupFetch(ctx, addr, topic, group, len(records), commit)
	if err != nil {
		return segmentioConsumedRecord{}, "", fmt.Errorf("step first-member: %w", err)
	}
	for i, got := range first {
		if got.Offset != int64(i) {
			return segmentioConsumedRecord{}, "", fmt.Errorf("step first-member: record %d arrived at offset %d, want %d (a new group with StartOffset=FirstOffset must start at 0)", i, got.Offset, i)
		}
		if mismatch := segmentioRecordMatches(got, records[i]); mismatch != nil {
			return segmentioConsumedRecord{}, "", fmt.Errorf("step first-member: offset %d: %w", got.Offset, mismatch)
		}
	}

	// 4. Second member of the same group: must resume at exactly the committed offset.
	rest, err := segmentioGroupFetch(ctx, addr, topic, group, len(records)-commit, 0)
	if err != nil {
		return segmentioConsumedRecord{}, "", fmt.Errorf("step resume: %w", err)
	}
	if resumeErr := segmentioCheckResume(rest, records, commit); resumeErr != nil {
		return segmentioConsumedRecord{}, "", fmt.Errorf("step resume: %w", resumeErr)
	}

	// 5. Delete.
	dresp, err := admin.DeleteTopics(ctx, &skafka.DeleteTopicsRequest{Addr: skafka.TCP(addr), Topics: []string{topic}})
	if err != nil {
		return segmentioConsumedRecord{}, "", fmt.Errorf("step delete: %w", err)
	}
	if topicErr := dresp.Errors[topic]; topicErr != nil {
		return segmentioConsumedRecord{}, "", fmt.Errorf("step delete: %w", topicErr)
	}
	return rest[0], note, nil
}

// segmentioImmediateWrite writes records to topic once: a brand-new Transport (so the first request
// is an all-topics Metadata), MaxAttempts 1, no auto-create.
func segmentioImmediateWrite(ctx context.Context, addr, topic string, records []Record) error {
	writeTr := &skafka.Transport{}
	defer writeTr.CloseIdleConnections()
	w := &skafka.Writer{
		Addr:                   skafka.TCP(addr),
		Topic:                  topic,
		RequiredAcks:           skafka.RequireAll,
		Balancer:               &skafka.LeastBytes{},
		MaxAttempts:            1,
		AllowAutoTopicCreation: false,
		Transport:              writeTr,
	}
	msgs := make([]skafka.Message, len(records))
	for i, rec := range records {
		msgs[i] = skafka.Message{Key: rec.Key, Value: rec.Value, Headers: toSegmentioHeaders(rec.Headers)}
	}
	werr := w.WriteMessages(ctx, msgs...)
	_ = w.Close()
	return werr
}

// segmentioMetadataLagAllowed reports whether an immediate-write error is the metadata-propagation
// answer real Kafka (KRaft) may legitimately give right after CreateTopics — and only on the oracle.
// Pure, so the allowance is unit-testable.
func segmentioMetadataLagAllowed(oracle bool, err error) bool {
	if !oracle || err == nil {
		return false
	}
	isLag := func(e error) bool {
		return errors.Is(e, skafka.UnknownTopicOrPartition) || errors.Is(e, skafka.LeaderNotAvailable)
	}
	var werrs skafka.WriteErrors
	if errors.As(err, &werrs) {
		// Every failed message must carry a lag error; a mix with anything else is not lag.
		n := 0
		for _, e := range werrs {
			if e == nil {
				continue
			}
			if !isLag(e) {
				return false
			}
			n++
		}
		return n > 0
	}
	return isLag(err)
}

// segmentioAwaitTopicMetadata polls a topic-scoped Metadata request (no auto-create) until the
// topic is listed without error and its partition has a leader, or wait elapses. It returns how
// long it took.
func segmentioAwaitTopicMetadata(ctx context.Context, admin *skafka.Client, addr, topic string, wait time.Duration) (time.Duration, error) {
	start := time.Now()
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	var last error
	for {
		md, err := admin.Metadata(wctx, &skafka.MetadataRequest{Addr: skafka.TCP(addr), Topics: []string{topic}})
		switch {
		case err != nil:
			last = err
		case len(md.Topics) != 1 || md.Topics[0].Name != topic:
			last = fmt.Errorf("metadata lists %d topic(s), not %q", len(md.Topics), topic)
		case md.Topics[0].Error != nil:
			last = md.Topics[0].Error
		case len(md.Topics[0].Partitions) == 0 || md.Topics[0].Partitions[0].Error != nil || md.Topics[0].Partitions[0].Leader.Host == "":
			last = fmt.Errorf("topic %q has no partition leader yet", topic)
		default:
			return time.Since(start), nil
		}
		select {
		case <-wctx.Done():
			return time.Since(start), fmt.Errorf("last metadata answer: %v", last)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// segmentioAwaitGroupCoordinator asks for group's coordinator (FindCoordinator, consumer key type)
// until the broker names one, for at most wait (and never past parent's deadline). A
// coordinator-loading answer (see segmentioRetriableCoordinatorError) or a transport error is
// retried; any other broker answer fails at once — it is not a coordinator that is still coming up.
// It returns how long the wait took and how many coordinator-loading answers it saw. Running out of
// time is an error naming the last answer and wrapping the context's error: a coordinator that
// never comes up must fail the arm, not be waited out silently.
func segmentioAwaitGroupCoordinator(parent context.Context, addr, group string, wait time.Duration) (time.Duration, int, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(parent, wait)
	defer cancel()
	tr := &skafka.Transport{}
	defer tr.CloseIdleConnections()
	admin := &skafka.Client{Addr: skafka.TCP(addr), Transport: tr}

	refusals := 0
	var last error
	for {
		resp, err := admin.FindCoordinator(ctx, &skafka.FindCoordinatorRequest{
			Addr:    skafka.TCP(addr),
			Key:     group,
			KeyType: skafka.CoordinatorKeyTypeConsumer,
		})
		switch {
		case err != nil:
			if ctx.Err() == nil || last == nil {
				last = err
			}
		case resp.Error != nil:
			if !segmentioRetriableCoordinatorError(resp.Error) {
				return time.Since(start), refusals, fmt.Errorf("FindCoordinator(group %q) answered %w, which is not a coordinator that is still loading", group, resp.Error)
			}
			refusals++
			last = resp.Error
		default:
			return time.Since(start), refusals, nil
		}
		select {
		case <-ctx.Done():
			return time.Since(start), refusals, fmt.Errorf("no group coordinator for %q within %s or the caller's deadline (%d coordinator-loading answers): %w; last answer: %v — "+
				"the broker never created __consumer_offsets (a single Kafka broker needs offsets.topic.replication.factor=1)",
				group, wait, refusals, ctx.Err(), last)
		case <-time.After(segmentioCoordinatorPoll):
		}
	}
}

// segmentioCheckResume asserts the second group member resumed at EXACTLY the committed offset and
// delivered the remaining records in order, byte-equal. Pure, so the resume rule is unit-testable.
func segmentioCheckResume(rest []segmentioConsumedRecord, records []Record, commit int) error {
	if want := len(records) - commit; len(rest) != want {
		return fmt.Errorf("the new group member delivered %d records after the commit point, want %d", len(rest), want)
	}
	if rest[0].Offset != int64(commit) {
		return fmt.Errorf("the new group member resumed at offset %d, want exactly the committed offset %d (the group position comes from segmentio's OffsetFetch v1)", rest[0].Offset, commit)
	}
	for i, got := range rest {
		want := int64(commit + i)
		if got.Offset != want {
			return fmt.Errorf("record %d after the resume arrived at offset %d, want %d", i, got.Offset, want)
		}
		if err := segmentioRecordMatches(got, records[commit+i]); err != nil {
			return fmt.Errorf("offset %d: %w", got.Offset, err)
		}
	}
	return nil
}

// segmentioRetriableCoordinatorError reports whether err is one of the three group-coordinator
// codes the Kafka protocol defines as retriable while a coordinator is (re)loading: 14
// COORDINATOR_LOAD_IN_PROGRESS, 15 COORDINATOR_NOT_AVAILABLE, 16 NOT_COORDINATOR. segmentio's
// Reader surfaces them through FetchMessage after its own join attempts (it keeps healing in the
// background), and a real Kafka broker returns them for the first group on a fresh broker while
// __consumer_offsets loads. Anything else stays fatal.
func segmentioRetriableCoordinatorError(err error) bool {
	return errors.Is(err, skafka.GroupLoadInProgress) ||
		errors.Is(err, skafka.GroupCoordinatorNotAvailable) ||
		errors.Is(err, skafka.NotCoordinatorForGroup)
}

// segmentioGroupFetch joins group as a fresh Reader, fetches n records with FetchMessage (no
// auto-commit), synchronously commits the first `commit` of them (CommitInterval 0 = synchronous
// OffsetCommit), and closes the reader (LeaveGroup). Retriable coordinator errors (see
// segmentioRetriableCoordinatorError) are retried until ctx expires — a broker that never stops
// answering them fails the round on the deadline — and counted in the returned retries.
func segmentioGroupFetch(ctx context.Context, addr, topic, group string, n, commit int) ([]segmentioConsumedRecord, error) {
	r := skafka.NewReader(skafka.ReaderConfig{
		Brokers:        []string{addr},
		GroupID:        group,
		Topic:          topic,
		StartOffset:    skafka.FirstOffset,
		CommitInterval: 0,
		MaxWait:        500 * time.Millisecond,
	})
	defer func() { _ = r.Close() }()

	raw := make([]skafka.Message, 0, n)
	out := make([]segmentioConsumedRecord, 0, n)
	var lastRetriable error
	for len(raw) < n {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if segmentioRetriableCoordinatorError(err) && ctx.Err() == nil {
				lastRetriable = err
				continue
			}
			if lastRetriable != nil {
				err = fmt.Errorf("%w (after retriable coordinator error %v)", err, lastRetriable)
			}
			return nil, fmt.Errorf("group %q FetchMessage (got %d/%d): %w", group, len(raw), n, err)
		}
		raw = append(raw, msg)
		out = append(out, segmentioConsumedRecord{Key: msg.Key, Value: msg.Value, Headers: fromSegmentioHeaders(msg.Headers), Offset: msg.Offset})
	}
	if commit > 0 {
		if err := r.CommitMessages(ctx, raw[:commit]...); err != nil {
			return nil, fmt.Errorf("group %q CommitMessages(first %d, last offset %d): %w", group, commit, raw[commit-1].Offset, err)
		}
	}
	return out, nil
}

// segmentioRecordMatches compares one consumed record with the record produced at that position.
func segmentioRecordMatches(got segmentioConsumedRecord, want Record) error {
	if !bytes.Equal(got.Key, want.Key) {
		return fmt.Errorf("key mismatch: produced %q, consumed %q", want.Key, got.Key)
	}
	if !bytes.Equal(got.Value, want.Value) {
		return fmt.Errorf("value mismatch: produced %q, consumed %q", want.Value, got.Value)
	}
	if HashHeaders(got.Headers) != HashHeaders(want.Headers) {
		return fmt.Errorf("header mismatch: produced %v, consumed %v", want.Headers, got.Headers)
	}
	return nil
}
