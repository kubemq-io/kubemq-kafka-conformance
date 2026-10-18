package conformance

// driver_sarama.go — the SMOKE sarama (github.com/IBM/sarama) driver.
//
// Unlike kcat/kafka-cli/java (driver_kcat.go, driver_kafkacli.go, driver_java.go), sarama is a
// pure in-process Go client: it has no podman/container/subprocess layer at all, so this driver
// dials target.BootstrapAddr() directly, exactly like the FullTranscript driver (driver_franzgo.go).
// It stays Kind() == DriverKindSmoke (a produce+consume subset, success-only — see harness.go's
// DriverKindSmoke doc), NOT FullTranscript: the scope is deliberately the same
// produce+consume projection of ScenarioCore that kcat/kafka-cli/java already exercise, plus admin
// CreateTopic (which sarama, unlike those three, actually has an API for).
//
// Scope: (1) CreateTopic via sarama's ClusterAdmin on a per-run-unique physical topic (1 partition
// / replication-factor 1 — matches ScenarioCore's 1-partition pin and driver_franzgo.go's
// resolveCreateTopicPartitionCount default); (2) Produce every record from the scenario's FIRST
// ActionProduce step (firstProduceStep, shared with driver_kcat.go); (3) Fetch/consume that same
// count back from offset 0 (earliest), count-terminated (never timeout-terminated — the
// outer context timeout is a hang-safety-net only); (4) verify byte-equal key/value/headers per
// position (1-partition topic => positional matching is well-defined, exactly
// kcatConsumedToEvents' rationale) — unlike kcat/kafka-cli, sarama DOES expose headers on its
// consume path, so this is the first Smoke driver that can verify header round-trip fidelity.
//
// Design decision (deviation from kcat/kafka-cli/java precedent, stated explicitly): this driver
// does NOT wrap any error with ErrInfra. ErrInfra's own doc scopes it to "container/network/
// tooling failure ... OUTSIDE the Kafka protocol itself" — sarama has no such layer, and by the
// time Run() is invoked target.BootstrapAddr() has already been proven live by the caller
// (NewKafkaTarget's waitKafkaReady does a real kgo Metadata/ApiVersions round-trip before
// returning). A failure inside Run() is therefore far more likely a genuine sarama-vs-broker
// protocol/compatibility finding than a transient network blip.

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

// saramaConsumeTimeout bounds how long the consume side waits for the count-terminated read to
// complete — a safety net against a hang, not the termination condition itself (mirrors
// kcatConsumeTimeout/kafkaCLITimeout/javaRunTimeout's per-file const pattern).
const saramaConsumeTimeout = 60 * time.Second

// SaramaDriver is the SMOKE sarama driver. It implements the Driver interface (harness.go). It
// takes no config struct: there is nothing to configure (no image, no container engine, no
// BrokerContainer) — it dials target.BootstrapAddr() directly.
type SaramaDriver struct{}

// NewSaramaDriver constructs the sarama Smoke Driver.
func NewSaramaDriver() *SaramaDriver { return &SaramaDriver{} }

// Name identifies this driver in Transcript.Driver / report output.
func (d *SaramaDriver) Name() string { return "sarama" }

// Kind reports this driver as Smoke — see harness.go's DriverKindSmoke doc.
func (d *SaramaDriver) Kind() DriverKind { return DriverKindSmoke }

// saramaConfig builds the sarama.Config this driver uses: defaults (Version defaults to
// DefaultVersion, an old-but-safe wire version per sarama's own doc), plus explicit
// Producer.Return.Successes (required by NewSyncProducer's verifyProducerConfig once any other
// Producer field, like RequiredAcks, is customized), Producer.RequiredAcks = WaitForAll
// (durable-ack round-trip, mirrors driver_franzgo.go's ProduceSync acks-required philosophy), and
// Consumer.Return.Errors = true. The last is load-bearing: sarama defaults it to false, in which
// case a partitionConsumer routes consume-side errors to sarama.Logger and NEVER to the pc.Errors()
// channel (see sendError in sarama's consumer.go) — that would leave saramaConsume's `case <-
// pc.Errors()` arm permanently dead, silently degrading a real broker/fetch error into a 60s ctx
// timeout instead of surfacing it promptly and precisely, which for a conformance driver is exactly
// the divergence we exist to catch. Extracted so it is directly unit-testable without a broker
// (mirrors franzGoClientOpts/resolveCreateTopicPartitionCount's extraction for the same reason).
func saramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Consumer.Return.Errors = true
	return cfg
}

// Run executes a produce+consume SUBSET of the given Scenario against target using an in-process
// sarama client dialing target.BootstrapAddr() directly. See the file doc for the exact subset.
//
// Run has no caller context, so it is RunContext under context.Background().
func (d *SaramaDriver) Run(scenario Scenario, target Target) (Transcript, error) {
	return d.RunContext(context.Background(), scenario, target)
}

// RunContext is Run bounded by ctx as well as by saramaConsumeTimeout. sarama's client, admin and
// producer calls take no context — each is bounded only by sarama's own network, admin and producer
// timeouts — so ctx is checked before each of them, and it bounds the consume directly. A caller
// whose deadline has passed therefore gets its context error at the next phase, never a success.
func (d *SaramaDriver) RunContext(ctx context.Context, scenario Scenario, target Target) (Transcript, error) {
	step, records, err := firstProduceStep(scenario)
	if err != nil {
		return Transcript{}, err
	}
	physicalTopic := smokePhysicalTopic(d.Name(), step.Topic)

	ctx, cancel := context.WithTimeout(ctx, saramaConsumeTimeout)
	defer cancel()
	callerGaveUp := func(phase string) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("kafkaconformance: sarama: %s on target %q not started: %w", phase, target.Name(), ctxErr)
		}
		return nil
	}
	if gaveUpErr := callerGaveUp("connect"); gaveUpErr != nil {
		return Transcript{}, gaveUpErr
	}

	addr := target.BootstrapAddr()
	cfg := saramaConfig()

	// Build ONE *sarama.Client and derive admin/producer/consumer from it via the New*FromClient
	// constructors so they share one connection. NewSyncProducerFromClient/NewConsumerFromClient
	// wrap the passed client in an internal nopCloserClient (verified in sarama's source: both
	// carry the identical "For clients passed in by the client, ensure we don't call Close() on
	// it" comment), so producer.Close()/pc.Close() are safe to defer independently.
	// NewClusterAdminFromClient's Close() does NOT do this — it calls the SHARED client's Close()
	// directly (admin.go) — so the admin handle is deliberately never Close()'d here; cleanup
	// relies on the one final client.Close() below.
	client, err := sarama.NewClient([]string{addr}, cfg)
	if err != nil {
		return Transcript{}, fmt.Errorf("kafkaconformance: sarama: build client for target %q: %w", target.Name(), err)
	}
	defer func() { _ = client.Close() }()

	if gaveUpErr := callerGaveUp("CreateTopic"); gaveUpErr != nil {
		return Transcript{}, gaveUpErr
	}
	if createErr := saramaCreateTopic(client, physicalTopic); createErr != nil {
		return Transcript{}, createErr
	}

	if gaveUpErr := callerGaveUp("produce"); gaveUpErr != nil {
		return Transcript{}, gaveUpErr
	}
	if produceErr := saramaProduce(client, physicalTopic, records); produceErr != nil {
		return Transcript{}, produceErr
	}

	consumed, err := saramaConsume(ctx, client, physicalTopic, step.Partition, len(records))
	if err != nil {
		return Transcript{}, err
	}

	events, err := saramaConsumedToEvents(consumed, records, physicalTopic)
	if err != nil {
		return Transcript{}, err
	}

	return Transcript{
		Driver:        d.Name(),
		DriverKind:    d.Kind(),
		Target:        target.Name(),
		LogicalTopic:  step.Topic,
		Events:        events,
		ClientVersion: "github.com/IBM/sarama " + goModuleVersion("github.com/IBM/sarama"),
	}, nil
}

// saramaCreateTopic issues a ClusterAdmin CreateTopic for a 1-partition, replication-factor-1
// topic — the same shape driver_franzgo.go's runCreateTopic already round-trips successfully
// against KubeMQ. The admin handle is deliberately not Close()'d (see Run's doc: doing so would
// tear down the shared *sarama.Client the producer/consumer still need).
func saramaCreateTopic(client sarama.Client, physicalTopic string) error {
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return fmt.Errorf("kafkaconformance: sarama: build cluster admin: %w", err)
	}
	err = admin.CreateTopic(physicalTopic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		return fmt.Errorf("kafkaconformance: sarama: CreateTopic %q: %w", physicalTopic, err)
	}
	return nil
}

// saramaProduce sends every record via SyncProducer.SendMessage (not the batch SendMessages, so
// per-record errors are individually attributable — matches driver_franzgo.go's runProduce
// per-record loop). Only records with a non-nil Key get ProducerMessage.Key set, mirroring
// harness.go's "nil Key means unkeyed" contract.
func saramaProduce(client sarama.Client, physicalTopic string, records []Record) error {
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return fmt.Errorf("kafkaconformance: sarama: build sync producer: %w", err)
	}
	defer func() { _ = producer.Close() }()

	for i, rec := range records {
		msg := &sarama.ProducerMessage{
			Topic:   physicalTopic,
			Value:   sarama.ByteEncoder(rec.Value),
			Headers: toSaramaHeaders(rec.Headers),
		}
		if rec.Key != nil {
			msg.Key = sarama.ByteEncoder(rec.Key)
		}
		if _, _, err := producer.SendMessage(msg); err != nil {
			return fmt.Errorf("kafkaconformance: sarama: produce record %d to topic %q: %w", i, physicalTopic, err)
		}
	}
	return nil
}

// saramaConsumedRecord is one message this driver observed on the consume side, target-agnostic
// (mirrors KcatMessage's role for the kcat driver).
type saramaConsumedRecord struct {
	Key     []byte
	Value   []byte
	Headers []RecordHeader
	Offset  int64
}

// saramaConsume opens a PartitionConsumer at sarama.OffsetOldest (earliest — the topic is fresh
// so offset 0 is guaranteed, per the same baseOffset==0 invariant CheckFreshTopicBaseOffsetZero
// asserts elsewhere) and reads count messages off pc.Messages()/pc.Errors() in a select loop
// bounded by ctx (mirrors driver_franzgo.go's runFetchFrom polling-loop shape, simplified to
// count-termination).
func saramaConsume(ctx context.Context, client sarama.Client, physicalTopic string, partition int32, count int) ([]saramaConsumedRecord, error) {
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("kafkaconformance: sarama: build consumer: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	pc, err := consumer.ConsumePartition(physicalTopic, partition, sarama.OffsetOldest)
	if err != nil {
		return nil, fmt.Errorf("kafkaconformance: sarama: ConsumePartition %q/%d: %w", physicalTopic, partition, err)
	}
	defer func() { _ = pc.Close() }()

	out := make([]saramaConsumedRecord, 0, count)
	for len(out) < count {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("kafkaconformance: sarama: consume topic %q partition %d: %w (got %d/%d records)",
				physicalTopic, partition, ctx.Err(), len(out), count)
		case msg := <-pc.Messages():
			out = append(out, saramaConsumedRecord{
				Key:     msg.Key,
				Value:   msg.Value,
				Headers: fromSaramaHeaders(msg.Headers),
				Offset:  msg.Offset,
			})
		case cerr, ok := <-pc.Errors():
			if !ok || cerr == nil {
				return nil, fmt.Errorf("kafkaconformance: sarama: consume topic %q partition %d: error channel closed (got %d/%d records)",
					physicalTopic, partition, len(out), count)
			}
			// Wrap the ConsumerError value, not the pointer: sarama defines Error and Unwrap on the
			// value receiver, so wrapping the value lets errors.As find a sarama.ConsumerError and
			// errors.Is reach the underlying broker error through its Unwrap.
			return nil, fmt.Errorf("kafkaconformance: sarama: consume topic %q partition %d: %w (got %d/%d records)",
				physicalTopic, partition, *cerr, len(out), count)
		}
	}
	return out, nil
}

// toSaramaHeaders converts our target-agnostic RecordHeader slice into sarama's RecordHeader
// slice. sarama.RecordHeader's Key/Value are both []byte (unlike our RecordHeader.Key string),
// so the key requires a []byte(...) conversion.
func toSaramaHeaders(headers []RecordHeader) []sarama.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	out := make([]sarama.RecordHeader, len(headers))
	for i, h := range headers {
		out[i] = sarama.RecordHeader{Key: []byte(h.Key), Value: h.Value}
	}
	return out
}

// fromSaramaHeaders converts sarama's RecordHeader slice ([]*sarama.RecordHeader on a consumed
// ConsumerMessage) back into our target-agnostic RecordHeader slice, so a consumed record's
// headers can be hashed with the same HashHeaders used elsewhere, proving header round-trip
// fidelity through the target.
func fromSaramaHeaders(headers []*sarama.RecordHeader) []RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	out := make([]RecordHeader, len(headers))
	for i, h := range headers {
		out[i] = RecordHeader{Key: string(h.Key), Value: h.Value}
	}
	return out
}

// saramaConsumedToEvents converts sarama's consumed messages into normalizable Events, matching
// each consumed message back to the produced Record it round-tripped by POSITION — a 1-partition
// topic makes positional matching well-defined (see kcatConsumedToEvents' identical rationale).
//
// Returns a plain (non-ErrInfra) error on a count mismatch, a position-i key/value mismatch, or
// a position-i HEADER mismatch — a Smoke driver checks success, and
// a mismatch IS a genuine round-trip divergence, not infra (see the file doc's ErrInfra
// design-decision note). The header check is a real per-position equality (via HashHeaders, the
// same canonical wire-order digest used for Event.HeadersSHA), not merely computed-and-ignored —
// without it, a header-dropping/corrupting target would still report VerdictIdentical, which
// contradicts this driver's own documented purpose ("the first Smoke driver that can verify
// header round-trip fidelity").
func saramaConsumedToEvents(consumed []saramaConsumedRecord, produced []Record, physicalTopic string) ([]Event, error) {
	if len(consumed) != len(produced) {
		return nil, fmt.Errorf("kafkaconformance: sarama round-trip count mismatch: produced %d, consumed %d", len(produced), len(consumed))
	}

	events := make([]Event, 0, len(consumed))
	for i, msg := range consumed {
		want := produced[i]
		if !bytes.Equal(msg.Key, want.Key) {
			return nil, fmt.Errorf("kafkaconformance: sarama round-trip key mismatch at position %d: produced %q, consumed %q", i, want.Key, msg.Key)
		}
		if !bytes.Equal(msg.Value, want.Value) {
			return nil, fmt.Errorf("kafkaconformance: sarama round-trip value mismatch at position %d: produced %q, consumed %q", i, want.Value, msg.Value)
		}
		if wantSHA, gotSHA := HashHeaders(want.Headers), HashHeaders(msg.Headers); wantSHA != gotSHA {
			return nil, fmt.Errorf("kafkaconformance: sarama round-trip header mismatch at position %d: produced %v (sha %s), consumed %v (sha %s)", i, want.Headers, wantSHA, msg.Headers, gotSHA)
		}
		if err := checkSmokeOffset("sarama", i, msg.Offset); err != nil {
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
