package conformance

// target_kafka.go — the real-Apache-Kafka Target.
//
// This Target does NOT self-start a broker. Container lifecycle (podman run, port mapping,
// readiness) is entirely owned by run.sh, which starts KAFKA_IMAGE (default
// docker.io/apache/kafka:4.3.1; KRaft single-node) and passes its reachable address in via the
// KAFKA_ADDR env var. This mirrors the tests/amqp10-jms/run.sh precedent's infra/test separation:
// the Go test process never shells out to podman itself.
//
// Readiness is also NOT this file's job to perform blindly at construction time — a bare TCP
// connect (or even a bare ApiVersions round-trip) can succeed while a freshly-started KRaft
// broker is still fenced/electing and will hang or error on real requests.
// NewKafkaTarget retries a real kgo Metadata/CreateTopics-shaped
// round-trip (via kgo's own Ping, which issues an ApiVersions+Metadata handshake under the
// hood) until it succeeds or the deadline elapses, so callers get a Target that is actually
// ready to run a Scenario against, not just reachable at the TCP level.

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaAddrEnv is the environment variable run.sh sets to the reachable host:port of the
// real-Kafka broker it started.
// AddrTarget is a broker reached by its bootstrap address: the Kafka oracle or KubeMQ, both
// started by runner/certify.sh. This replaces kubemq-server's KafkaTarget (KAFKA_ADDR) and its
// in-process KubeMQ target; nothing here starts a broker.
type AddrTarget struct {
	name string
	addr string
}

// NewAddrTarget waits until addr answers a Kafka ApiVersions ping, then returns the target.
// A broker that never answers is ErrInfra.
func NewAddrTarget(ctx context.Context, name, addr string) (*AddrTarget, error) {
	if addr == "" {
		return nil, fmt.Errorf("%w: target %q has no bootstrap address", ErrInfra, name)
	}
	if err := waitKafkaReady(ctx, addr); err != nil {
		return nil, fmt.Errorf("%w: %s at %q did not become ready: %v", ErrInfra, name, addr, err)
	}
	return &AddrTarget{name: name, addr: addr}, nil
}

func (k *AddrTarget) BootstrapAddr() string { return k.addr }
func (k *AddrTarget) Name() string          { return k.name }
func (k *AddrTarget) Close() error          { return nil }

func waitKafkaReady(ctx context.Context, addr string) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConnIdleTimeout(5*time.Second),
		kgo.RequestTimeoutOverhead(5*time.Second),
	)
	if err != nil {
		return fmt.Errorf("build readiness client: %w", err)
	}
	defer cl.Close()

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("readiness deadline exceeded, last error: %w", lastErr)
			}
			return ctx.Err()
		default:
		}

		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := cl.Ping(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return fmt.Errorf("readiness deadline exceeded, last error: %w", lastErr)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// BootstrapAddr implements Target.

// Target names. The oracle must be named OracleTargetName: the segmentio driver grants real
// Kafka (and only real Kafka) a bounded metadata-propagation allowance after CreateTopics.
const (
	OracleTargetName = "real-kafka"
	KubeMQTargetName = "kubemq"
)
