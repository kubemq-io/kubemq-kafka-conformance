package probe

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	fprobe "github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	register("tls-round-trip", tlsRoundTrip)
	register("tls-advertised-port", tlsAdvertisedPort)
	register("tls-plaintext-client-refused", tlsPlaintextClient)
	register("tls-protocol-versions", tlsProtocolVersions)
}

// tlsRoundTrip: over the TLS port only, create a topic, produce 30 records, consume them back.
func tlsRoundTrip(ctx context.Context, env Env) (Observation, error) {
	var rec *tlsRecorder
	cl, err := kgoTLS(env, "", &rec)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := topicName(env, "tls-rt")
	if err := createTopic(ctx, kadm.NewClient(cl), topic, 1); err != nil {
		return nil, err
	}
	sent, err := produce(ctx, cl, topic, 30)
	if err != nil {
		return nil, err
	}
	got, err := consume(ctx, func(opts ...kgo.Opt) (*kgo.Client, error) { return kgoTLS(env, "", nil, opts...) }, topic, len(sent))
	if err != nil {
		return nil, err
	}
	obs := fprobe.AnalyzeRoundTrip(sent, got).Observation()
	obs["tls_versions"] = rec.seen()
	return obs, nil
}

// tlsAdvertisedPort: a client that bootstraps on the TLS port must be sent back to TLS endpoints
// in Metadata, or its next connection goes to a port that does not speak TLS. On a single node the
// advertised port must be the TLS port dialed; on a cluster every advertised broker must be one of
// the TLS endpoints given (each broker's own TLS address).
func tlsAdvertisedPort(ctx context.Context, env Env) (Observation, error) {
	cl, err := kgoTLS(env, "", nil)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	md, err := kadm.NewClient(cl).BrokerMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("metadata over TLS: %w", err)
	}
	same := AllAdvertisedOnTLS(md.Brokers, Seeds(env.TLS))
	if !same {
		fmt.Fprintf(os.Stderr, "[security] %s Metadata over TLS advertises %v; TLS endpoints given %v\n", env.Target, md.Brokers, Seeds(env.TLS))
	}
	return Observation{"brokers": len(md.Brokers), "every_broker_on_the_tls_port": same}, nil
}

// apiVersionsV0 is a complete ApiVersions v0 request frame, correlation id 7, client id "kcert".
func apiVersionsV0() []byte {
	body := []byte{0, 18, 0, 0, 0, 0, 0, 7, 0, 5, 'k', 'c', 'e', 'r', 't'}
	frame := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(frame, uint32(len(body)))
	return append(frame, body...)
}

// PlaintextResult classifies what a plaintext Kafka request to a TLS port got back: the bytes
// read before the connection ended, and how it ended. Pure, for the mutation tests.
func PlaintextResult(read []byte, endErr error) Observation {
	kafka := len(read) >= 8 && binary.BigEndian.Uint32(read[4:8]) == 7
	return Observation{"kafka_response": kafka, "connection_ended": Outcome(endErr)}
}

// tlsPlaintextClient: a plaintext ApiVersions request sent to the TLS port gets no Kafka answer
// and the connection is closed.
func tlsPlaintextClient(ctx context.Context, env Env) (Observation, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	c, err := d.DialContext(ctx, "tcp", First(env.TLS))
	if err != nil {
		return nil, err
	}
	defer c.Close()
	if _, err := c.Write(apiVersionsV0()); err != nil {
		return PlaintextResult(nil, err), nil
	}
	_ = c.SetReadDeadline(time.Now().Add(15 * time.Second))
	var read []byte
	buf := make([]byte, 4096)
	for {
		n, err := c.Read(buf)
		read = append(read, buf[:n]...)
		if err != nil {
			return PlaintextResult(read, err), nil
		}
	}
}

// tlsProtocolVersions: a handshake restricted to one TLS version, for 1.0 through 1.3. The TCP
// connection is made first and a failure there is an error, never "refused": a refused TLS
// version must be the broker's answer, not a local network fault.
func tlsProtocolVersions(ctx context.Context, env Env) (Observation, error) {
	obs := Observation{}
	host, _, _ := net.SplitHostPort(First(env.TLS))
	for _, v := range []uint16{tls.VersionTLS10, tls.VersionTLS11, tls.VersionTLS12, tls.VersionTLS13} {
		d := net.Dialer{Timeout: 10 * time.Second}
		raw, err := d.DialContext(ctx, "tcp", First(env.TLS))
		if err != nil {
			return nil, fmt.Errorf("connect for %s: %w", tls.VersionName(v), err)
		}
		c := tls.Client(raw, &tls.Config{RootCAs: env.Certs.Roots, ServerName: host, MinVersion: v, MaxVersion: v})
		hctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = c.HandshakeContext(hctx)
		cancel()
		_ = c.Close()
		obs[tls.VersionName(v)] = HandshakeOutcome(err)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[security] %s %s handshake: %v\n", env.Target, tls.VersionName(v), err)
		}
	}
	return obs, nil
}

// HandshakeOutcome names how a TLS handshake ended: "accepted", "refused" (the peer or the
// local TLS stack rejected it), or the Outcome class of any other failure. Pure, for the tests.
func HandshakeOutcome(err error) string {
	switch o := Outcome(err); o {
	case "accepted":
		return o
	case "tls-refused":
		return "refused"
	default:
		return "failed-" + o
	}
}

// AllAdvertisedOnTLS reports whether there is at least one advertised broker and every one is a
// TLS endpoint: on one TLS endpoint, by port (a single node may advertise another host name for
// itself); on several, by exact host:port. Pure, for the tests.
func AllAdvertisedOnTLS(brokers kadm.BrokerDetails, tlsEndpoints []string) bool {
	if len(brokers) == 0 || len(tlsEndpoints) == 0 {
		return false
	}
	want := map[string]bool{}
	ports := map[string]bool{}
	for _, e := range tlsEndpoints {
		want[e] = true
		if _, p, err := net.SplitHostPort(e); err == nil {
			ports[p] = true
		}
	}
	for _, b := range brokers {
		ep := net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port)))
		if len(tlsEndpoints) == 1 {
			if !ports[strconv.Itoa(int(b.Port))] {
				return false
			}
		} else if !want[ep] {
			return false
		}
	}
	return true
}
