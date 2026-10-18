package probe

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	fprobe "github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	register("mtls-trusted-client", mtlsTrustedClient)
	register("mtls-no-client-certificate", mtlsNoClientCert)
	register("mtls-untrusted-client-certificate", mtlsUntrustedClientCert)
	register("mtls-principal", mtlsPrincipal)
}

// mtlsTrustedClient: the admin certificate (allowed everything on both brokers) creates a topic,
// produces 30 records and consumes them back over the TLS port.
func mtlsTrustedClient(ctx context.Context, env Env) (Observation, error) {
	cl, err := kgoTLS(env, "admin", nil)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := topicName(env, "mtls-rt")
	if err := createTopic(ctx, kadm.NewClient(cl), topic, 1); err != nil {
		return nil, err
	}
	sent, err := produce(ctx, cl, topic, 30)
	if err != nil {
		return nil, err
	}
	got, err := consume(ctx, func(opts ...kgo.Opt) (*kgo.Client, error) { return kgoTLS(env, "admin", nil, opts...) }, topic, len(sent))
	if err != nil {
		return nil, err
	}
	return fprobe.AnalyzeRoundTrip(sent, got).Observation(), nil
}

// apiVersionsOverTLS connects to the TLS port with cfg, sends ApiVersions and reads the answer.
// It returns how the attempt ended: "answered" or the Outcome class of the failure. A failed
// TCP connect is an error.
func apiVersionsOverTLS(ctx context.Context, env Env, cfg *tls.Config) (string, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	raw, err := d.DialContext(ctx, "tcp", First(env.TLS))
	if err != nil {
		return "", fmt.Errorf("connect: %w", err)
	}
	c := tls.Client(raw, cfg)
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(15 * time.Second))
	if err := c.HandshakeContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "[security] %s handshake: %v\n", env.Target, err)
		return Outcome(err), nil
	}
	// With TLS 1.3 the server checks the client certificate after the client's handshake ends,
	// so a refusal arrives on the first read.
	if _, err := c.Write(apiVersionsV0()); err != nil {
		fmt.Fprintf(os.Stderr, "[security] %s write: %v\n", env.Target, err)
		return Outcome(err), nil
	}
	buf := make([]byte, 8)
	if _, err := ioReadFull(c, buf); err != nil {
		fmt.Fprintf(os.Stderr, "[security] %s read: %v\n", env.Target, err)
		return Outcome(err), nil
	}
	if PlaintextResult(buf, nil)["kafka_response"] == true {
		return "answered", nil
	}
	return "garbage", nil
}

// mtlsNoClientCert: a client that trusts the broker but has no certificate.
func mtlsNoClientCert(ctx context.Context, env Env) (Observation, error) {
	cfg, err := env.Certs.ClientConfig("")
	if err != nil {
		return nil, err
	}
	cfg.ServerName = hostOf(First(env.TLS))
	res, err := apiVersionsOverTLS(ctx, env, cfg)
	if err != nil {
		return nil, err
	}
	return Observation{"api_versions": res}, nil
}

// mtlsUntrustedClientCert: a certificate for alice signed by an authority the broker does not
// trust, sent even though the broker's request names only the trusted authority (a well-behaved
// client would withhold it; this one insists, as an attacker would).
func mtlsUntrustedClientCert(ctx context.Context, env Env) (Observation, error) {
	pair, err := tls.LoadX509KeyPair(filepath.Join(env.Certs.Dir, "rogue-alice.crt"), filepath.Join(env.Certs.Dir, "rogue-alice.key"))
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{RootCAs: env.Certs.Roots, ServerName: hostOf(First(env.TLS)), MinVersion: tls.VersionTLS12,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &pair, nil }}
	res, err := apiVersionsOverTLS(ctx, env, cfg)
	if err != nil {
		return nil, err
	}
	return Observation{"api_versions": res}, nil
}

// mtlsPrincipal: the posture grants alice's full distinguished name on prefix kcert-sec-dn- and
// her common name alone on kcert-sec-cn- (Kafka ACLs and the equivalent KubeMQ policy). alice
// produces one record to a topic of each prefix; the error codes show which principal the
// broker derived from her certificate. Idempotence is off so only the topic grant is tested.
func mtlsPrincipal(ctx context.Context, env Env) (Observation, error) {
	admin, err := kgoTLS(env, "admin", nil)
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	alice, err := kgoTLS(env, "alice", nil, kgo.DisableIdempotentWrite(), kgo.RequestRetries(0),
		kgo.ProduceRequestTimeout(10*time.Second), kgo.RecordDeliveryTimeout(20*time.Second))
	if err != nil {
		return nil, err
	}
	defer alice.Close()
	obs := Observation{}
	for field, prefix := range map[string]string{
		"grant_to_full_distinguished_name": "kcert-sec-dn-",
		"grant_to_common_name_only":        "kcert-sec-cn-",
	} {
		topic := fmt.Sprintf("%s%s-%d", prefix, env.Target, time.Now().UnixNano())
		if err := createTopic(ctx, kadm.NewClient(admin), topic, 1); err != nil {
			return nil, err
		}
		perr := alice.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("x")}).FirstErr()
		obs[field+"_produce_error"] = errName(perr)
	}
	return obs, nil
}

func hostOf(addr string) string {
	h, _, _ := net.SplitHostPort(addr)
	return h
}
