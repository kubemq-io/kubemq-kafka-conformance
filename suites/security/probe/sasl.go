package probe

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sort"
	"time"

	fprobe "github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func init() {
	register("sasl-plain", saslMechanismProbe("PLAIN"))
	register("sasl-scram-sha-256", saslMechanismProbe("SCRAM-SHA-256"))
	register("sasl-scram-sha-512", saslMechanismProbe("SCRAM-SHA-512"))
	register("sasl-handshake-unknown-mechanism", saslHandshakeUnknown)
	register("sasl-no-credentials", saslNoCredentials)
	register("sasl-describe-user-scram-credentials", saslDescribeScram)
	register("sasl-alter-user-scram-credentials", saslAlterScram)
}

// The SASL users of the posture (suites/security/posture.Users).
const (
	saslUser     = "alice"
	saslPassword = "alice-secret"
	adminUser    = "admin"
	adminPass    = "admin-secret"
)

func mechanism(name, user, pass string) sasl.Mechanism {
	switch name {
	case "PLAIN":
		return plain.Auth{User: user, Pass: pass}.AsMechanism()
	case "SCRAM-SHA-256":
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()
	default:
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()
	}
}

// saslClient is a franz-go client on one of the posture's ports ("plain" is SASL_PLAINTEXT,
// "tls" is SASL_SSL) authenticating with m.
func saslClient(env Env, port string, m sasl.Mechanism, opts ...kgo.Opt) (*kgo.Client, error) {
	opts = append([]kgo.Opt{kgo.SASL(m), kgo.FetchMaxWait(500 * time.Millisecond), kgo.RequestRetries(1)}, opts...)
	if port == "tls" {
		return kgoTLS(env, "", nil, opts...)
	}
	return kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(Seeds(env.Plain)...)}, opts...)...)
}

// authOutcome is how a login with m on port ended: "NONE" or the error's name.
func authOutcome(ctx context.Context, env Env, port string, m sasl.Mechanism) string {
	cl, err := saslClient(env, port, m)
	if err != nil {
		return "client-" + Outcome(err)
	}
	defer cl.Close()
	actx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	_, err = kadm.NewClient(cl).BrokerMetadata(actx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[security] %s %s login: %v\n", env.Target, port, err)
	}
	return errName(err)
}

// saslMechanismProbe: on both SASL ports, alice logs in with the mechanism and round-trips 10
// records; then a wrong password and an unknown user try to log in.
func saslMechanismProbe(mech string) Probe {
	return func(ctx context.Context, env Env) (Observation, error) {
		obs := Observation{}
		for _, port := range saslPorts(env) {
			newClient := func(opts ...kgo.Opt) (*kgo.Client, error) {
				return saslClient(env, port, mechanism(mech, saslUser, saslPassword), opts...)
			}
			obs[port+"_good_credentials"] = roundTripOutcome(ctx, env, newClient, "sasl-"+port)
			obs[port+"_wrong_password"] = authOutcome(ctx, env, port, mechanism(mech, saslUser, "not-the-password"))
			obs[port+"_unknown_user"] = authOutcome(ctx, env, port, mechanism(mech, "nobody", "nobody-secret"))
		}
		return obs, nil
	}
}

// rawConn opens the posture's port ("plain" or "tls") without SASL. A failed connect is an error.
func rawConn(ctx context.Context, env Env, port string) (net.Conn, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	addr := First(env.Plain)
	if port == "tls" {
		addr = First(env.TLS)
	}
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", port, err)
	}
	if port == "tls" {
		tc := tls.Client(c, &tls.Config{RootCAs: env.Certs.Roots, ServerName: hostOf(addr), MinVersion: tls.VersionTLS12})
		if err := tc.HandshakeContext(ctx); err != nil {
			c.Close()
			return nil, fmt.Errorf("tls handshake: %w", err)
		}
		c = tc
	}
	_ = c.SetDeadline(time.Now().Add(15 * time.Second))
	return c, nil
}

// frame builds a request: header v1 (api key, version, correlation id, client id "kcert") + body.
func frame(apiKey, version int16, corr int32, body []byte) []byte {
	hdr := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 'k', 'c', 'e', 'r', 't'}
	binary.BigEndian.PutUint16(hdr[0:], uint16(apiKey))
	binary.BigEndian.PutUint16(hdr[2:], uint16(version))
	binary.BigEndian.PutUint32(hdr[4:], uint32(corr))
	msg := append(hdr, body...)
	out := make([]byte, 4, 4+len(msg))
	binary.BigEndian.PutUint32(out, uint32(len(msg)))
	return append(out, msg...)
}

// readFrame reads one response and returns its body after the correlation id.
func readFrame(c net.Conn, corr int32) ([]byte, error) {
	var size [4]byte
	if _, err := ioReadFull(c, size[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(size[:])
	if n < 4 || n > 1<<20 {
		return nil, fmt.Errorf("response size %d", n)
	}
	b := make([]byte, n)
	if _, err := ioReadFull(c, b); err != nil {
		return nil, err
	}
	if got := int32(binary.BigEndian.Uint32(b)); got != corr {
		return nil, fmt.Errorf("correlation id %d, want %d", got, corr)
	}
	return b[4:], nil
}

// ParseSaslHandshake reads a SaslHandshake v1 response body: error code and enabled mechanisms
// (sorted). Pure, for the tests.
func ParseSaslHandshake(b []byte) (int16, []string, error) {
	if len(b) < 6 {
		return 0, nil, fmt.Errorf("short SaslHandshake response")
	}
	code := int16(binary.BigEndian.Uint16(b))
	n := int(int32(binary.BigEndian.Uint32(b[2:])))
	b = b[6:]
	var mechs []string
	for i := 0; i < n; i++ {
		if len(b) < 2 {
			return 0, nil, fmt.Errorf("short mechanism list")
		}
		l := int(binary.BigEndian.Uint16(b))
		if len(b) < 2+l {
			return 0, nil, fmt.Errorf("short mechanism name")
		}
		mechs = append(mechs, string(b[2:2+l]))
		b = b[2+l:]
	}
	sort.Strings(mechs)
	return code, mechs, nil
}

// saslHandshakeUnknown: SaslHandshake v1 asking for GSSAPI on both ports: the error code and the
// mechanisms the broker says it has enabled.
func saslHandshakeUnknown(ctx context.Context, env Env) (Observation, error) {
	obs := Observation{}
	for _, port := range saslPorts(env) {
		c, err := rawConn(ctx, env, port)
		if err != nil {
			return nil, err
		}
		body := append([]byte{0, 6}, "GSSAPI"...)
		if _, err := c.Write(frame(17, 1, 11, body)); err != nil {
			c.Close()
			return nil, err
		}
		resp, err := readFrame(c, 11)
		c.Close()
		if err != nil {
			obs[port+"_handshake"] = "no answer: " + Outcome(err)
			continue
		}
		code, mechs, err := ParseSaslHandshake(resp)
		if err != nil {
			return nil, err
		}
		obs[port+"_error_code"] = code
		obs[port+"_enabled_mechanisms"] = mechs
	}
	return obs, nil
}

// saslNoCredentials: a client that skips SASL: ApiVersions (allowed before login), then Metadata.
func saslNoCredentials(ctx context.Context, env Env) (Observation, error) {
	obs := Observation{}
	for _, port := range saslPorts(env) {
		c, err := rawConn(ctx, env, port)
		if err != nil {
			return nil, err
		}
		av := "answered"
		if _, err := c.Write(frame(18, 0, 7, nil)); err != nil {
			av = Outcome(err)
		} else if _, err := readFrame(c, 7); err != nil {
			av = Outcome(err)
		}
		md := "answered"
		if _, err := c.Write(frame(3, 0, 8, []byte{0, 0, 0, 0})); err != nil {
			md = Outcome(err)
		} else if _, err := readFrame(c, 8); err != nil {
			md = Outcome(err)
		}
		c.Close()
		obs[port+"_api_versions"] = av
		obs[port+"_metadata_without_login"] = md
	}
	return obs, nil
}

func scramName(m kadm.ScramMechanism) string {
	switch m {
	case kadm.ScramSha256:
		return "SCRAM-SHA-256"
	case kadm.ScramSha512:
		return "SCRAM-SHA-512"
	}
	return "UNKNOWN"
}

// saslDescribeScram: DescribeUserScramCredentials for alice and an unknown user, as admin.
func saslDescribeScram(ctx context.Context, env Env) (Observation, error) {
	cl, err := saslClient(env, "plain", mechanism("PLAIN", adminUser, adminPass))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	res, err := kadm.NewClient(cl).DescribeUserSCRAMs(ctx, saslUser, "nobody")
	if err != nil {
		return Observation{"request_error": errName(err)}, nil
	}
	obs := Observation{}
	for _, u := range []string{saslUser, "nobody"} {
		d, ok := res[u]
		if !ok {
			obs[u] = "absent from the response"
			continue
		}
		var creds []string
		for _, ci := range d.CredInfos {
			creds = append(creds, fmt.Sprintf("%s/%d", scramName(ci.Mechanism), ci.Iterations))
		}
		sort.Strings(creds)
		obs[u+"_error"] = errName(d.Err)
		obs[u+"_credentials"] = creds
	}
	return obs, nil
}

// saslAlterScram: as admin, create SCRAM-SHA-256 credentials for a new user carol, then carol
// logs in with them on the plain port.
func saslAlterScram(ctx context.Context, env Env) (Observation, error) {
	cl, err := saslClient(env, "plain", mechanism("PLAIN", adminUser, adminPass))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	user := "carol-" + env.Target
	res, err := kadm.NewClient(cl).AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{User: user, Mechanism: kadm.ScramSha256, Iterations: 4096, Password: "carol-secret"}})
	obs := Observation{}
	if err != nil {
		obs["alter_error"] = errName(err)
	} else {
		obs["alter_error"] = errName(res[user].Err)
	}
	// Kafka applies the change through its metadata log; give it a moment before logging in.
	time.Sleep(2 * time.Second)
	obs["new_user_login"] = authOutcome(ctx, env, "plain", mechanism("SCRAM-SHA-256", user, "carol-secret"))
	return obs, nil
}

// roundTripOutcome logs in with newClient, creates a topic, produces 10 records and consumes
// them back: "NONE" when all of it worked, otherwise the step and the error's name.
func roundTripOutcome(ctx context.Context, env Env, newClient func(...kgo.Opt) (*kgo.Client, error), label string) string {
	cl, err := newClient()
	if err != nil {
		return "client-" + Outcome(err)
	}
	defer cl.Close()
	topic := topicName(env, label)
	if err := createTopic(ctx, kadm.NewClient(cl), topic, 1); err != nil {
		return "create: " + errName(err)
	}
	sent, err := produce(ctx, cl, topic, 10)
	if err != nil {
		return "produce: " + errName(err)
	}
	got, err := consume(ctx, newClient, topic, len(sent))
	if err != nil {
		return "consume: " + errName(err)
	}
	if a := fprobe.AnalyzeRoundTrip(sent, got); a.Missing+a.Duplicates+a.OutOfOrder > 0 || !a.ContentMatches {
		return fmt.Sprintf("round trip wrong: %+v", a)
	}
	return "NONE"
}

// saslPorts are the SASL ports a deployment has: always the plain one (SASL_PLAINTEXT), and the
// TLS one (SASL_SSL) when it is given.
func saslPorts(env Env) []string {
	if env.TLS == "" {
		return []string{"plain"}
	}
	return []string{"plain", "tls"}
}
