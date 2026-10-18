package probe

// Verifier mutation tests for the security track (docs/methodology.md): each feeds a defect into
// a classifier or the observation diff and proves the row would not stay green.

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"syscall"
	"testing"
	"time"

	fprobe "github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestMutation_PlaintextOnTLSPortDetectsAKafkaAnswer(t *testing.T) {
	clean := PlaintextResult([]byte{0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x0a}, io.EOF)
	if clean["kafka_response"] != false || clean["connection_ended"] != "closed" {
		t.Fatalf("control: a TLS alert then close must read as refused and closed: %v", clean)
	}
	answer := make([]byte, 12)
	binary.BigEndian.PutUint32(answer, 8)
	binary.BigEndian.PutUint32(answer[4:], 7) // correlation id of the probe's request
	muts := map[string]Observation{
		"broker answered in plaintext":  PlaintextResult(answer, io.EOF),
		"broker kept the socket open":   PlaintextResult(nil, os.ErrDeadlineExceeded),
		"broker answered then hung":     PlaintextResult(answer, os.ErrDeadlineExceeded),
		"connection reset is not close": PlaintextResult(nil, errors.New("boom")),
	}
	for name, obs := range muts {
		if _, same := fprobe.Diff(clean, obs); same {
			t.Errorf("plaintext-on-TLS verifier stayed green when %s: %v", name, obs)
		}
	}
	if o := PlaintextResult(nil, syscall.ECONNRESET); o["connection_ended"] != "closed" {
		t.Errorf("a reset is a close: %v", o)
	}
}

func TestMutation_HandshakeOutcome(t *testing.T) {
	cases := map[string]struct {
		err  error
		want string
	}{
		"accepted":            {nil, "accepted"},
		"alert from the peer": {tls.AlertError(70), "refused"},
		"local version error": {errors.New("tls: no supported versions satisfy MinVersion and MaxVersion"), "refused"},
		"network timeout":     {os.ErrDeadlineExceeded, "failed-timeout"},
		"peer closed":         {io.EOF, "failed-closed"},
		"dial cancelled":      {context.DeadlineExceeded, "failed-timeout"},
		"other":               {&net.OpError{Op: "dial", Err: errors.New("x")}, "failed-error"},
	}
	for name, c := range cases {
		if got := HandshakeOutcome(c.err); got != c.want {
			t.Errorf("%s: got %q, want %q", name, got, c.want)
		}
	}
}

func TestMutation_DiffDetectsAcceptedOldTLSVersion(t *testing.T) {
	kafka := Observation{"TLS 1.0": "refused", "TLS 1.1": "refused", "TLS 1.2": "accepted", "TLS 1.3": "accepted"}
	kubemq := Observation{"TLS 1.0": "accepted", "TLS 1.1": "refused", "TLS 1.2": "accepted", "TLS 1.3": "accepted"}
	diff, same := fprobe.Diff(kafka, kubemq)
	if same || diff != `TLS 1.0: Kafka "refused", KubeMQ "accepted"` {
		t.Fatalf("got %q, %v", diff, same)
	}
}

func TestApiVersionsFrame(t *testing.T) {
	f := apiVersionsV0()
	if int(binary.BigEndian.Uint32(f)) != len(f)-4 || binary.BigEndian.Uint16(f[4:]) != 18 || binary.BigEndian.Uint32(f[8:]) != 7 {
		t.Fatalf("bad ApiVersions v0 frame %x", f)
	}
}

func TestMutation_PrincipalErrorNames(t *testing.T) {
	if errName(nil) != "NONE" || errName(kerr.TopicAuthorizationFailed) != "TOPIC_AUTHORIZATION_FAILED" || errName(io.EOF) != "client-closed" {
		t.Fatal("error names")
	}
	kafka := Observation{"grant_to_full_distinguished_name_produce_error": "NONE", "grant_to_common_name_only_produce_error": "TOPIC_AUTHORIZATION_FAILED"}
	for name, kubemq := range map[string]Observation{
		"principal is the common name": {"grant_to_full_distinguished_name_produce_error": "TOPIC_AUTHORIZATION_FAILED", "grant_to_common_name_only_produce_error": "NONE"},
		"authorization not enforced":   {"grant_to_full_distinguished_name_produce_error": "NONE", "grant_to_common_name_only_produce_error": "NONE"},
	} {
		if _, same := fprobe.Diff(kafka, kubemq); same {
			t.Errorf("principal verifier stayed green when %s", name)
		}
	}
}

func TestMutation_SaslHandshakeParse(t *testing.T) {
	// error 33, mechanisms [SCRAM-SHA-256, PLAIN]
	b := []byte{0, 33, 0, 0, 0, 2, 0, 13}
	b = append(b, "SCRAM-SHA-256"...)
	b = append(b, 0, 5)
	b = append(b, "PLAIN"...)
	code, mechs, err := ParseSaslHandshake(b)
	if err != nil || code != 33 || len(mechs) != 2 || mechs[0] != "PLAIN" || mechs[1] != "SCRAM-SHA-256" {
		t.Fatalf("got %d %v %v", code, mechs, err)
	}
	if _, _, err := ParseSaslHandshake(b[:len(b)-2]); err == nil {
		t.Error("a truncated mechanism list must not parse")
	}
	kafka := Observation{"plain_error_code": int16(33), "plain_enabled_mechanisms": []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}}
	for name, kubemq := range map[string]Observation{
		"handshake accepted an unknown mechanism": {"plain_error_code": int16(0), "plain_enabled_mechanisms": []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}},
		"a mechanism missing from the list":       {"plain_error_code": int16(33), "plain_enabled_mechanisms": []string{"PLAIN", "SCRAM-SHA-256"}},
	} {
		if _, same := fprobe.Diff(kafka, kubemq); same {
			t.Errorf("handshake verifier stayed green when %s", name)
		}
	}
}

func TestFrameHeader(t *testing.T) {
	f := frame(17, 1, 11, []byte{0, 6})
	if int(binary.BigEndian.Uint32(f)) != len(f)-4 || binary.BigEndian.Uint16(f[4:]) != 17 || binary.BigEndian.Uint16(f[6:]) != 1 || binary.BigEndian.Uint32(f[8:]) != 11 {
		t.Fatalf("bad frame %x", f)
	}
}

func TestChallengeStatus(t *testing.T) {
	if ChallengeStatus([]byte(`{"status":"invalid_token"}`)) != "invalid_token" || ChallengeStatus([]byte("x")) != "not JSON" {
		t.Fatal("challenge status")
	}
	var ch string
	m := rfcOAuth{token: "t", challenge: &ch}
	if done, resp, _ := m.Challenge([]byte(`{"status":"invalid_token"}`)); done || len(resp) != 1 || resp[0] != 1 || ch != "invalid_token" {
		t.Fatalf("an error challenge must be answered with 0x01: %v %v %q", done, resp, ch)
	}
	if done, _, _ := m.Challenge(nil); !done {
		t.Fatal("an empty server message ends a successful login")
	}
}

func TestMutation_SaslAuthenticateV1Parse(t *testing.T) {
	// error 0, null message, 0 auth bytes, lifetime 10000
	b := []byte{0, 0, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x27, 0x10}
	r, err := ParseSaslAuthenticateV1(b)
	if err != nil || r.ErrorCode != 0 || r.SessionLifetime != 10000 || lifetimeClass(r.SessionLifetime) != "limited" {
		t.Fatalf("got %+v %v", r, err)
	}
	// error 58 with a message, no lifetime
	b = append([]byte{0, 58, 0, 3, 'b', 'a', 'd', 0, 0, 0, 0}, make([]byte, 8)...)
	if r, err := ParseSaslAuthenticateV1(b); err != nil || r.ErrorCode != 58 || lifetimeClass(r.SessionLifetime) != "unlimited" {
		t.Fatalf("got %+v %v", r, err)
	}
	if _, err := ParseSaslAuthenticateV1(b[:9]); err == nil {
		t.Error("a truncated response must not parse")
	}
}

func TestMutation_VerifiableProducerCounts(t *testing.T) {
	out := []byte(`{"name":"startup_complete"}
{"name":"producer_send_success","offset":0}
{"name":"producer_send_error","exception":"x"}
not json
{"name":"producer_send_success","offset":1}
`)
	if a, f := VerifiableProducerCounts(out); a != 2 || f != 1 {
		t.Fatalf("got %d acked, %d failed", a, f)
	}
}

func TestMutation_RateHeld(t *testing.T) {
	if !RateHeld(quotaPayload, 20*time.Second) || !RateHeld(quotaPayload, 7*time.Second) {
		t.Error("a client kept near the quota must read as held")
	}
	if RateHeld(quotaPayload, time.Second) || RateHeld(quotaPayload, 0) {
		t.Error("a client running far above the quota must not read as held")
	}
}

func TestMutation_AdvertisedOnTLS(t *testing.T) {
	b := func(eps ...[2]any) kadm.BrokerDetails {
		var out kadm.BrokerDetails
		for i, e := range eps {
			out = append(out, kadm.BrokerDetail{NodeID: int32(i), Host: e[0].(string), Port: int32(e[1].(int))})
		}
		return out
	}
	// single node: port decides (the broker may advertise another name for itself)
	if !AllAdvertisedOnTLS(b([2]any{"localhost", 9094}), []string{"127.0.0.1:9094"}) {
		t.Error("single node on the TLS port")
	}
	if AllAdvertisedOnTLS(b([2]any{"127.0.0.1", 9092}), []string{"127.0.0.1:9094"}) {
		t.Error("single node sent to the plain port must fail")
	}
	cluster := []string{"10.0.0.1:31193", "10.0.0.2:31194", "10.0.0.3:31195"}
	if !AllAdvertisedOnTLS(b([2]any{"10.0.0.1", 31193}, [2]any{"10.0.0.2", 31194}, [2]any{"10.0.0.3", 31195}), cluster) {
		t.Error("cluster on its TLS endpoints")
	}
	if AllAdvertisedOnTLS(b([2]any{"10.0.0.1", 31193}, [2]any{"10.0.0.2", 31092}), cluster) {
		t.Error("one broker advertised on a plain port must fail")
	}
	if AllAdvertisedOnTLS(nil, cluster) || !reflect.DeepEqual(Seeds(" a:1, ,b:2"), []string{"a:1", "b:2"}) || First("") != "" {
		t.Error("empty metadata, Seeds, First")
	}
}
