package probe

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	register("reauth-session-lifetime", reauthSessionLifetime)
	register("reauth-same-connection", reauthSameConnection)
	register("reauth-expired-session", reauthExpiredSession)
	register("reauth-franz-go-long-session", reauthFranzGoLongSession)
}

// ReauthWindow is the oracle's connections.max.reauth.ms in the reauth posture (posture.Specs).
const ReauthWindow = 10 * time.Second

// SaslAuthResult is a parsed SaslAuthenticate v1 response.
type SaslAuthResult struct {
	ErrorCode       int16
	SessionLifetime int64 // milliseconds; 0 means the broker sets no limit
}

// ParseSaslAuthenticateV1 reads error_code, error_message, auth_bytes and session_lifetime_ms.
// Pure, for the tests.
func ParseSaslAuthenticateV1(b []byte) (SaslAuthResult, error) {
	var r SaslAuthResult
	if len(b) < 4 {
		return r, fmt.Errorf("short SaslAuthenticate response")
	}
	r.ErrorCode = int16(binary.BigEndian.Uint16(b))
	b = b[2:]
	if l := int(int16(binary.BigEndian.Uint16(b))); l > 0 {
		if len(b) < 2+l {
			return r, fmt.Errorf("short error message")
		}
		b = b[2+l:]
	} else {
		b = b[2:]
	}
	if len(b) < 4 {
		return r, fmt.Errorf("short auth bytes")
	}
	if l := int(int32(binary.BigEndian.Uint32(b))); l > 0 {
		if len(b) < 4+l {
			return r, fmt.Errorf("short auth bytes")
		}
		b = b[4+l:]
	} else {
		b = b[4:]
	}
	if len(b) < 8 {
		return r, fmt.Errorf("short session lifetime")
	}
	r.SessionLifetime = int64(binary.BigEndian.Uint64(b))
	return r, nil
}

// saslPlainLogin runs SaslHandshake v1 (PLAIN) and SaslAuthenticate v1 for alice on c. A network
// failure is returned as an error with the step; a refusal is in the result's error code.
func saslPlainLogin(c net.Conn, corr int32) (SaslAuthResult, error) {
	mech := append([]byte{0, 5}, "PLAIN"...)
	if _, err := c.Write(frame(17, 1, corr, mech)); err != nil {
		return SaslAuthResult{}, fmt.Errorf("handshake write: %w", err)
	}
	resp, err := readFrame(c, corr)
	if err != nil {
		return SaslAuthResult{}, fmt.Errorf("handshake: %w", err)
	}
	if code, _, err := ParseSaslHandshake(resp); err != nil || code != 0 {
		return SaslAuthResult{ErrorCode: code}, err
	}
	token := []byte("\x00" + saslUser + "\x00" + saslPassword)
	body := make([]byte, 4, 4+len(token))
	binary.BigEndian.PutUint32(body, uint32(len(token)))
	body = append(body, token...)
	if _, err := c.Write(frame(36, 1, corr+1, body)); err != nil {
		return SaslAuthResult{}, fmt.Errorf("authenticate write: %w", err)
	}
	resp, err = readFrame(c, corr+1)
	if err != nil {
		return SaslAuthResult{}, fmt.Errorf("authenticate: %w", err)
	}
	return ParseSaslAuthenticateV1(resp)
}

// metadataV0 sends Metadata v0 (all topics) and reports "answered" or how the connection ended.
func metadataV0(c net.Conn, corr int32) string {
	if _, err := c.Write(frame(3, 0, corr, []byte{0, 0, 0, 0})); err != nil {
		return Outcome(err)
	}
	if _, err := readFrame(c, corr); err != nil {
		return Outcome(err)
	}
	return "answered"
}

func lifetimeClass(ms int64) string {
	if ms > 0 {
		return "limited"
	}
	return "unlimited"
}

// reauthSessionLifetime: the session lifetime a PLAIN login is given, on both ports.
func reauthSessionLifetime(ctx context.Context, env Env) (Observation, error) {
	obs := Observation{}
	for _, port := range []string{"plain", "tls"} {
		c, err := rawConn(ctx, env, port)
		if err != nil {
			return nil, err
		}
		r, err := saslPlainLogin(c, 21)
		c.Close()
		if err != nil {
			return nil, fmt.Errorf("%s login: %w", port, err)
		}
		obs[port+"_login_error_code"] = r.ErrorCode
		obs[port+"_session_lifetime"] = lifetimeClass(r.SessionLifetime)
	}
	return obs, nil
}

// reauthSameConnection: KIP-368 re-authentication: after a login, the client sends SaslHandshake
// and SaslAuthenticate again on the same connection, then a Metadata request.
func reauthSameConnection(ctx context.Context, env Env) (Observation, error) {
	c, err := rawConn(ctx, env, "plain")
	if err != nil {
		return nil, err
	}
	defer c.Close()
	if r, err := saslPlainLogin(c, 31); err != nil || r.ErrorCode != 0 {
		return nil, fmt.Errorf("first login: %v (error code %d)", err, r.ErrorCode)
	}
	obs := Observation{}
	r, err := saslPlainLogin(c, 33)
	switch {
	case err != nil:
		obs["reauthentication"] = "connection " + Outcome(err)
	case r.ErrorCode != 0:
		obs["reauthentication"] = fmt.Sprintf("error code %d", r.ErrorCode)
	default:
		obs["reauthentication"] = "accepted"
	}
	obs["metadata_after"] = metadataV0(c, 35)
	return obs, nil
}

// reauthExpiredSession: a client that never re-authenticates sends Metadata after the oracle's
// re-authentication window has passed.
func reauthExpiredSession(ctx context.Context, env Env) (Observation, error) {
	c, err := rawConn(ctx, env, "plain")
	if err != nil {
		return nil, err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(ReauthWindow + 20*time.Second))
	if r, err := saslPlainLogin(c, 41); err != nil || r.ErrorCode != 0 {
		return nil, fmt.Errorf("login: %v (error code %d)", err, r.ErrorCode)
	}
	before := metadataV0(c, 43)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(ReauthWindow + 3*time.Second):
	}
	return Observation{"metadata_within_window": before, "metadata_after_window": metadataV0(c, 45)}, nil
}

// reauthFranzGoLongSession: a franz-go SCRAM-SHA-512 producer (franz-go re-authenticates when the
// broker gives a session lifetime) sends one record every 2 seconds for 2.5 windows.
func reauthFranzGoLongSession(ctx context.Context, env Env) (Observation, error) {
	cl, err := saslClient(env, "plain", mechanism("SCRAM-SHA-512", saslUser, saslPassword), kgo.RequestRetries(3))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	topic := topicName(env, "reauth-long")
	if err := createTopic(ctx, kadm.NewClient(cl), topic, 1); err != nil {
		return nil, err
	}
	ok, failed := 0, 0
	deadline := time.Now().Add(ReauthWindow*5/2 + time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte(fmt.Sprint(i))}).FirstErr(); err != nil {
			failed++
		} else {
			ok++
		}
		time.Sleep(2 * time.Second)
	}
	return Observation{"all_produced": failed == 0 && ok > 0, "failed_sends": failed}, nil
}
