package probe

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/security/oidc"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
)

func init() {
	register("oauthbearer-valid-token", oauthValid)
	register("oauthbearer-invalid-tokens", oauthInvalid)
	register("oauthbearer-error-challenge", oauthErrorChallenge)
	register("oauthbearer-plain-port", oauthPlainPort)
}

// rfcOAuth is an OAUTHBEARER client that follows RFC 7628 section 3.2.3, as Kafka's Java client
// does: when the server answers a bad token with an error challenge (a JSON status), the client
// sends the single byte 0x01 and the server then fails the login. It records the challenge.
// (franz-go's own mechanism treats the challenge as unexpected data.)
type rfcOAuth struct {
	token     string
	challenge *string
}

func (m rfcOAuth) Name() string { return "OAUTHBEARER" }

func (m rfcOAuth) Authenticate(context.Context, string) (sasl.Session, []byte, error) {
	return m, []byte("n,,\x01auth=Bearer " + m.token + "\x01\x01"), nil
}

func (m rfcOAuth) Challenge(resp []byte) (bool, []byte, error) {
	if len(resp) == 0 {
		return true, nil, nil
	}
	*m.challenge = ChallengeStatus(resp)
	return false, []byte{0x01}, nil
}

// ChallengeStatus is the "status" of an RFC 7628 error challenge, or "not JSON". Pure.
func ChallengeStatus(b []byte) string {
	var c struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return "not JSON"
	}
	return c.Status
}

func oauthMech(env Env, c oidc.Claims) (string, error) {
	return oidc.Token(env.Certs.Dir, c)
}

// oauthValid: a valid token for alice on the TLS port: login and a 10-record round trip.
func oauthValid(ctx context.Context, env Env) (Observation, error) {
	tok, err := oauthMech(env, oidc.Claims{})
	if err != nil {
		return nil, err
	}
	m := oauth.Auth{Token: tok}.AsMechanism()
	newClient := func(opts ...kgo.Opt) (*kgo.Client, error) { return saslClient(env, "tls", m, opts...) }
	return Observation{"tls_round_trip": roundTripOutcome(ctx, env, newClient, "oauth")}, nil
}

// oauthInvalid: tokens that must be refused on the TLS port: expired, wrong audience, wrong
// issuer, and signed by a different key under the issuer's key id.
func oauthInvalid(ctx context.Context, env Env) (Observation, error) {
	obs := Observation{}
	for field, c := range map[string]oidc.Claims{
		"expired":        {Expires: time.Now().Add(-10 * time.Minute)},
		"wrong_audience": {Audience: "someone-else"},
		"wrong_issuer":   {Issuer: "http://other-issuer:8080"},
		"bad_signature":  {Rogue: true},
	} {
		tok, err := oauthMech(env, c)
		if err != nil {
			return nil, err
		}
		var ch string
		obs[field] = authOutcome(ctx, env, "tls", rfcOAuth{token: tok, challenge: &ch})
	}
	return obs, nil
}

// oauthErrorChallenge: what the server sends before failing a login with an expired token:
// Kafka sends an RFC 7628 error challenge; the final result is in oauthbearer-invalid-tokens.
func oauthErrorChallenge(ctx context.Context, env Env) (Observation, error) {
	tok, err := oauthMech(env, oidc.Claims{Expires: time.Now().Add(-10 * time.Minute)})
	if err != nil {
		return nil, err
	}
	ch := "none"
	authOutcome(ctx, env, "tls", rfcOAuth{token: tok, challenge: &ch})
	return Observation{"error_challenge_status": ch}, nil
}

// oauthPlainPort: a valid token on the SASL_PLAINTEXT port, where Kafka allows OAUTHBEARER.
func oauthPlainPort(ctx context.Context, env Env) (Observation, error) {
	tok, err := oauthMech(env, oidc.Claims{})
	if err != nil {
		return nil, err
	}
	return Observation{"plain_port_login": authOutcome(ctx, env, "plain", oauth.Auth{Token: tok}.AsMechanism())}, nil
}
