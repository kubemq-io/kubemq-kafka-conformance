package probe

import (
	"context"
	"encoding/base64"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func init() {
	register("delegation-token", delegationToken)
	register("gssapi-configurable", gssapiConfigurable)
}

// delegationToken: alice (logged in with SCRAM-SHA-256) creates a delegation token, then logs in
// with it (SCRAM-SHA-256 with tokenauth=true).
func delegationToken(ctx context.Context, env Env) (Observation, error) {
	cl, err := saslClient(env, "plain", mechanism("SCRAM-SHA-256", saslUser, saslPassword))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	tok, err := kadm.NewClient(cl).CreateDelegationToken(ctx, kadm.CreateDelegationToken{MaxLifetime: -1})
	obs := Observation{"create": errName(err)}
	if err != nil {
		obs["token_login"] = "not attempted"
		return obs, nil
	}
	// A new token reaches the broker's token cache through the metadata log; allow it 15 seconds.
	m := scram.Auth{User: tok.TokenID, Pass: base64.StdEncoding.EncodeToString(tok.HMAC), IsToken: true}.AsSha256Mechanism()
	for i := 0; i < 15; i++ {
		if obs["token_login"] = authOutcome(ctx, env, "plain", m); obs["token_login"] == "NONE" {
			break
		}
		time.Sleep(time.Second)
	}
	return obs, nil
}

// GSSAPIRefusal is the observation for KubeMQ's answer to a GSSAPI mechanism in its config.
// Pure, for the tests.
func GSSAPIRefusal(out string) string {
	if strings.Contains(out, `unknown SaslMechanisms entry "GSSAPI"`) {
		return "refused by configuration validation"
	}
	if strings.TrimSpace(out) == "" {
		return "no output"
	}
	return "not refused"
}

// gssapiConfigurable has no oracle leg (methodology: GSSAPI evidence row). Apache Kafka supports
// SASL/GSSAPI (Kerberos) as documented; running it would need a Kerberos KDC. On KubeMQ the row
// starts the released image with GSSAPI in its mechanism list and records the answer of its
// configuration validation, which runs before the license check, so no key is passed.
func gssapiConfigurable(ctx context.Context, env Env) (Observation, error) {
	if env.Target == "real-kafka" {
		return Observation{"gssapi_mechanism": "configurable (documented; not run)"}, nil
	}
	image := os.Getenv("KUBEMQ_IMAGE_REPO") + "@" + os.Getenv("KUBEMQ_IMAGE_DIGEST")
	out, _ := clientRunPlain(ctx, []string{
		"-e", "STORE_ENGINE=next", "-e", "CONNECTORS_KAFKA_ENABLE=true", "-e", "CONNECTORS_KAFKA_PORT=9092",
		"-e", "CONNECTORS_KAFKA_SASL_MECHANISMS=GSSAPI",
		"-e", "CONNECTORS_KAFKA_DEFAULT_USER=alice", "-e", "CONNECTORS_KAFKA_DEFAULT_PASS=alice-secret",
	}, image)
	return Observation{"gssapi_mechanism": GSSAPIRefusal(string(out))}, nil
}
