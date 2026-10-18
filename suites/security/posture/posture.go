// Package posture writes the boot-time configuration of both brokers for one security posture.
//
// Both brokers get the same listeners per posture: a plain port and a TLS port. The runner boots
// them from suites/security/compose-oracle.yaml and compose-kubemq.yaml with the files written
// here (runner/lib/brokers.sh, posture_up). Each product is configured the way its own
// documentation says:
//
//	Kafka 4.3.1  server.properties: listeners CLIENT (plain port) and CLIENTTLS (TLS port), plus an
//	             ADMIN plaintext listener for the runner's readiness check and track setup
//	KubeMQ       config YAML: certificates in the global security block, the Kafka connector's
//	             port and tlsport (KubeMQ has no Kafka-specific TLS settings)
package posture

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/security/oidc"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/security/tlsca"
)

// Ports are the posture's host ports (the runner's POSTURE_* variables). Containers listen on
// the same port numbers they publish.
type Ports struct {
	OraclePlain, OracleTLS, OracleAdmin int
	KubeMQPlain, KubeMQTLS, KubeMQAPI   int
}

// Spec is what one posture switches on.
type Spec struct {
	Name string
	// PlainProtocol and TLSProtocol are the Kafka security protocols of the two client ports.
	PlainProtocol, TLSProtocol string
	// ClientAuth: the TLS port requires a client certificate signed by the test authority
	// (Kafka listener ssl.client.auth=required; KubeMQ security.ca).
	ClientAuth bool
	// Authorize turns authorization on (Kafka StandardAuthorizer; KubeMQ authentication and
	// authorization with a policy) and grants Grants. The admin client certificate may do
	// everything on both brokers (Kafka super user; KubeMQ policy for its principal).
	Authorize bool
	Grants    []Grant
	// SuperUser is the Kafka principal of the admin client ("User:..."). The KubeMQ policy
	// grants the same principal everything (KubeMQClientID: written as a regular expression
	// KubeMQ's policy loader accepts).
	SuperUser string
	// SASL: both client ports require SASL (SASL_PLAINTEXT and SASL_SSL) with PLAIN,
	// SCRAM-SHA-256 and SCRAM-SHA-512 for Users. Kafka: JAAS user list for PLAIN, SCRAM
	// credentials written at storage format; KubeMQ: the Kafka connector's credentials list.
	SASL bool
	// OAuth adds OAUTHBEARER (implies SASL) with tokens from the run's issuer
	// (suites/security/oidc). Kafka offers it on both client ports, as it allows; KubeMQ offers
	// it only on its TLS port.
	OAuth bool
	// ReauthMS sets connections.max.reauth.ms (KIP-368) on both brokers: the oracle's
	// server.properties and KubeMQ's Connectors.Kafka.ConnectionsMaxReauthMs (environment
	// CONNECTORS_KAFKA_CONNECTIONS_MAX_REAUTH_MS, KubeMQ 3.6.18 and later), written as the
	// connectionsmaxreauthms key of the Kafka connector block because the config YAML is the
	// only per-posture input the KubeMQ container takes. A KubeMQ release without the setting
	// ignores the key and gives sessions no lifetime.
	ReauthMS int
	// QuotaBytesPerSec is the produce and fetch byte-rate quota for every user (Kafka: default
	// user entity producer_byte_rate and consumer_byte_rate, set over the admin listener after
	// boot; KubeMQ: the Kafka connector's producebyterate and fetchbyterate, per principal).
	QuotaBytesPerSec int
	// DelegationTokens enables delegation tokens on the oracle (delegation.token.secret.key).
	// KubeMQ has no delegation-token setting.
	DelegationTokens bool
}

// User is one SASL user, the same on both brokers. Passwords are throwaway test values.
type User struct{ Name, Password string }

// Users are the SASL users of every SASL posture.
var Users = []User{{"alice", "alice-secret"}, {"admin", "admin-secret"}}

// ScramIterations is the SCRAM iteration count on both brokers (Kafka's minimum and KubeMQ's
// default).
const ScramIterations = 4096

// Grant is one permission written the way each product spells it: a Kafka ACL and the
// equivalent KubeMQ policy entry. KubeMQ policies have only Read and Write: Kafka's Read and
// Describe map to Read; Write, Create, Alter and Delete map to Write (the mapping KubeMQ's own
// authorizer applies per request).
type Grant struct {
	KafkaPrincipal string // "User:<name>"
	KubeMQClientID string // KubeMQ policy ClientID (an anchored regular expression)
	Resource       string // "topic" (default), "group" or "transactional-id"
	Prefix         string // resource name prefix (Kafka PREFIXED pattern; KubeMQ "<prefix>.*")
	Literal        bool   // Kafka LITERAL pattern on exactly Prefix (KubeMQ: exactly that name)
	// Ops are the Kafka operations; nil means Read, Write and Describe.
	Ops []kadm.ACLOperation
	// Deny makes the rule a DENY. On both brokers it is created through CreateAcls after boot
	// (ApplyKafkaACLs, ApplyKubeMQACLs); it has no entry in the KubeMQ boot policy.
	Deny bool
}

func (g Grant) ops() []kadm.ACLOperation {
	if g.Ops == nil {
		return []kadm.ACLOperation{kadm.OpRead, kadm.OpWrite, kadm.OpDescribe}
	}
	return g.Ops
}

// kubemqAccess is the KubeMQ Read and Write equivalent of a grant's Kafka operations.
func (g Grant) kubemqAccess() (read, write bool) {
	for _, op := range g.ops() {
		switch op {
		case kadm.OpRead, kadm.OpDescribe:
			read = true
		default:
			write = true
		}
	}
	return read, write
}

// KubeMQClientID writes a principal name as a KubeMQ policy ClientID. KubeMQ's policy loader
// parses entries as CSV and (through v3.6.16) refuses commas, spaces and backslashes in a
// ClientID, so each comma and space becomes "."; the ClientID is a regular expression, so the
// result still matches the name.
func KubeMQClientID(principal string) string {
	return strings.NewReplacer(",", ".", " ", ".").Replace(principal)
}

// AdminDN is the admin client certificate's distinguished name, Kafka's default principal for it.
const AdminDN = "CN=admin,OU=kcert,O=KubeMQ Certification"

// AliceDN is alice's distinguished name (tlsca.Clients).
const AliceDN = "CN=alice,OU=kcert,O=KubeMQ Certification"

// Specs are the postures the track knows. A matrix row names one.
var Specs = map[string]Spec{
	"tls":    {Name: "tls", PlainProtocol: "PLAINTEXT", TLSProtocol: "SSL"},
	"sasl":   {Name: "sasl", PlainProtocol: "SASL_PLAINTEXT", TLSProtocol: "SASL_SSL", SASL: true, DelegationTokens: true},
	"reauth": {Name: "reauth", PlainProtocol: "SASL_PLAINTEXT", TLSProtocol: "SASL_SSL", SASL: true, ReauthMS: 10000},
	"oauth":  {Name: "oauth", PlainProtocol: "SASL_PLAINTEXT", TLSProtocol: "SASL_SSL", SASL: true, OAuth: true},
	// mtls: the same grant is written once for alice's full distinguished name (what Kafka's
	// default principal mapping produces) and once for her common name alone, on two topic
	// prefixes, so the principal each broker derives from the certificate is observable.
	"mtls": {Name: "mtls", PlainProtocol: "PLAINTEXT", TLSProtocol: "SSL", ClientAuth: true, Authorize: true, SuperUser: "User:" + AdminDN, Grants: []Grant{
		// KubeMQ's policy loader parses entries as CSV and refuses commas, spaces and backslashes
		// in a ClientID (the broker does not boot), so a distinguished name cannot be written
		// literally. The ClientID is a regular expression: each comma and space is written as
		// ".", which still matches alice's distinguished name.
		{KafkaPrincipal: "User:" + AliceDN, KubeMQClientID: KubeMQClientID(AliceDN), Prefix: "kcert-sec-dn-"},
		{KafkaPrincipal: "User:alice", KubeMQClientID: "alice", Prefix: "kcert-sec-cn-"},
	}},
	"quota": {Name: "quota", PlainProtocol: "SASL_PLAINTEXT", TLSProtocol: "SASL_SSL", SASL: true, QuotaBytesPerSec: 256 * 1024},
	// acl: SASL users, authorization on; alice's grants, written for each product.
	"acl": {Name: "acl", PlainProtocol: "SASL_PLAINTEXT", TLSProtocol: "SASL_SSL", SASL: true, Authorize: true, SuperUser: "User:admin", Grants: []Grant{
		{KafkaPrincipal: "User:alice", KubeMQClientID: "alice", Prefix: "kcert-acl-ok-",
			Ops: []kadm.ACLOperation{kadm.OpRead, kadm.OpWrite, kadm.OpDescribe, kadm.OpCreate}},
		{KafkaPrincipal: "User:alice", KubeMQClientID: "alice", Resource: "group", Prefix: "kcert-acl-g-",
			Ops: []kadm.ACLOperation{kadm.OpRead}},
		{KafkaPrincipal: "User:alice", KubeMQClientID: "alice", Resource: "transactional-id", Prefix: "kcert-acl-tx-",
			Ops: []kadm.ACLOperation{kadm.OpWrite, kadm.OpDescribe}},
		// Write only: Kafka's Write implies Describe (Metadata), so this grant alone must let alice
		// see the topic.
		{KafkaPrincipal: "User:alice", KubeMQClientID: "alice", Prefix: "kcert-acl-w-",
			Ops: []kadm.ACLOperation{kadm.OpWrite}},
		{KafkaPrincipal: "User:alice", KubeMQClientID: "alice", Prefix: ACLBlockedTopic, Literal: true,
			Ops: []kadm.ACLOperation{kadm.OpWrite}, Deny: true},
	}},
}

// ACLBlockedTopic is inside alice's allowed prefix but carries a Kafka DENY rule for Write.
const ACLBlockedTopic = "kcert-acl-ok-blocked"

// Names lists the known postures, sorted.
func Names() []string {
	var out []string
	for n := range Specs {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Write writes into dir everything both brokers need for posture name:
//
//	ca.crt server.pem   mounted into the oracle at /posture
//	oracle.properties   Kafka server.properties
//	oracle-start.sh     formats the KRaft log and starts Kafka (the compose entrypoint)
//	kubemq.config       KubeMQ config YAML (the runner passes it as the image's CONFIG variable)
func Write(dir, name string, p Ports, certDir string) error {
	spec, ok := Specs[name]
	if !ok {
		return fmt.Errorf("unknown posture %q (known: %s)", name, strings.Join(Names(), ", "))
	}
	for _, f := range []string{"ca.crt", "server.pem"} {
		b, err := os.ReadFile(filepath.Join(certDir, f))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, f), b, 0o644); err != nil {
			return err
		}
	}
	cert, key, err := tlsca.ServerPEM(certDir)
	if err != nil {
		return err
	}
	ca, err := os.ReadFile(filepath.Join(certDir, "ca.crt"))
	if err != nil {
		return err
	}
	// The issuer's file server in compose-kubemq.yaml serves this directory in every posture.
	if err := os.MkdirAll(filepath.Join(dir, "oidc"), 0o755); err != nil {
		return err
	}
	if spec.OAuth {
		if err := oidc.WriteIssuer(filepath.Join(dir, "oidc"), certDir); err != nil {
			return err
		}
	}
	if spec.ReauthMS > 0 {
		// Java client settings for the re-authentication row (alice, SCRAM-SHA-512).
		props := "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\n" +
			"sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"alice\" password=\"alice-secret\";\n"
		if err := os.WriteFile(filepath.Join(dir, "reauth-java.properties"), []byte(props), 0o644); err != nil {
			return err
		}
	}
	files := map[string]string{
		"oracle.properties": OracleProperties(spec, p),
		"oracle-start.sh":   OracleStart(spec),
		"kubemq.config":     KubeMQConfig(spec, p, string(cert), string(key), string(ca)),
	}
	for f, body := range files {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(body), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// OracleProperties is the oracle's server.properties for a posture. Everything not listed is a
// Kafka default.
func OracleProperties(s Spec, p Ports) string {
	lines := []string{
		"process.roles=broker,controller",
		"node.id=1",
		"controller.quorum.voters=1@localhost:29093",
		"controller.listener.names=CONTROLLER",
		"inter.broker.listener.name=ADMIN",
		fmt.Sprintf("listeners=CONTROLLER://localhost:29093,ADMIN://0.0.0.0:%d,CLIENT://0.0.0.0:%d,CLIENTTLS://0.0.0.0:%d", p.OracleAdmin, p.OraclePlain, p.OracleTLS),
		fmt.Sprintf("advertised.listeners=ADMIN://127.0.0.1:%d,CLIENT://127.0.0.1:%d,CLIENTTLS://127.0.0.1:%d", p.OracleAdmin, p.OraclePlain, p.OracleTLS),
		fmt.Sprintf("listener.security.protocol.map=CONTROLLER:PLAINTEXT,ADMIN:PLAINTEXT,CLIENT:%s,CLIENTTLS:%s", s.PlainProtocol, s.TLSProtocol),
		"log.dirs=/tmp/kraft-combined-logs",
		"num.partitions=1",
		// Single node: internal topics cannot have more than one copy.
		"offsets.topic.replication.factor=1",
		"transaction.state.log.replication.factor=1",
		"transaction.state.log.min.isr=1",
		"share.coordinator.state.topic.replication.factor=1",
		"share.coordinator.state.topic.min.isr=1",
		"group.initial.rebalance.delay.ms=0",
		"ssl.keystore.type=PEM",
		"ssl.keystore.location=/posture/server.pem",
		"ssl.truststore.type=PEM",
		"ssl.truststore.location=/posture/ca.crt",
	}
	if s.ClientAuth {
		lines = append(lines, "listener.name.clienttls.ssl.client.auth=required")
	}
	if s.SASL {
		var plain strings.Builder
		plain.WriteString("org.apache.kafka.common.security.plain.PlainLoginModule required")
		for _, u := range Users {
			fmt.Fprintf(&plain, " user_%s=\"%s\"", u.Name, u.Password)
		}
		plain.WriteString(";")
		mechs := "PLAIN,SCRAM-SHA-256,SCRAM-SHA-512"
		if s.OAuth {
			mechs += ",OAUTHBEARER"
		}
		lines = append(lines, "sasl.enabled.mechanisms="+mechs)
		for _, l := range []string{"client", "clienttls"} {
			lines = append(lines,
				"listener.name."+l+".plain.sasl.jaas.config="+plain.String(),
				"listener.name."+l+".scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;",
				"listener.name."+l+".scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;")
			if s.OAuth {
				lines = append(lines,
					"listener.name."+l+".oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;",
					"listener.name."+l+".oauthbearer.sasl.server.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler")
			}
		}
		if s.DelegationTokens {
			// Throwaway signing secret for this run's tokens; not a secret.
			lines = append(lines, "delegation.token.secret.key=kcert-delegation-token-key-not-a-secret")
		}
		if s.ReauthMS > 0 {
			lines = append(lines, fmt.Sprintf("connections.max.reauth.ms=%d", s.ReauthMS))
		}
		if s.OAuth {
			// The key set is read from a file (allowed by KAFKA_OPTS in compose-oracle.yaml); the
			// issuer is only compared, never fetched.
			lines = append(lines,
				"sasl.oauthbearer.jwks.endpoint.url=file:///posture/oidc/jwks.json",
				"sasl.oauthbearer.expected.audience="+oidc.Audience,
				"sasl.oauthbearer.expected.issuer="+oidc.Issuer)
		}
	}
	if s.Authorize {
		lines = append(lines,
			"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
			// ANONYMOUS is the ADMIN and CONTROLLER listeners' principal; no row uses them.
			"super.users=User:ANONYMOUS;"+s.SuperUser)
	}
	return strings.Join(lines, "\n") + "\n"
}

// KubeMQPolicy is the KubeMQ authorization policy for a posture's grants, plus the admin
// certificate's principal (its common name, which is what KubeMQ derives) on everything.
func KubeMQPolicy(s Spec) string {
	type entry struct {
		EventsStore bool   `json:"EventsStore"`
		ClientID    string `json:"ClientID"`
		Channel     string `json:"Channel"`
		Read        bool   `json:"Read"`
		Write       bool   `json:"Write"`
	}
	policy := []entry{{EventsStore: true, ClientID: KubeMQClientID(strings.TrimPrefix(s.SuperUser, "User:")), Channel: ".*", Read: true, Write: true}}
	for _, g := range s.Grants {
		if g.Deny {
			// Created on the running broker through CreateAcls instead (ApplyKubeMQACLs), the
			// way a Kafka operator creates it.
			continue
		}
		ch := regexp.QuoteMeta(g.Prefix) + ".*"
		if g.Literal {
			ch = regexp.QuoteMeta(g.Prefix)
		}
		r, w := g.kubemqAccess()
		policy = append(policy, entry{EventsStore: true, ClientID: g.KubeMQClientID, Channel: ch, Read: r, Write: w})
	}
	b, _ := json.Marshal(policy)
	return string(b)
}

// KafkaACLs are the oracle's ACLs for a posture's grants, created over the admin listener after
// boot (Kafka keeps ACLs in its metadata log, not in server.properties).
func KafkaACLs(s Spec) []*kadm.ACLBuilder {
	var out []*kadm.ACLBuilder
	for _, g := range s.Grants {
		out = append(out, aclBuilder(g))
	}
	return out
}

// KubeMQACLs are the ACLs KubeMQ gets through CreateAcls after boot: the posture's DENY rules,
// which its boot policy does not carry. The allow grants are in the boot policy (KubeMQPolicy).
func KubeMQACLs(s Spec) []*kadm.ACLBuilder {
	var out []*kadm.ACLBuilder
	for _, g := range s.Grants {
		if g.Deny {
			out = append(out, aclBuilder(g))
		}
	}
	return out
}

// aclBuilder writes one grant as a Kafka ACL binding.
func aclBuilder(g Grant) *kadm.ACLBuilder {
	b := kadm.NewACLs()
	if g.Deny {
		b = b.Deny(g.KafkaPrincipal).DenyHosts("*")
	} else {
		b = b.Allow(g.KafkaPrincipal).AllowHosts("*")
	}
	switch g.Resource {
	case "group":
		b = b.Groups(g.Prefix)
	case "transactional-id":
		b = b.TransactionalIDs(g.Prefix)
	default:
		b = b.Topics(g.Prefix)
	}
	pattern := kadm.ACLPatternPrefixed
	if g.Literal {
		pattern = kadm.ACLPatternLiteral
	}
	return b.ResourcePatternType(pattern).Operations(g.ops()...)
}

// ApplyKafkaACLs creates a posture's ACLs on the oracle through its plaintext admin listener.
func ApplyKafkaACLs(ctx context.Context, adminAddr string, s Spec) error {
	acls := KafkaACLs(s)
	if len(acls) == 0 {
		return nil
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(adminAddr))
	if err != nil {
		return err
	}
	defer cl.Close()
	return createACLs(ctx, kadm.NewClient(cl), acls, "the oracle")
}

// ApplyKubeMQACLs creates a posture's KubeMQACLs on KubeMQ through CreateAcls, as the admin user
// over SASL PLAIN on its plain port (KubeMQ has no separate admin listener; the boot policy
// grants admin everything).
func ApplyKubeMQACLs(ctx context.Context, plainAddr string, s Spec) error {
	acls := KubeMQACLs(s)
	if len(acls) == 0 {
		return nil
	}
	var admin User
	for _, u := range Users {
		if "User:"+u.Name == s.SuperUser {
			admin = u
		}
	}
	if admin.Name == "" {
		return fmt.Errorf("posture %s: super user %s is not a SASL user", s.Name, s.SuperUser)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(plainAddr),
		kgo.SASL(plain.Auth{User: admin.Name, Pass: admin.Password}.AsMechanism()))
	if err != nil {
		return err
	}
	defer cl.Close()
	return createACLs(ctx, kadm.NewClient(cl), acls, "KubeMQ")
}

func createACLs(ctx context.Context, adm *kadm.Client, acls []*kadm.ACLBuilder, broker string) error {
	for _, b := range acls {
		res, err := adm.CreateACLs(ctx, b)
		if err != nil {
			return fmt.Errorf("create ACLs on %s: %w", broker, err)
		}
		for _, r := range res {
			if r.Err != nil {
				return fmt.Errorf("create ACL for %s on %s on %s: %w", r.Principal, r.Name, broker, r.Err)
			}
		}
	}
	return nil
}

// OracleStart is the oracle container's entrypoint script. SCRAM credentials are written into
// the metadata log at format time, so they exist before the first client connects.
func OracleStart(s Spec) string {
	scram := ""
	if s.SASL {
		for _, u := range Users {
			for _, m := range []string{"SCRAM-SHA-256", "SCRAM-SHA-512"} {
				scram += fmt.Sprintf(" --add-scram '%s=[name=%s,password=%s,iterations=%d]'", m, u.Name, u.Password, ScramIterations)
			}
		}
	}
	return `#!/bin/sh
set -e
/opt/kafka/bin/kafka-storage.sh format -t "$(/opt/kafka/bin/kafka-storage.sh random-uuid)" -c /posture/oracle.properties` + scram + `
exec /opt/kafka/bin/kafka-server-start.sh /posture/oracle.properties
`
}

// KubeMQConfig is KubeMQ's config YAML for a posture. The other connectors are switched off by
// environment in compose-kubemq.yaml, exactly as in the kit's plain KubeMQ.
func KubeMQConfig(s Spec, p Ports, certPEM, keyPEM, caPEM string) string {
	var extra strings.Builder
	if s.ClientAuth {
		fmt.Fprintf(&extra, "  ca:\n    data: |\n%s\n", indent(caPEM, 6))
	}
	if s.Authorize {
		// KubeMQ authorization requires authentication to be on; its JWT key only matters for
		// KubeMQ's own token login, which no row uses. Not a secret.
		fmt.Fprintf(&extra, "authentication:\n  enable: true\n  jwtconfig:\n    key: \"kcert-posture-hs256-key-not-a-secret-0123456789\"\n    signaturetype: \"HS256\"\n")
		fmt.Fprintf(&extra, "authorization:\n  enable: true\n  policydata: '%s'\n", KubeMQPolicy(s))
	}
	return fmt.Sprintf(`api:
  enable: true
  port: %d
store:
  engine: next
security:
  cert:
    data: |
%s
  key:
    data: |
%s
%sconnectors:
  kafka:
    enable: true
    port: "%d"
    tlsport: "%d"
    advertisedhost: "127.0.0.1"
%s`, p.KubeMQAPI, indent(certPEM, 6), indent(keyPEM, 6), extra.String(), p.KubeMQPlain, p.KubeMQTLS, kubemqSASL(s))
}

func indent(s string, n int) string {
	pad := strings.Repeat(" ", n)
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i := range lines {
		lines[i] = pad + lines[i]
	}
	return strings.Join(lines, "\n")
}

// kubemqSASL is the Kafka connector's SASL block: the static credentials list (KubeMQ reads SASL
// users only at boot) and the SCRAM iteration count. Empty for a posture without SASL.
func kubemqSASL(s Spec) string {
	if !s.SASL {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "    scramiterations: %d\n    credentials:\n", ScramIterations)
	for _, u := range Users {
		fmt.Fprintf(&b, "      - username: %q\n        password: %q\n", u.Name, u.Password)
	}
	if s.QuotaBytesPerSec > 0 {
		fmt.Fprintf(&b, "    producebyterate: %d\n    fetchbyterate: %d\n", s.QuotaBytesPerSec, s.QuotaBytesPerSec)
	}
	if s.ReauthMS > 0 {
		fmt.Fprintf(&b, "    connectionsmaxreauthms: %d\n", s.ReauthMS)
	}
	if s.OAuth {
		fmt.Fprintf(&b, "    oauthbearer:\n      issuer: %q\n      clientid: %q\n", oidc.Issuer, oidc.Audience)
	}
	if s.Authorize && s.SuperUser != "" {
		// Kafka's super.users. A policy grant on ".*" lets the admin read and write every topic but
		// not manage ACLs: KubeMQ (v3.6.18 and newer) answers CreateAcls, DeleteAcls and
		// DescribeAcls only for a principal in this list, so without it ApplyKubeMQACLs is refused
		// CLUSTER_AUTHORIZATION_FAILED and the posture's DENY rules never reach the broker.
		fmt.Fprintf(&b, "    superusers:\n      - %q\n", strings.TrimPrefix(s.SuperUser, "User:"))
	}
	return b.String()
}

// ApplyKafkaQuotas sets a posture's default user quotas on the oracle through its admin listener.
func ApplyKafkaQuotas(ctx context.Context, adminAddr string, s Spec) error {
	if s.QuotaBytesPerSec <= 0 {
		return nil
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(adminAddr))
	if err != nil {
		return err
	}
	defer cl.Close()
	rate := float64(s.QuotaBytesPerSec)
	res, err := kadm.NewClient(cl).AlterClientQuotas(ctx, []kadm.AlterClientQuotaEntry{{
		Entity: kadm.ClientQuotaEntity{{Type: "user"}},
		Ops:    []kadm.AlterClientQuotaOp{{Key: "producer_byte_rate", Value: rate}, {Key: "consumer_byte_rate", Value: rate}},
	}})
	if err != nil {
		return fmt.Errorf("set default user quotas on the oracle: %w", err)
	}
	for _, r := range res {
		if r.Err != nil {
			return fmt.Errorf("set default user quotas on the oracle: %w", r.Err)
		}
	}
	return nil
}
