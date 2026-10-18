package posture

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var ports = Ports{OraclePlain: 1001, OracleTLS: 1002, OracleAdmin: 1003, KubeMQPlain: 2001, KubeMQTLS: 2002, KubeMQAPI: 2003}

func TestOracleListeners(t *testing.T) {
	p := OracleProperties(Specs["tls"], ports)
	for _, want := range []string{
		"listeners=CONTROLLER://localhost:29093,ADMIN://0.0.0.0:1003,CLIENT://0.0.0.0:1001,CLIENTTLS://0.0.0.0:1002",
		"listener.security.protocol.map=CONTROLLER:PLAINTEXT,ADMIN:PLAINTEXT,CLIENT:PLAINTEXT,CLIENTTLS:SSL",
		"ssl.keystore.location=/posture/server.pem",
	} {
		if !strings.Contains(p, want+"\n") {
			t.Errorf("oracle properties lack %q", want)
		}
	}
}

func TestKubeMQConfigParses(t *testing.T) {
	var cfg struct {
		API struct {
			Port int `yaml:"port"`
		} `yaml:"api"`
		Security struct {
			Cert struct {
				Data string `yaml:"data"`
			} `yaml:"cert"`
		} `yaml:"security"`
		Connectors struct {
			Kafka struct {
				Port    string `yaml:"port"`
				TLSPort string `yaml:"tlsport"`
			} `yaml:"kafka"`
		} `yaml:"connectors"`
	}
	pemText := "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"
	if err := yaml.Unmarshal([]byte(KubeMQConfig(Specs["tls"], ports, pemText, "KEY\n", "CA\n")), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.API.Port != 2003 || cfg.Connectors.Kafka.Port != "2001" || cfg.Connectors.Kafka.TLSPort != "2002" {
		t.Errorf("ports wrong: %+v", cfg)
	}
	if cfg.Security.Cert.Data != pemText {
		t.Errorf("certificate not carried verbatim: %q", cfg.Security.Cert.Data)
	}
}

func TestMTLSPostureBothSides(t *testing.T) {
	s := Specs["mtls"]
	p := OracleProperties(s, ports)
	for _, want := range []string{
		"listener.name.clienttls.ssl.client.auth=required",
		"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
		"super.users=User:ANONYMOUS;User:" + AdminDN,
	} {
		if !strings.Contains(p, want+"\n") {
			t.Errorf("oracle properties lack %q", want)
		}
	}
	pol := KubeMQPolicy(s)
	for _, bad := range []string{",OU=", " Certification"} {
		if strings.Contains(pol, bad) {
			t.Errorf("KubeMQ policy carries %q, which its CSV loader refuses: %s", bad, pol)
		}
	}
	if !strings.Contains(pol, `"ClientID":"CN=alice.OU=kcert.O=KubeMQ.Certification"`) || !strings.Contains(pol, `"ClientID":"alice","Channel":"kcert-sec-cn-.*"`) {
		t.Errorf("KubeMQ policy lacks the two alice grants: %s", pol)
	}
	cfg := KubeMQConfig(s, ports, "C\n", "K\n", "CA\n")
	for _, want := range []string{"  ca:\n    data: |\n      CA\n", "authorization:\n  enable: true\n", "authentication:\n  enable: true\n"} {
		if !strings.Contains(cfg, want) {
			t.Errorf("KubeMQ config lacks %q:\n%s", want, cfg)
		}
	}
	if acls := KafkaACLs(s); len(acls) != 2 {
		t.Errorf("want 2 oracle ACL builders, got %d", len(acls))
	}
	if strings.Contains(OracleProperties(Specs["tls"], ports), "authorizer") || strings.Contains(KubeMQConfig(Specs["tls"], ports, "C\n", "K\n", "CA\n"), "ca:") {
		t.Error("tls posture must not ask for client certificates or authorization")
	}
}

func TestSASLPostureBothSides(t *testing.T) {
	s := Specs["sasl"]
	p := OracleProperties(s, ports)
	for _, want := range []string{
		"listener.security.protocol.map=CONTROLLER:PLAINTEXT,ADMIN:PLAINTEXT,CLIENT:SASL_PLAINTEXT,CLIENTTLS:SASL_SSL",
		"sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512",
		`listener.name.clienttls.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required user_alice="alice-secret" user_admin="admin-secret";`,
		"listener.name.client.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;",
	} {
		if !strings.Contains(p, want+"\n") {
			t.Errorf("oracle properties lack %q", want)
		}
	}
	if st := OracleStart(s); !strings.Contains(st, "--add-scram 'SCRAM-SHA-512=[name=alice,password=alice-secret,iterations=4096]'") {
		t.Errorf("oracle start script lacks alice's SCRAM credential:\n%s", st)
	}
	var cfg struct {
		Connectors struct {
			Kafka struct {
				ScramIterations int `yaml:"scramiterations"`
				Credentials     []struct {
					Username, Password string
				} `yaml:"credentials"`
			} `yaml:"kafka"`
		} `yaml:"connectors"`
	}
	if err := yaml.Unmarshal([]byte(KubeMQConfig(s, ports, "C\n", "K\n", "CA\n")), &cfg); err != nil {
		t.Fatal(err)
	}
	k := cfg.Connectors.Kafka
	if k.ScramIterations != 4096 || len(k.Credentials) != 2 || k.Credentials[0].Username != "alice" || k.Credentials[0].Password != "alice-secret" {
		t.Errorf("KubeMQ SASL block wrong: %+v", k)
	}
}

func TestKubeMQAdminIsTheKafkaSuperUser(t *testing.T) {
	if pol := KubeMQPolicy(Specs["mtls"]); !strings.Contains(pol, `"ClientID":"CN=admin.OU=kcert.O=KubeMQ.Certification","Channel":".*"`) {
		t.Errorf("mtls: KubeMQ admin must be the certificate's distinguished name, as Kafka's super user: %s", pol)
	}
	if pol := KubeMQPolicy(Specs["acl"]); !strings.Contains(pol, `"ClientID":"admin","Channel":".*"`) {
		t.Errorf("acl: KubeMQ admin must be the SASL user admin: %s", pol)
	}
}

// The acl posture's admin may manage ACLs on KubeMQ: only a principal in the Kafka connector's
// superusers list may call CreateAcls, so ApplyKubeMQACLs needs admin listed there.
func TestACLPostureAdminIsKubeMQSuperUser(t *testing.T) {
	var cfg struct {
		Connectors struct {
			Kafka struct {
				SuperUsers []string `yaml:"superusers"`
			} `yaml:"kafka"`
		} `yaml:"connectors"`
	}
	if err := yaml.Unmarshal([]byte(KubeMQConfig(Specs["acl"], ports, "C\n", "K\n", "CA\n")), &cfg); err != nil {
		t.Fatal(err)
	}
	if su := cfg.Connectors.Kafka.SuperUsers; len(su) != 1 || su[0] != "admin" {
		t.Errorf("acl: KubeMQ superusers = %q, want [admin] (the bare principal, no User: prefix)", su)
	}
	if c := KubeMQConfig(Specs["sasl"], ports, "C\n", "K\n", "CA\n"); strings.Contains(c, "superusers") {
		t.Errorf("a posture without authorization lists no super user:\n%s", c)
	}
}

// The acl posture's DENY reaches KubeMQ through CreateAcls: KubeMQACLs carries exactly the deny
// rules, written as on the oracle, and the boot policy still carries none of them.
func TestACLPostureDenyGoesThroughCreateAcls(t *testing.T) {
	s := Specs["acl"]
	acls := KubeMQACLs(s)
	if len(acls) != 1 {
		t.Fatalf("KubeMQACLs: %d rules, want the one DENY", len(acls))
	}
	if err := acls[0].ValidateCreate(); err != nil {
		t.Fatalf("the DENY is not a valid CreateAcls binding: %v", err)
	}
	if strings.Contains(KubeMQPolicy(s), ACLBlockedTopic) {
		t.Fatalf("the boot policy names %s; the DENY must come only through CreateAcls", ACLBlockedTopic)
	}
	if got := len(KafkaACLs(s)); got != len(s.Grants) {
		t.Fatalf("KafkaACLs: %d rules, want %d (every grant)", got, len(s.Grants))
	}
	if KubeMQACLs(Specs["sasl"]) != nil {
		t.Fatal("a posture without DENY rules creates nothing on KubeMQ")
	}
}

// TestReauthPostureBothSides: the reauth posture's window reaches both brokers, and only that
// posture carries it.
func TestReauthPostureBothSides(t *testing.T) {
	s := Specs["reauth"]
	if p := OracleProperties(s, ports); !strings.Contains(p, "connections.max.reauth.ms=10000\n") {
		t.Errorf("oracle properties lack the re-authentication window:\n%s", p)
	}
	var cfg struct {
		Connectors struct {
			Kafka struct {
				ConnectionsMaxReauthMs int `yaml:"connectionsmaxreauthms"`
				Credentials            []struct {
					Username, Password string
				} `yaml:"credentials"`
			} `yaml:"kafka"`
		} `yaml:"connectors"`
	}
	if err := yaml.Unmarshal([]byte(KubeMQConfig(s, ports, "C\n", "K\n", "CA\n")), &cfg); err != nil {
		t.Fatal(err)
	}
	if k := cfg.Connectors.Kafka; k.ConnectionsMaxReauthMs != 10000 || len(k.Credentials) != 2 {
		t.Errorf("KubeMQ reauth posture wrong: %+v", k)
	}
	if c := KubeMQConfig(Specs["sasl"], ports, "C\n", "K\n", "CA\n"); strings.Contains(c, "connectionsmaxreauthms") {
		t.Errorf("sasl posture must not limit KubeMQ sessions:\n%s", c)
	}
}
