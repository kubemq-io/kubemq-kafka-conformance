package tlsca

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func readCert(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(b)
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestGenerateChainsAndNames(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(dir, []string{"localhost", "127.0.0.1", "gateway.example"}); err != nil {
		t.Fatal(err)
	}
	m, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv := readCert(t, filepath.Join(dir, "server.crt"))
	for _, host := range []string{"localhost", "127.0.0.1", "gateway.example"} {
		if _, err := srv.Verify(x509.VerifyOptions{Roots: m.Roots, DNSName: host}); err != nil {
			t.Errorf("server certificate not valid for %s: %v", host, err)
		}
	}
	client := x509.VerifyOptions{Roots: m.Roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	alice := readCert(t, filepath.Join(dir, "alice.crt"))
	if _, err := alice.Verify(client); err != nil {
		t.Errorf("alice must chain to the test authority: %v", err)
	}
	if got := alice.Subject.String(); got != "CN=alice,OU=kcert,O=KubeMQ Certification" {
		t.Errorf("alice subject %q", got)
	}
	if _, err := readCert(t, filepath.Join(dir, "rogue-alice.crt")).Verify(client); err == nil {
		t.Error("rogue-alice must not chain to the test authority")
	}
	if _, err := m.ClientConfig("alice"); err != nil {
		t.Errorf("client config: %v", err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "server.pem")); len(b) == 0 {
		t.Error("server.pem missing")
	}
}
