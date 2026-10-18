// Package tlsca generates the throwaway certificate set the security track uses, at run time.
// Nothing here is a secret: every key is created for one run, lives in the run directory (never
// in git) and is valid for two days.
//
// Files written into a directory (all PEM; private keys are unencrypted PKCS#8, which Go, the
// Java PEM keystore of Kafka and KubeMQ all read):
//
//	ca.crt                 the certificate authority both brokers trust
//	server.crt, server.key the broker certificate; SANs are the hosts given to Generate
//	server.pem             key + certificate in one file (Kafka's ssl.keystore.type=PEM)
//	<client>.crt/.key      client certificates (see Clients)
//	rogue-ca.crt           a second authority nobody trusts
package tlsca

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Client is one client certificate the track issues.
type Client struct {
	Name    string    // file name stem
	Subject pkix.Name // exact subject
	Rogue   bool      // signed by the untrusted authority instead of ca
}

// Clients are the client certificates every run issues. alice's subject has more than a common
// name on purpose: Kafka's default principal is the whole distinguished name, so a broker that
// uses only the common name is visible.
var Clients = []Client{
	{Name: "alice", Subject: pkix.Name{CommonName: "alice", OrganizationalUnit: []string{"kcert"}, Organization: []string{"KubeMQ Certification"}}},
	{Name: "admin", Subject: pkix.Name{CommonName: "admin", OrganizationalUnit: []string{"kcert"}, Organization: []string{"KubeMQ Certification"}}},
	{Name: "rogue-alice", Subject: pkix.Name{CommonName: "alice", OrganizationalUnit: []string{"kcert"}, Organization: []string{"KubeMQ Certification"}}, Rogue: true},
}

type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

// Generate writes a fresh certificate set into dir. hosts become the server certificate's SANs
// (IP addresses or DNS names).
func Generate(dir string, hosts []string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	ca, err := newAuthority("kcert test authority")
	if err != nil {
		return err
	}
	rogue, err := newAuthority("kcert rogue authority")
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(dir, "ca.crt"), "CERTIFICATE", ca.cert.Raw); err != nil {
		return err
	}
	if err := writePEM(filepath.Join(dir, "rogue-ca.crt"), "CERTIFICATE", rogue.cert.Raw); err != nil {
		return err
	}
	srv := &x509.Certificate{
		Subject:     pkix.Name{CommonName: "kcert broker"},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature,
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			srv.IPAddresses = append(srv.IPAddresses, ip)
		} else {
			srv.DNSNames = append(srv.DNSNames, h)
		}
	}
	if err := issue(dir, "server", srv, ca, true); err != nil {
		return err
	}
	for _, c := range Clients {
		tmpl := &x509.Certificate{
			Subject:     c.Subject,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			KeyUsage:    x509.KeyUsageDigitalSignature,
		}
		signer := ca
		if c.Rogue {
			signer = rogue
		}
		if err := issue(dir, c.Name, tmpl, signer, false); err != nil {
			return err
		}
	}
	return nil
}

func newAuthority(cn string) (*authority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: cn, Organization: []string{"KubeMQ Certification"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(48 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &authority{cert: cert, key: key}, nil
}

// issue signs tmpl with a and writes <name>.crt and <name>.key (and <name>.pem when combined).
func issue(dir, name string, tmpl *x509.Certificate, a *authority, combined bool) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	tmpl.SerialNumber = serial()
	tmpl.NotBefore = time.Now().Add(-time.Hour)
	tmpl.NotAfter = time.Now().Add(47 * time.Hour)
	der, err := x509.CreateCertificate(rand.Reader, tmpl, a.cert, &key.PublicKey, a.key)
	if err != nil {
		return fmt.Errorf("issue %s: %w", name, err)
	}
	pk, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(dir, name+".crt"), "CERTIFICATE", der); err != nil {
		return err
	}
	if err := writePEM(filepath.Join(dir, name+".key"), "PRIVATE KEY", pk); err != nil {
		return err
	}
	if !combined {
		return nil
	}
	b := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk})
	b = append(b, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	return os.WriteFile(filepath.Join(dir, name+".pem"), b, 0o644)
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	return n
}

// writePEM writes world-readable files: the broker containers run as unprivileged users, and
// these throwaway keys protect nothing.
func writePEM(path, typ string, der []byte) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o644)
}

// Material is a loaded certificate set, for clients.
type Material struct {
	Dir   string
	Roots *x509.CertPool // trusts ca.crt only
}

// Load reads the authority a directory's set was issued by.
func Load(dir string) (*Material, error) {
	b, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("%s/ca.crt: no certificate", dir)
	}
	return &Material{Dir: dir, Roots: pool}, nil
}

// ClientConfig is a TLS client configuration trusting the test authority, optionally presenting
// a client certificate by name ("" for none).
func (m *Material) ClientConfig(client string) (*tls.Config, error) {
	cfg := &tls.Config{RootCAs: m.Roots, MinVersion: tls.VersionTLS12}
	if client != "" {
		pair, err := tls.LoadX509KeyPair(filepath.Join(m.Dir, client+".crt"), filepath.Join(m.Dir, client+".key"))
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

// ServerPEM returns the server certificate and key, for brokers configured with inline PEM.
func ServerPEM(dir string) (cert, key []byte, err error) {
	if cert, err = os.ReadFile(filepath.Join(dir, "server.crt")); err != nil {
		return nil, nil, err
	}
	key, err = os.ReadFile(filepath.Join(dir, "server.key"))
	return cert, key, err
}
