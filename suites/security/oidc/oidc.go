// Package oidc is the security track's token issuer for OAUTHBEARER rows. It needs no server
// process: the discovery document and the key set are static files, served to KubeMQ by a small
// file server in suites/security/compose-kubemq.yaml and read by the Kafka oracle from a file
// (sasl.oauthbearer.jwks.endpoint.url=file:...). Tokens are signed by the probes themselves with
// the run's signing key. Keys are created per run and never committed.
package oidc

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

const (
	// Issuer is the issuer URL in every token and in both brokers' config. KubeMQ fetches its
	// discovery document from it (the file server's compose service name); Kafka only compares it.
	Issuer = "http://kcert-oidc:8080"
	// Audience is the expected audience on both brokers (KubeMQ calls it the client id).
	Audience = "kcert-kafka"
	// KeyID names the signing key in the key set; the rogue key uses the same id.
	KeyID = "kcert-k1"
)

// GenerateKeys writes oidc-signing.key and oidc-rogue.key (RSA 2048, PKCS#8 PEM) into dir.
func GenerateKeys(dir string) error {
	for _, name := range []string{"oidc-signing.key", "oidc-rogue.key"} {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return err
		}
		der, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, name), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func loadKey(path string) (*rsa.PrivateKey, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	blk, _ := pem.Decode(b)
	if blk == nil {
		return nil, fmt.Errorf("%s: no PEM block", path)
	}
	k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		return nil, err
	}
	rk, ok := k.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s: not an RSA key", path)
	}
	return rk, nil
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// WriteIssuer writes the issuer's static files into dir: .well-known/openid-configuration and
// jwks.json (the public half of the signing key in keyDir).
func WriteIssuer(dir, keyDir string) error {
	k, err := loadKey(filepath.Join(keyDir, "oidc-signing.key"))
	if err != nil {
		return err
	}
	jwks := map[string]any{"keys": []map[string]string{{
		"kty": "RSA", "use": "sig", "alg": "RS256", "kid": KeyID,
		"n": b64(k.N.Bytes()), "e": b64(big.NewInt(int64(k.E)).Bytes()),
	}}}
	disc := map[string]any{
		"issuer": Issuer, "jwks_uri": Issuer + "/jwks.json",
		"authorization_endpoint": Issuer + "/authorize", "token_endpoint": Issuer + "/token",
		"response_types_supported": []string{"token"}, "subject_types_supported": []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
	}
	if err := os.MkdirAll(filepath.Join(dir, ".well-known"), 0o755); err != nil {
		return err
	}
	for path, v := range map[string]any{"jwks.json": jwks, ".well-known/openid-configuration": disc} {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, path), b, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// Claims are the claims of one token. Zero values take the valid defaults.
type Claims struct {
	Subject, Issuer, Audience string
	Expires                   time.Time
	Rogue                     bool // sign with the rogue key (same key id, wrong key)
}

// Token is a signed RS256 JWT for c, using the keys in keyDir.
func Token(keyDir string, c Claims) (string, error) {
	keyFile := "oidc-signing.key"
	if c.Rogue {
		keyFile = "oidc-rogue.key"
	}
	k, err := loadKey(filepath.Join(keyDir, keyFile))
	if err != nil {
		return "", err
	}
	now := time.Now()
	if c.Subject == "" {
		c.Subject = "alice"
	}
	if c.Issuer == "" {
		c.Issuer = Issuer
	}
	if c.Audience == "" {
		c.Audience = Audience
	}
	if c.Expires.IsZero() {
		c.Expires = now.Add(time.Hour)
	}
	hdr, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": KeyID})
	body, _ := json.Marshal(map[string]any{
		"iss": c.Issuer, "aud": c.Audience, "sub": c.Subject, "scope": "kafka",
		"iat": now.Add(-time.Minute).Unix(), "nbf": now.Add(-time.Minute).Unix(), "exp": c.Expires.Unix(),
		"jti": fmt.Sprintf("kcert-%d", now.UnixNano()),
	})
	signing := b64(hdr) + "." + b64(body)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	return signing + "." + b64(sig), nil
}
