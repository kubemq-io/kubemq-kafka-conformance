package oidc

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// verify checks a token against the published key set, the way a broker does.
func verify(t *testing.T, dir, tok string) bool {
	t.Helper()
	var set struct {
		Keys []struct{ N, E, Kid string } `json:"keys"`
	}
	b, _ := os.ReadFile(filepath.Join(dir, "jwks.json"))
	if err := json.Unmarshal(b, &set); err != nil || len(set.Keys) != 1 || set.Keys[0].Kid != KeyID {
		t.Fatalf("key set: %s %v", b, err)
	}
	n, _ := base64.RawURLEncoding.DecodeString(set.Keys[0].N)
	e, _ := base64.RawURLEncoding.DecodeString(set.Keys[0].E)
	pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
	parts := strings.Split(tok, ".")
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	return rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig) == nil
}

func TestIssuerAndTokens(t *testing.T) {
	dir := t.TempDir()
	if err := GenerateKeys(dir); err != nil {
		t.Fatal(err)
	}
	if err := WriteIssuer(dir, dir); err != nil {
		t.Fatal(err)
	}
	var disc map[string]any
	b, _ := os.ReadFile(filepath.Join(dir, ".well-known", "openid-configuration"))
	if err := json.Unmarshal(b, &disc); err != nil || disc["issuer"] != Issuer || disc["jwks_uri"] != Issuer+"/jwks.json" {
		t.Fatalf("discovery document: %s", b)
	}
	good, err := Token(dir, Claims{})
	if err != nil || !verify(t, dir, good) {
		t.Fatalf("a valid token must verify: %v", err)
	}
	rogue, _ := Token(dir, Claims{Rogue: true})
	if verify(t, dir, rogue) {
		t.Fatal("a token signed by the rogue key must not verify")
	}
	body, _ := base64.RawURLEncoding.DecodeString(strings.Split(good, ".")[1])
	var c map[string]any
	_ = json.Unmarshal(body, &c)
	if c["iss"] != Issuer || c["aud"] != Audience || c["sub"] != "alice" {
		t.Fatalf("claims %v", c)
	}
}
