// Package apikeys queries a broker's ApiVersions and diffs the Kafka oracle's against
// KubeMQ's. Shared by cmd/apikeys-gen (writes matrix/apikeys.yaml) and cmd/protocol-track
// (gives every key a verdict on each run).
package apikeys

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"gopkg.in/yaml.v3"
)

type Versions struct {
	Keys     map[int16][2]int16
	Features map[string]Feature
}

type Feature struct {
	SupportedMin, SupportedMax int16
	Finalized                  *int16
}

type Impact struct {
	Key    int16  `yaml:"key"`
	Impact string `yaml:"impact"`
	Breaks string `yaml:"breaks"`
	Scope  string `yaml:"scope,omitempty"`
}

// Output schema of matrix/apikeys.yaml.
type KeyRow struct {
	ID        string `yaml:"id"`
	Key       int16  `yaml:"key"`
	Name      string `yaml:"name"`
	OracleMin int16  `yaml:"oracle_min"`
	OracleMax int16  `yaml:"oracle_max"`
	KubeMQMin *int16 `yaml:"kubemq_min"`
	KubeMQMax *int16 `yaml:"kubemq_max"`
	// Status: full (KubeMQ covers the oracle's whole range), narrower (advertised with a
	// smaller range), not-advertised, kubemq-only (KubeMQ advertises a key the oracle does not).
	Status     string `yaml:"status"`
	Breaks     string `yaml:"breaks,omitempty"`
	Impact     string `yaml:"impact,omitempty"`
	Provenance string `yaml:"provenance"`
	Scope      string `yaml:"scope,omitempty"`
}

type FeatureRow struct {
	ID              string `yaml:"id"`
	Feature         string `yaml:"feature"`
	OracleSupported string `yaml:"oracle_supported"`
	OracleFinalized string `yaml:"oracle_finalized"`
	KubeMQSupported string `yaml:"kubemq_supported"`
	KubeMQFinalized string `yaml:"kubemq_finalized"`
	Status          string `yaml:"status"`
	Provenance      string `yaml:"provenance"`
}

type Doc struct {
	Generated string         `yaml:"generated"`
	Oracle    string         `yaml:"oracle"`
	KubeMQ    string         `yaml:"kubemq"`
	Summary   map[string]int `yaml:"summary"`
	Keys      []KeyRow       `yaml:"keys"`
	Features  []FeatureRow   `yaml:"features"`
}

func Query(ctx context.Context, addr string) (Versions, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("kcert-apikeys-gen"))
	if err != nil {
		return Versions{}, err
	}
	defer cl.Close()
	req := kmsg.NewPtrApiVersionsRequest()
	req.ClientSoftwareName = "kcert-apikeys-gen"
	req.ClientSoftwareVersion = "1"
	var resp *kmsg.ApiVersionsResponse
	for {
		r, err := req.RequestWith(ctx, cl)
		if err == nil {
			resp = r
			break
		}
		select {
		case <-ctx.Done():
			return Versions{}, fmt.Errorf("ApiVersions to %s: %w", addr, err)
		case <-time.After(time.Second):
		}
	}
	if resp.ErrorCode != 0 {
		return Versions{}, fmt.Errorf("ApiVersions to %s: error code %d", addr, resp.ErrorCode)
	}
	v := Versions{Keys: map[int16][2]int16{}, Features: map[string]Feature{}}
	for _, k := range resp.ApiKeys {
		v.Keys[k.ApiKey] = [2]int16{k.MinVersion, k.MaxVersion}
	}
	for _, f := range resp.SupportedFeatures {
		v.Features[f.Name] = Feature{SupportedMin: f.MinVersion, SupportedMax: f.MaxVersion}
	}
	for _, f := range resp.FinalizedFeatures {
		x := v.Features[f.Name]
		lvl := f.MaxVersionLevel
		x.Finalized = &lvl
		v.Features[f.Name] = x
	}
	return v, nil
}

func LoadImpacts(path string) (map[int16]Impact, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc struct {
		Impacts []Impact `yaml:"impacts"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	m := map[int16]Impact{}
	for _, i := range doc.Impacts {
		if i.Impact == "" || i.Breaks == "" {
			return nil, fmt.Errorf("%s: key %d needs both impact and breaks", path, i.Key)
		}
		if i.Scope != "" && (i.Scope != "broker-internal" || !IsBrokerInternalKey(i.Key)) {
			return nil, fmt.Errorf("%s: key %d cannot be excluded from application compatibility", path, i.Key)
		}
		m[i.Key] = i
	}
	return m, nil
}

func keyName(k int16) string {
	if n := kmsg.NameForKey(k); n != "" {
		return n
	}
	return fmt.Sprintf("Key%d", k)
}

func Build(ov, kv Versions, impacts map[int16]Impact) (Doc, []int16) {
	doc := Doc{Summary: map[string]int{}}
	var missing []int16
	keys := map[int16]bool{}
	for k := range ov.Keys {
		keys[k] = true
	}
	for k := range kv.Keys {
		keys[k] = true
	}
	sorted := make([]int16, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	for _, k := range sorted {
		o, inO := ov.Keys[k]
		q, inK := kv.Keys[k]
		row := KeyRow{ID: fmt.Sprintf("apikey.%d", k), Key: k, Name: keyName(k), Provenance: "kubemq-authored"}
		if inO {
			row.OracleMin, row.OracleMax = o[0], o[1]
		}
		if inK {
			mn, mx := q[0], q[1]
			row.KubeMQMin, row.KubeMQMax = &mn, &mx
		}
		switch {
		case inO && !inK && impacts[k].Scope == "broker-internal" && IsBrokerInternalKey(k):
			row.Status, row.Scope = "not-applicable", "broker-internal"
			row.Breaks, row.Impact = impacts[k].Breaks, impacts[k].Impact
		case inO && !inK:
			row.Status = "not-advertised"
		case !inO && inK:
			row.Status = "kubemq-only"
		case q[0] <= o[0] && q[1] >= o[1]:
			row.Status = "full"
		default:
			row.Status = "narrower"
		}
		if row.Status == "not-advertised" || row.Status == "narrower" {
			if im, ok := impacts[k]; ok {
				row.Breaks, row.Impact = im.Breaks, im.Impact
			} else {
				missing = append(missing, k)
			}
		}
		doc.Summary[row.Status]++
		doc.Keys = append(doc.Keys, row)
	}

	names := map[string]bool{}
	for n := range ov.Features {
		names[n] = true
	}
	for n := range kv.Features {
		names[n] = true
	}
	fnames := make([]string, 0, len(names))
	for n := range names {
		fnames = append(fnames, n)
	}
	sort.Strings(fnames)
	for _, n := range fnames {
		o, inO := ov.Features[n]
		q, inK := kv.Features[n]
		fr := FeatureRow{ID: "feature." + n, Feature: n, Provenance: "kubemq-authored",
			OracleSupported: rng(inO, o), OracleFinalized: fin(inO, o),
			KubeMQSupported: rng(inK, q), KubeMQFinalized: fin(inK, q)}
		switch {
		case inO && !inK:
			fr.Status = "not-advertised"
		case !inO && inK:
			fr.Status = "kubemq-only"
		case fr.OracleSupported == fr.KubeMQSupported && fr.OracleFinalized == fr.KubeMQFinalized:
			fr.Status = "same"
		default:
			fr.Status = "differs"
		}
		doc.Features = append(doc.Features, fr)
	}
	return doc, missing
}

func rng(ok bool, f Feature) string {
	if !ok {
		return "-"
	}
	return fmt.Sprintf("%d-%d", f.SupportedMin, f.SupportedMax)
}

func fin(ok bool, f Feature) string {
	if !ok || f.Finalized == nil {
		return "-"
	}
	return fmt.Sprint(*f.Finalized)
}

// Render encodes a Doc exactly as committed in matrix/apikeys.yaml.
func Render(doc Doc) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("# GENERATED by suites/protocol/cmd/apikeys-gen from the live ApiVersions of the Kafka oracle\n")
	buf.WriteString("# and KubeMQ. Do not edit. Impacts come from matrix/apikey-impacts.yaml.\n")
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Generate queries both brokers and builds the document. missing lists keys that differ from
// the oracle and have no curated impact.
func Generate(ctx context.Context, oracleAddr, kubemqAddr, impactsPath, oracleVersion, kubemqVersion string) (Doc, []int16, error) {
	ov, err := Query(ctx, oracleAddr)
	if err != nil {
		return Doc{}, nil, fmt.Errorf("oracle: %w", err)
	}
	kv, err := Query(ctx, kubemqAddr)
	if err != nil {
		return Doc{}, nil, fmt.Errorf("kubemq: %w", err)
	}
	return GenerateFromVersions(ov, kv, impactsPath, oracleVersion, kubemqVersion)
}

// GenerateFromVersions builds the matrix from caller-observed ApiVersions snapshots.
func GenerateFromVersions(ov, kv Versions, impactsPath, oracleVersion, kubemqVersion string) (Doc, []int16, error) {
	impacts, err := LoadImpacts(impactsPath)
	if err != nil {
		return Doc{}, nil, err
	}
	doc, missing := Build(ov, kv, impacts)
	doc.Generated = "by suites/protocol/cmd/apikeys-gen from live ApiVersions; do not edit"
	doc.Oracle = "apache/kafka " + oracleVersion
	doc.KubeMQ = "kubemq-next " + kubemqVersion
	return doc, missing, nil
}

// Load reads a committed matrix/apikeys.yaml.
func Load(path string) (Doc, error) {
	var d Doc
	b, err := os.ReadFile(path)
	if err != nil {
		return d, err
	}
	return d, yaml.Unmarshal(b, &d)
}

// KeyDeviation is the exact deviation text a gap registration for this key must carry. It
// names both version ranges, so a change on either broker invalidates the sign-off.
func KeyDeviation(k KeyRow) string {
	if k.KubeMQMin == nil {
		return fmt.Sprintf("ApiKey %d %s not advertised; Kafka advertises v%d-v%d", k.Key, k.Name, k.OracleMin, k.OracleMax)
	}
	return fmt.Sprintf("ApiKey %d %s advertised v%d-v%d; Kafka advertises v%d-v%d", k.Key, k.Name, *k.KubeMQMin, *k.KubeMQMax, k.OracleMin, k.OracleMax)
}

// FeatureDeviation is the exact deviation text a gap registration for this feature must carry.
func FeatureDeviation(f FeatureRow) string {
	if f.KubeMQSupported == "-" {
		return fmt.Sprintf("ApiVersions feature %s not advertised; Kafka supports %s, finalized %s", f.Feature, f.OracleSupported, f.OracleFinalized)
	}
	return fmt.Sprintf("ApiVersions feature %s: KubeMQ supports %s, finalized %s; Kafka supports %s, finalized %s", f.Feature, f.KubeMQSupported, f.KubeMQFinalized, f.OracleSupported, f.OracleFinalized)
}

// IsBrokerInternalKey restricts scope exclusions to the six owner-approved
// broker-only requests. Controller tools and client-facing requests stay in scope.
func IsBrokerInternalKey(key int16) bool {
	switch key {
	case 27, 83, 84, 85, 86, 87:
		return true
	default:
		return false
	}
}

// BrokerInternalDeviation binds an exclusion to the observed request and range.
func BrokerInternalDeviation(k KeyRow) string {
	return KeyDeviation(k) + "; broker-internal, not applicable to application replacement; Kafka broker interoperability remains unsupported"
}
