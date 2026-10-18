// Command apikeys-gen generates matrix/apikeys.yaml by asking the Kafka oracle and KubeMQ for
// their ApiVersions and diffing them key by key. The list of keys is never written by hand:
// it is whatever the oracle advertises.
//
// Every key the oracle advertises and KubeMQ does not, or advertises with a narrower version
// range, must have an entry in the curated impact table (matrix/apikey-impacts.yaml) naming the
// tool or client path that breaks. Generation fails when one is missing, so a new gap cannot
// enter the matrix without someone writing down what it costs a user.
//
//	apikeys-gen -oracle 127.0.0.1:28092 -kubemq 127.0.0.1:21092 \
//	  -impacts ../../matrix/apikey-impacts.yaml -out ../../matrix/apikeys.yaml \
//	  -oracle-version 4.3.1 -kubemq-version v3.6.15
//
// With -check, the file is regenerated in memory and compared with -out; any difference exits 1.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/apikeys"
)

const (
	apiVersionsSettleFor  = 5 * time.Second
	apiVersionsPollEvery  = time.Second
	apiVersionsQueryLimit = 60 * time.Second
)

type apiVersionsSettler struct {
	query     func(context.Context, string) (apikeys.Versions, error)
	now       func() time.Time
	wait      func(context.Context, time.Duration) error
	stableFor time.Duration
	pollEvery time.Duration
}

func settleApiVersions(ctx context.Context, oracle, kubemq string) (apikeys.Versions, apikeys.Versions, error) {
	return apiVersionsSettler{
		query:     apikeys.Query,
		now:       time.Now,
		wait:      waitForApiVersions,
		stableFor: apiVersionsSettleFor,
		pollEvery: apiVersionsPollEvery,
	}.settle(ctx, oracle, kubemq)
}

func (s apiVersionsSettler) settle(ctx context.Context, oracle, kubemq string) (apikeys.Versions, apikeys.Versions, error) {
	if s.query == nil || s.now == nil || s.wait == nil || s.stableFor <= 0 || s.pollEvery <= 0 {
		return apikeys.Versions{}, apikeys.Versions{}, fmt.Errorf("invalid ApiVersions settling configuration")
	}

	var previousOracle, previousKubeMQ apikeys.Versions
	var stableSince time.Time
	observed := false
	for {
		if err := ctx.Err(); err != nil {
			return apikeys.Versions{}, apikeys.Versions{}, fmt.Errorf("ApiVersions observations did not settle: %w", err)
		}
		oracleVersions, err := s.query(ctx, oracle)
		if err != nil {
			return apikeys.Versions{}, apikeys.Versions{}, fmt.Errorf("oracle ApiVersions observation: %w", err)
		}
		if err := validateApiVersions("oracle", oracleVersions); err != nil {
			return apikeys.Versions{}, apikeys.Versions{}, err
		}
		kubemqVersions, err := s.query(ctx, kubemq)
		if err != nil {
			return apikeys.Versions{}, apikeys.Versions{}, fmt.Errorf("KubeMQ ApiVersions observation: %w", err)
		}
		if err := validateApiVersions("KubeMQ", kubemqVersions); err != nil {
			return apikeys.Versions{}, apikeys.Versions{}, err
		}
		if err := ctx.Err(); err != nil {
			return apikeys.Versions{}, apikeys.Versions{}, fmt.Errorf("ApiVersions observations did not settle: %w", err)
		}

		now := s.now()
		if !observed || !reflect.DeepEqual(previousOracle, oracleVersions) || !reflect.DeepEqual(previousKubeMQ, kubemqVersions) {
			previousOracle = cloneApiVersions(oracleVersions)
			previousKubeMQ = cloneApiVersions(kubemqVersions)
			stableSince = now
			observed = true
		} else if now.Sub(stableSince) >= s.stableFor {
			if err := ctx.Err(); err != nil {
				return apikeys.Versions{}, apikeys.Versions{}, fmt.Errorf("ApiVersions observations did not settle: %w", err)
			}
			return previousOracle, previousKubeMQ, nil
		}

		if err := s.wait(ctx, s.pollEvery); err != nil {
			return apikeys.Versions{}, apikeys.Versions{}, fmt.Errorf("ApiVersions observations did not settle: %w", err)
		}
	}
}

func waitForApiVersions(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func validateApiVersions(name string, versions apikeys.Versions) error {
	if len(versions.Keys) == 0 {
		return fmt.Errorf("%s ApiVersions observation has no API keys", name)
	}
	for key, versions := range versions.Keys {
		if versions[0] > versions[1] {
			return fmt.Errorf("%s ApiVersions observation has invalid range for key %d", name, key)
		}
	}
	for feature, versions := range versions.Features {
		if versions.SupportedMin > versions.SupportedMax {
			return fmt.Errorf("%s ApiVersions observation has invalid supported range for feature %q", name, feature)
		}
		if versions.Finalized != nil && (*versions.Finalized < versions.SupportedMin || *versions.Finalized > versions.SupportedMax) {
			return fmt.Errorf("%s ApiVersions observation has invalid finalized level for feature %q", name, feature)
		}
	}
	return nil
}

func cloneApiVersions(versions apikeys.Versions) apikeys.Versions {
	clone := apikeys.Versions{
		Keys: make(map[int16][2]int16, len(versions.Keys)),
	}
	if versions.Features != nil {
		clone.Features = make(map[string]apikeys.Feature, len(versions.Features))
	}
	for key, rng := range versions.Keys {
		clone.Keys[key] = rng
	}
	for name, feature := range versions.Features {
		if feature.Finalized != nil {
			finalized := *feature.Finalized
			feature.Finalized = &finalized
		}
		clone.Features[name] = feature
	}
	return clone
}

func main() {
	oracle := flag.String("oracle", "", "Kafka oracle bootstrap address")
	kubemq := flag.String("kubemq", "", "KubeMQ bootstrap address")
	impactsPath := flag.String("impacts", "", "curated impact table (matrix/apikey-impacts.yaml)")
	outPath := flag.String("out", "", "matrix/apikeys.yaml")
	oracleVer := flag.String("oracle-version", "", "Kafka version, recorded in the file")
	kubemqVer := flag.String("kubemq-version", "", "KubeMQ version, recorded in the file")
	check := flag.Bool("check", false, "compare with -out instead of writing it")
	draft := flag.Bool("draft", false, "write keys without a curated impact as UNCURATED instead of failing (for curation only; never commit a draft)")
	registerFrom := flag.String("register-from", "", "print docs/gaps.md register lines (unsigned) for every differing key and feature of this committed matrix, then exit; needs no broker")
	flag.Parse()
	if *registerFrom != "" {
		printRegister(*registerFrom)
		return
	}
	if *oracle == "" || *kubemq == "" || *impactsPath == "" || *outPath == "" || *oracleVer == "" || *kubemqVer == "" {
		fatal("need -oracle, -kubemq, -impacts, -out, -oracle-version and -kubemq-version")
	}

	ctx, cancel := context.WithTimeout(context.Background(), apiVersionsQueryLimit)
	defer cancel()
	oracleVersions, kubemqVersions, err := settleApiVersions(ctx, *oracle, *kubemq)
	if err != nil {
		fatal(err.Error())
	}
	doc, missing, err := apikeys.GenerateFromVersions(oracleVersions, kubemqVersions, *impactsPath, *oracleVer, *kubemqVer)
	if err != nil {
		fatal(err.Error())
	}
	if len(missing) > 0 && *draft {
		for i := range doc.Keys {
			if doc.Keys[i].Status != "full" && doc.Keys[i].Status != "kubemq-only" && doc.Keys[i].Impact == "" {
				doc.Keys[i].Impact, doc.Keys[i].Breaks = "UNCURATED", "UNCURATED"
			}
		}
	} else if len(missing) > 0 {
		fatal(fmt.Sprintf("no curated impact in %s for keys %v: add what breaks for a user, then regenerate", *impactsPath, missing))
	}
	b, err := apikeys.Render(doc)
	if err != nil {
		fatal(err.Error())
	}
	if *check {
		cur, err := os.ReadFile(*outPath)
		if err != nil {
			fatal(err.Error())
		}
		if !bytes.Equal(cur, b) {
			fmt.Fprintf(os.Stderr, "[apikeys] %s is stale: the brokers' ApiVersions no longer match it; regenerate\n", *outPath)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "[apikeys] %s matches the live brokers\n", *outPath)
		return
	}
	if err := os.WriteFile(*outPath, b, 0o644); err != nil {
		fatal(err.Error())
	}
	fmt.Fprintf(os.Stderr, "[apikeys] wrote %s: %v\n", *outPath, doc.Summary)
}

// printRegister prints one unsigned register line per key or feature that differs from the
// oracle, with the exact deviation text the protocol track checks against the sign-off.
func printRegister(path string) {
	doc, err := apikeys.Load(path)
	if err != nil {
		fatal(err.Error())
	}
	esc := func(s string) string { return strings.ReplaceAll(s, "|", "\\|") }
	for _, k := range doc.Keys {
		if k.Status == "full" || k.Status == "kubemq-only" {
			continue
		}
		road, work := roadmap(k.Key)
		fmt.Printf("| `%s` | %s | %s %s | %s | %s | - |\n", k.ID, esc(apikeys.KeyDeviation(k)), esc(k.Breaks)+".", esc(k.Impact), work, road)
	}
	for _, f := range doc.Features {
		if f.Status == "same" || f.Status == "kubemq-only" {
			continue
		}
		fmt.Printf("| `%s` | %s | %s | None needed for clients; they use the behaviour of the oldest feature level. | No version committed. | - |\n",
			f.ID, esc(apikeys.FeatureDeviation(f)), esc(featureImpact(f.Feature)))
	}
}

// roadmap is the roadmap and workaround text per key. Only the KIP-848 keys have a plan;
// every other gap reads "No version committed".
func roadmap(key int16) (string, string) {
	switch key {
	case 68, 69:
		return "Planned, no version (KIP-848).", "Set group.protocol=classic."
	case 27, 83, 84, 85, 86, 87:
		return "No version committed.", "None needed: broker-internal request."
	case 33:
		return "No version committed.", "Use IncrementalAlterConfigs (kafka-configs.sh, current Admin clients)."
	case 0, 1, 2, 8, 9, 10, 11, 13, 14, 18, 22, 24, 25, 26, 28:
		return "No version committed.", "None needed for clients that negotiate versions (all current clients do)."
	}
	return "No version committed.", "None from Kafka tooling."
}

// featureImpact names who reads a feature flag and which plan rows measure the consequence.
func featureImpact(name string) string {
	const java = "Kafka 4.x Java clients negotiate transaction.version and group.version from these ApiVersions feature flags; with none advertised they fall back to the oldest behaviour."
	switch name {
	case "transaction.version":
		return java + " Producers therefore use transaction version 1; the KIP-890 transaction.version=2 rows (Track B, transactions) measure the consequence."
	case "group.version":
		return java + " Consumers therefore cannot use the KIP-848 group protocol; the KIP-848 rows (Track B, consumer groups) measure the consequence."
	}
	return java + " kafka-features.sh describe and other tools that read this flag see nothing for KubeMQ. The KIP-890 transaction.version=2 and KIP-848 rows measure the client consequence."
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "[apikeys] ERROR:", msg)
	os.Exit(2)
}
