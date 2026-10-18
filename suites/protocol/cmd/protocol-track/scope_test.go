package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/apikeys"
)

func TestBrokerInternalClassificationRequiresPriorExactApproval(t *testing.T) {
	key := apikeys.KeyRow{ID: "apikey.27", Key: 27, Name: "WriteTxnMarkers", OracleMin: 1, OracleMax: 2, Status: "not-applicable", Scope: "broker-internal", Provenance: "kubemq-authored"}
	dev := apikeys.BrokerInternalDeviation(key)
	for _, tc := range []struct {
		name, approved, signed, verdict string
	}{
		{"approved", dev, "owner 2026-10-13T07:00Z", "NOT-APPLICABLE"},
		{"unsigned", dev, "pending", "FAIL"},
		{"late", dev, "owner 2026-10-13T09:00Z", "FAIL"},
		{"changed range", dev + " changed", "owner 2026-10-13T07:00Z", "FAIL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := apikeys.Doc{Keys: []apikeys.KeyRow{key}}
			b, _ := apikeys.Render(doc)
			reg := register(t, fmt.Sprintf("| apikey.27 | %s | %s |", tc.approved, tc.signed))
			rows := classifyAPIKeys(doc, nil, b, b, nil, reg, time.Date(2026, 10, 13, 8, 0, 0, 0, time.UTC))
			if got := verdicts(rows)[key.ID]; got != tc.verdict {
				t.Fatalf("got %s, want %s", got, tc.verdict)
			}
		})
	}
	// Even a matching owner sign-off cannot turn a client-facing request into
	// a broker-internal scope exclusion.
	key.ID, key.Key, key.Name = "apikey.1", 1, "Fetch"
	dev = apikeys.BrokerInternalDeviation(key)
	doc := apikeys.Doc{Keys: []apikeys.KeyRow{key}}
	b, _ := apikeys.Render(doc)
	reg := register(t, fmt.Sprintf("| apikey.1 | %s | owner 2026-10-13T07:00Z |", dev))
	if got := verdicts(classifyAPIKeys(doc, nil, b, b, nil, reg, time.Date(2026, 10, 13, 8, 0, 0, 0, time.UTC)))[key.ID]; got != "FAIL" {
		t.Fatalf("client request hidden as %s", got)
	}
}
