// Command txn-driver is the franz-go transaction driver. It runs one scenario against one
// broker, inside a client container, and prints a verify.Report as the last line of its
// standard output. Exit 1 means the scenario could not complete (the reason is on stderr).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/transactions/franz"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/transactions/verify"
)

func main() {
	bootstrap := flag.String("bootstrap", "", "broker bootstrap address")
	scenario := flag.String("scenario", "", "scenario name (SCENARIOS.md)")
	target := flag.String("target", "broker", "real-kafka or kubemq, for topic names only")
	deadline := flag.Duration("deadline", 3*time.Minute, "bound on the whole scenario")
	flag.Parse()
	s := franz.Scenarios[*scenario]
	if *bootstrap == "" || s == nil {
		fmt.Fprintf(os.Stderr, "txn-driver: need -bootstrap and a known -scenario (got %q)\n", *scenario)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *deadline)
	defer cancel()
	facts, err := s(ctx, franz.Env{Bootstrap: *bootstrap, Target: *target})
	if err != nil {
		fmt.Fprintln(os.Stderr, "txn-driver:", err)
		os.Exit(1)
	}
	b, err := json.Marshal(facts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "txn-driver:", err)
		os.Exit(1)
	}
	out, _ := json.Marshal(verify.Report{Client: franz.Client(), Scenario: *scenario, Facts: b})
	fmt.Println(string(out))
}
