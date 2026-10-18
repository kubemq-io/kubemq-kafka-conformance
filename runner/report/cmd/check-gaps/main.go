// Command check-gaps fails when docs/gaps.md and the matrices disagree (see
// report.RegisterProblems), or when the matrices break a methodology rule that can be checked
// without a run (see report.MatrixProblems: provenance, track registration, quarantine and VOID
// note form). It needs no broker and runs in pr.yml.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/runner/report"
)

func main() {
	root := flag.String("root", ".", "repository root")
	flag.Parse()
	problems, err := report.RegisterProblems(filepath.Join(*root, "matrix"), filepath.Join(*root, "docs", "gaps.md"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "[check-gaps] ERROR:", err)
		os.Exit(2)
	}
	more, err := report.MatrixProblems(filepath.Join(*root, "matrix"), filepath.Join(*root, "runner", "tracks.txt"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "[check-gaps] ERROR:", err)
		os.Exit(2)
	}
	problems = append(problems, more...)
	for _, p := range problems {
		fmt.Fprintln(os.Stderr, "[check-gaps]", p)
	}
	if len(problems) > 0 {
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "[check-gaps] docs/gaps.md is consistent with the matrices; every matrix row has provenance and its track is registered")
}
