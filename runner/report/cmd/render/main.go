// Command render builds the report for one run.
//
//	render -run-dir .work/run-X -root .            # writes into the run directory only
//	render -run-dir .work/run-X -root . -publish   # also writes COMPATIBILITY.md and reports/<version>/
//	render -run-dir .work/run-X -root . -publish -public   # the public repository's report, in results/
//
// A Kafka release-candidate preview (run.json kafka_preview) publishes to
// reports/preview/kafka-<rc>-kubemq-<version>/ and never writes COMPATIBILITY.md.
//
// Exit codes: 0 report passed, 1 report failed, 2 the inputs are malformed.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/runner/report"
)

func main() {
	runDir := flag.String("run-dir", "", "run directory written by runner/certify.sh")
	root := flag.String("root", ".", "repository root")
	publish := flag.Bool("publish", false, "write COMPATIBILITY.md and reports/<kubemq-version>/ in the repository")
	flag.BoolVar(&report.Public, "public", false, "public-repository report: KubeMQ without version or image, no operator, command or commit lines; publishes to results/")
	flag.Parse()
	if *runDir == "" {
		fatal("need -run-dir")
	}
	rep, err := report.Build(report.Inputs{
		RunDir:     *runDir,
		MatrixDir:  filepath.Join(*root, "matrix"),
		GapsPath:   filepath.Join(*root, "docs", "gaps.md"),
		Quarantine: filepath.Join(*root, "matrix", "quarantine.yaml"),
		VoidNotes:  filepath.Join(*root, "matrix", "void-notes.yaml"),
		ReportsDir: filepath.Join(*root, "reports"),
		TracksFile: filepath.Join(*root, "runner", "tracks.txt"),
		AllTracks:  *publish,
	})
	if err != nil {
		fatal(err.Error())
	}
	reportMD := report.ReportMarkdown(rep)
	compatMD := report.CompatibilityMarkdown(rep)
	summary, err := report.SummaryJSON(rep)
	if err != nil {
		fatal(err.Error())
	}
	preview := rep.Run.KafkaPreview != ""
	outDirs := []string{filepath.Join(*runDir, "report")}
	if *publish {
		outDirs = append(outDirs, filepath.Join(*root, report.PublishDir(rep)))
	}
	for _, d := range outDirs {
		write(filepath.Join(d, "REPORT.md"), []byte(reportMD))
		write(filepath.Join(d, "summary.json"), summary)
	}
	// A preview against a Kafka release candidate never writes COMPATIBILITY.md, not even the
	// run directory's copy, so nobody mistakes it for the certified page.
	if !preview {
		write(filepath.Join(*runDir, "report", "COMPATIBILITY.md"), []byte(compatMD))
		if *publish {
			write(filepath.Join(*root, "COMPATIBILITY.md"), []byte(compatMD))
		}
	}

	for _, t := range rep.Tracks {
		fmt.Fprintf(os.Stderr, "[report] %-10s rows %-4d PASS %d  GAP %d  FAIL %d  VOID %d  NOT-RUN %d  FLAKY %d\n",
			t.Track, t.Rows, t.Counts[report.Pass], t.Counts[report.Gap], t.Counts[report.Fail], t.Counts[report.Void], t.Counts[report.NotRun], t.Counts[report.Flaky])
	}
	fmt.Fprintf(os.Stderr, "[report] written to %s\n", outDirs[len(outDirs)-1])
	if !rep.Passed {
		for _, r := range rep.Reasons {
			fmt.Fprintf(os.Stderr, "[report] FAILED: %s\n", r)
		}
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "[report] PASSED")
}

func write(path string, b []byte) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		fatal(err.Error())
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		fatal(err.Error())
	}
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "[report] ERROR:", msg)
	os.Exit(2)
}
