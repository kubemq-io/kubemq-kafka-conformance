// Command merge builds one release report from the run directories of one KubeMQ version run on
// several machines (each track on one machine).
//
//	merge -root . -out reports/v3.6.17 [-allow-mixed-commits] <run-dir>...
//
// Writes REPORT.md and summary.json to -out and COMPATIBILITY.md at the repository root (never
// for a Kafka release-candidate preview). Every registered track is covered: a track no run
// directory ran is NOT-RUN. Exit codes: 0 report passed, 1 report failed, 2 refused or malformed.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/runner/report"
)

func main() {
	root := flag.String("root", ".", "repository root")
	out := flag.String("out", "", "where REPORT.md and summary.json go, for example reports/v3.6.17")
	flag.BoolVar(&report.Public, "public", false, "public-repository report: KubeMQ without version or image, no operator, command or commit lines; -out must be results")
	allowMixed := flag.Bool("allow-mixed-commits", false, "merge runs made from different kit commits; the header says so")
	superseded := map[string][]string{}
	notRun := map[string]string{}
	final := flag.String("final", "", "final release report: every run must use this kit commit, clean, for a released KubeMQ; refuses --allow-mixed-commits, --superseded and --not-run-reason")
	flag.Func("not-run-reason", "<track>=<reason>: why a track no run covered was not run, shown on its rows (repeatable)", func(v string) error {
		i := strings.Index(v, "=")
		if i <= 0 {
			return fmt.Errorf("want <track>=<reason>, got %q", v)
		}
		notRun[v[:i]] = v[i+1:]
		return nil
	})
	flag.Func("superseded", "<run-dir>:<track>: do not use that track's results from that run; a later run in the merge replaces them (repeatable)", func(v string) error {
		i := strings.LastIndex(v, ":")
		if i <= 0 || i == len(v)-1 {
			return fmt.Errorf("want <run-dir>:<track>, got %q", v)
		}
		d := filepath.Clean(v[:i])
		superseded[d] = append(superseded[d], v[i+1:])
		return nil
	})
	flag.Parse()
	dirs := flag.Args()
	if *out == "" || len(dirs) == 0 {
		fatal("usage: merge -root <repo> -out reports/<version> [-allow-mixed-commits] <run-dir>...")
	}
	if *final != "" && (*allowMixed || len(superseded) > 0 || len(notRun) > 0) {
		fatal("--final is single-commit by construction: it refuses --allow-mixed-commits, --superseded and --not-run-reason")
	}
	tmp, err := os.MkdirTemp("", "kcert-merge-")
	if err != nil {
		fatal(err.Error())
	}
	defer os.RemoveAll(tmp)
	for i := range dirs {
		dirs[i] = filepath.Clean(dirs[i])
	}
	if _, err := report.MergeRuns(dirs, tmp, *allowMixed, superseded); err != nil {
		fatal(err.Error())
	}
	if err := report.SetNotRunReasons(tmp, notRun); err != nil {
		fatal(err.Error())
	}
	if *final != "" {
		if err := report.CheckFinal(tmp, *final); err != nil {
			fatal(err.Error())
		}
	}
	rep, err := report.Build(report.Inputs{
		RunDir:     tmp,
		MatrixDir:  filepath.Join(*root, "matrix"),
		GapsPath:   filepath.Join(*root, "docs", "gaps.md"),
		Quarantine: filepath.Join(*root, "matrix", "quarantine.yaml"),
		VoidNotes:  filepath.Join(*root, "matrix", "void-notes.yaml"),
		ReportsDir: filepath.Join(*root, "reports"),
		TracksFile: filepath.Join(*root, "runner", "tracks.txt"),
		AllTracks:  true,
	})
	if err != nil {
		fatal(err.Error())
	}
	outDir := *out
	if !filepath.IsAbs(outDir) {
		outDir = filepath.Join(*root, outDir)
	}
	if want := filepath.Base(report.PublishDir(rep)); filepath.Base(outDir) != want {
		fatal(fmt.Sprintf("-out %s does not match this report; it belongs in %s", *out, report.PublishDir(rep)))
	}
	summary, err := report.SummaryJSON(rep)
	if err != nil {
		fatal(err.Error())
	}
	write(filepath.Join(outDir, "REPORT.md"), []byte(report.ReportMarkdown(rep)))
	write(filepath.Join(outDir, "summary.json"), summary)
	if rep.Run.KafkaPreview == "" {
		write(filepath.Join(*root, "COMPATIBILITY.md"), []byte(report.CompatibilityMarkdown(rep)))
	}
	for _, t := range rep.Tracks {
		fmt.Fprintf(os.Stderr, "[merge] %-14s rows %-4d PASS %d  GAP %d  FAIL %d  VOID %d  NOT-RUN %d  FLAKY %d\n",
			t.Track, t.Rows, t.Counts[report.Pass], t.Counts[report.Gap], t.Counts[report.Fail], t.Counts[report.Void], t.Counts[report.NotRun], t.Counts[report.Flaky])
	}
	fmt.Fprintf(os.Stderr, "[merge] %d runs merged; written to %s\n", len(rep.Run.Sources), outDir)
	if !rep.Passed {
		for _, r := range rep.Reasons {
			fmt.Fprintf(os.Stderr, "[merge] FAILED: %s\n", r)
		}
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "[merge] PASSED")
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
	fmt.Fprintln(os.Stderr, "[merge] ERROR:", msg)
	os.Exit(2)
}
