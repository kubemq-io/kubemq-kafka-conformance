package report

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegisterProblems(t *testing.T) {
	root := t.TempDir()
	must(t, os.MkdirAll(filepath.Join(root, "matrix"), 0o755))
	must(t, os.MkdirAll(filepath.Join(root, "docs"), 0o755))
	must(t, os.WriteFile(filepath.Join(root, "matrix", "f.yaml"), []byte("track: f\nrows:\n  - id: a\n  - id: b\n"), 0o644))
	write := func(body string) []string {
		must(t, os.WriteFile(filepath.Join(root, "docs", "gaps.md"), []byte("| Row | Deviation | Signed off |\n|---|---|---|\n"+body), 0o644))
		p, err := RegisterProblems(filepath.Join(root, "matrix"), filepath.Join(root, "docs", "gaps.md"))
		must(t, err)
		return p
	}
	if p := write("| `a` | x | reviewer 2026-10-06 |\n| `b` | y | - |\n"); len(p) != 0 {
		t.Fatalf("control: consistent register reported %v", p)
	}
	if p := write("| `a` | deviation with \\| a pipe | reviewer 2026-10-06 |\n"); len(p) != 0 {
		t.Fatalf("an escaped pipe in the deviation must not shift the sign-off cell: %v", p)
	}
	cases := map[string]string{
		"orphan":    "| `z` | x | reviewer 2026-10-06 |\n",
		"bad sign":  "| `a` | x | pending |\n",
		"duplicate": "| `a` | x | reviewer 2026-10-06 |\n| `a` | x | reviewer 2026-10-06 |\n",
	}
	want := map[string]string{"orphan": "not a row of any matrix", "bad sign": "neither", "duplicate": "registered twice"}
	for name, body := range cases {
		p := write(body)
		if len(p) == 0 || !strings.Contains(strings.Join(p, "\n"), want[name]) {
			t.Errorf("%s: got %v", name, p)
		}
	}
}

func TestMatrixProblems(t *testing.T) {
	root := t.TempDir()
	m := filepath.Join(root, "matrix")
	must(t, os.MkdirAll(m, 0o755))
	write := func(name, content string) { must(t, os.WriteFile(filepath.Join(m, name), []byte(content), 0o644)) }
	tracks := filepath.Join(root, "tracks.txt")
	must(t, os.WriteFile(tracks, []byte("protocol quick\n"), 0o644))
	write("protocol.yaml", "track: protocol\nrows:\n  - {id: a, provenance: kubemq-authored}\n  - {id: b, provenance: upstream, source: ducktape test}\n")
	problems, err := MatrixProblems(m, tracks)
	must(t, err)
	if len(problems) != 0 {
		t.Fatalf("clean matrices reported problems: %v", problems)
	}

	write("protocol.yaml", "track: protocol\nrows:\n  - {id: a}\n  - {id: b, provenance: upstream}\n")
	write("operations.yaml", "track: operations\nrows:\n  - {id: c, provenance: kubemq-authored}\n")
	write("quarantine.yaml", "quarantine:\n  - {row: a, reason: timing}\n  - {row: zz, reason: timing, signed_off: reviewer 2026-10-05}\n")
	write("void-notes.yaml", "notes:\n  - {row: a, kubemq_version: v1, oracle_log: https://x/log, note: y}\n")
	problems, err = MatrixProblems(m, tracks)
	must(t, err)
	joined := strings.Join(problems, "\n")
	for _, want := range []string{
		`a has provenance ""`,
		"b is upstream but its source does not name the upstream test",
		`matrix track "operations" is not registered`,
		"quarantine.yaml: a needs a reason and a",
		"quarantine.yaml: zz is not a row of any matrix",
		"void-notes.yaml: a needs kubemq_version, note, and oracle_log",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing problem %q in:\n%s", want, joined)
		}
	}
}
