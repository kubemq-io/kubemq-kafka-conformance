package report

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// MatrixProblems checks the committed matrices and their side files, with no broker:
//   - every matrix row records its provenance: "upstream" with a source naming the upstream
//     test, or "kubemq-authored";
//   - every matrix track is registered in runner/tracks.txt (otherwise no run would cover it);
//   - every matrix/quarantine.yaml entry names a matrix row and has a reason and a
//     "<who> <YYYY-MM-DD>" sign-off;
//   - every matrix/void-notes.yaml entry names a matrix row, a KubeMQ version, a note, and the
//     oracle log linked by digest ("<link>@sha256:<64 hex>").
//
// It returns one line per problem; none means the matrices are consistent.
func MatrixProblems(matrixDir, tracksFile string) ([]string, error) {
	var problems []string
	expected, err := ExpectedRows(matrixDir)
	if err != nil {
		return nil, err
	}
	owned := map[string]bool{}
	for _, ids := range expected {
		for _, id := range ids {
			owned[id] = true
		}
	}

	registered, err := registeredTracks(tracksFile)
	if err != nil {
		return nil, err
	}
	for _, t := range sortedTracks(expected) {
		if !contains(registered, t) {
			problems = append(problems, fmt.Sprintf("matrix track %q is not registered in runner/tracks.txt, so no run would cover its rows", t))
		}
	}

	files, err := filepath.Glob(filepath.Join(matrixDir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	for _, f := range files {
		base := filepath.Base(f)
		if base == "apikey-impacts.yaml" || base == "quarantine.yaml" || base == "void-notes.yaml" {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		type row struct {
			ID         string `yaml:"id"`
			Provenance string `yaml:"provenance"`
			Source     string `yaml:"source"`
		}
		var doc struct {
			Rows     []row `yaml:"rows"`
			Keys     []row `yaml:"keys"`
			Features []row `yaml:"features"`
		}
		if err := yaml.Unmarshal(b, &doc); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		for _, list := range [][]row{doc.Rows, doc.Keys, doc.Features} {
			for _, r := range list {
				switch {
				case r.Provenance == "kubemq-authored":
				case r.Provenance == "upstream" && strings.TrimSpace(r.Source) != "":
				case r.Provenance == "upstream":
					problems = append(problems, fmt.Sprintf("matrix/%s: %s is upstream but its source does not name the upstream test", base, r.ID))
				default:
					problems = append(problems, fmt.Sprintf("matrix/%s: %s has provenance %q; use upstream (with source) or kubemq-authored", base, r.ID, r.Provenance))
				}
			}
		}
	}

	if b, err := os.ReadFile(filepath.Join(matrixDir, "quarantine.yaml")); err == nil {
		var doc struct {
			Quarantine []QuarantineEntry `yaml:"quarantine"`
		}
		if err := yaml.Unmarshal(b, &doc); err != nil {
			return nil, fmt.Errorf("matrix/quarantine.yaml: %w", err)
		}
		for _, q := range doc.Quarantine {
			if !owned[q.Row] {
				problems = append(problems, fmt.Sprintf("matrix/quarantine.yaml: %s is not a row of any matrix", q.Row))
			}
			if strings.TrimSpace(q.Reason) == "" || !gapSignOff.MatchString(strings.TrimSpace(q.SignedOff)) {
				problems = append(problems, fmt.Sprintf("matrix/quarantine.yaml: %s needs a reason and a \"<who> <YYYY-MM-DD>\" sign-off; until then it quarantines nothing", q.Row))
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	if b, err := os.ReadFile(filepath.Join(matrixDir, "void-notes.yaml")); err == nil {
		var doc struct {
			Notes []VoidNote `yaml:"notes"`
		}
		if err := yaml.Unmarshal(b, &doc); err != nil {
			return nil, fmt.Errorf("matrix/void-notes.yaml: %w", err)
		}
		for _, n := range doc.Notes {
			if !owned[n.Row] {
				problems = append(problems, fmt.Sprintf("matrix/void-notes.yaml: %s is not a row of any matrix", n.Row))
			}
			if n.KubeMQVersion == "" || strings.TrimSpace(n.Note) == "" || !oracleLogRef.MatchString(n.OracleLog) {
				problems = append(problems, fmt.Sprintf("matrix/void-notes.yaml: %s needs kubemq_version, note, and oracle_log as <link>@sha256:<64 hex>; until then it explains nothing", n.Row))
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return problems, nil
}
