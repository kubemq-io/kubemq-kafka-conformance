package report

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// RegisterProblems checks docs/gaps.md against the matrices, with no broker:
//   - every register entry names a row some matrix owns (no orphan registrations);
//   - every entry's sign-off reads "<who> <YYYY-MM-DD>" or "-" (a draft);
//   - no row is registered twice.
//
// It returns one line per problem; none means the register is consistent.
func RegisterProblems(matrixDir, gapsPath string) ([]string, error) {
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
	f, err := os.Open(gapsPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var problems []string
	seen := map[string]bool{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "|") || !strings.HasSuffix(line, "|") {
			continue
		}
		cells := splitTableCells(line)
		if len(cells) < 3 {
			continue
		}
		id := strings.Trim(strings.TrimSpace(cells[0]), "`")
		if !gapRowID.MatchString(id) {
			continue // header, separator, or prose table
		}
		signed := strings.TrimSpace(cells[len(cells)-1])
		if !owned[id] {
			problems = append(problems, fmt.Sprintf("docs/gaps.md line %d: %s is not a row of any matrix", n, id))
		}
		if signed != "-" && !gapSignOff.MatchString(signed) {
			problems = append(problems, fmt.Sprintf("docs/gaps.md line %d: %s sign-off %q is neither \"<who> <YYYY-MM-DD>\" nor \"-\"", n, id, signed))
		}
		if seen[id] {
			problems = append(problems, fmt.Sprintf("docs/gaps.md line %d: %s is registered twice", n, id))
		}
		seen[id] = true
	}
	return problems, sc.Err()
}

// splitTableCells splits a Markdown table line on unescaped pipes, unescaping "\|", exactly as
// suites/protocol/gaps does, so the checker and the runner read the same cells.
func splitTableCells(line string) []string {
	line = strings.TrimSuffix(strings.TrimPrefix(line, "|"), "|")
	var cells []string
	var cur strings.Builder
	for i := 0; i < len(line); i++ {
		if line[i] == '\\' && i+1 < len(line) && line[i+1] == '|' {
			cur.WriteByte('|')
			i++
			continue
		}
		if line[i] == '|' {
			cells = append(cells, strings.TrimSpace(cur.String()))
			cur.Reset()
			continue
		}
		cur.WriteByte(line[i])
	}
	return append(cells, strings.TrimSpace(cur.String()))
}
