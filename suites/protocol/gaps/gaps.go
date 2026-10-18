// Package gaps reads the pre-registered gap register in docs/gaps.md.
//
// The register is a Markdown table: first cell the row id, second cell the exact deviation that
// was signed off, last cell the sign-off as "<who> <YYYY-MM-DD>". A row counts as registered
// for a run only when:
//   - the sign-off cell has that form (anything else, "-", "pending", is unsigned);
//   - the sign-off date is not after the run date (a gap is registered before the run);
//   - the caller confirms the deviation observed in the run is exactly the one signed off
//     (Matches). A deviation that changed since sign-off needs a new sign-off.
package gaps

import (
	"bufio"
	"os"
	"regexp"
	"strings"
	"time"
)

var (
	rowID = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)
	// "<who> <YYYY-MM-DD>" or "<who> <YYYY-MM-DDTHH:MMZ>" (seconds optional), UTC.
	signOff = regexp.MustCompile(`^(\S.*\S)\s+(\d{4}-\d{2}-\d{2})(T\d{2}:\d{2}(:\d{2})?Z)?$`)
)

// Entry is one line of the register.
type Entry struct {
	Row       string
	Deviation string
	SignedBy  string
	// Signed is when the gap was signed off: a sign-off with a UTC time is that instant. A
	// date-only sign-off dated after DateOnlyGrandfatheredThrough counts as 23:59:59Z of its day,
	// so it binds only to runs starting on a later day; one dated on or before it counts as
	// 00:00:00Z of its day (see DateOnlyGrandfatheredThrough).
	Signed time.Time
	Valid  bool   // the sign-off cell has one of the two forms above
	Raw    string // the sign-off cell as written
}

// Load parses the register. A missing file is an empty register, not an error.
func Load(path string) (map[string]Entry, error) {
	out := map[string]Entry{}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "|") || !strings.HasSuffix(line, "|") {
			continue
		}
		cells := splitCells(line)
		if len(cells) < 3 {
			continue
		}
		id := strings.Trim(cells[0], "`")
		if !rowID.MatchString(id) {
			continue
		}
		e := Entry{Row: id, Deviation: cells[1], Raw: cells[len(cells)-1]}
		if m := signOff.FindStringSubmatch(e.Raw); m != nil {
			if t, ok := signedAt(m[2], m[3]); ok {
				e.SignedBy, e.Signed, e.Valid = m[1], t, true
			}
		}
		out[id] = e
	}
	return out, sc.Err()
}

// DateOnlyGrandfatheredThrough: date-only sign-offs dated on or before this day were all given
// before the row first ran, except security.delegation-tokens
// (signed 2026-10-07 after its 15:06 UTC run, handled by a fresh run). They count from 00:00:00Z
// of their day. Later sign-offs carry a UTC time; a later date-only one counts from 23:59:59Z.
// The same rule applies in runner/report.
const DateOnlyGrandfatheredThrough = "2026-10-07"

// signedAt is the instant of a sign-off: its date plus UTC time; for a date alone, 00:00:00Z
// through DateOnlyGrandfatheredThrough and 23:59:59Z after it.
func signedAt(date, clock string) (time.Time, bool) {
	if clock == "" {
		t, err := time.Parse("2006-01-02", date)
		if err != nil {
			return time.Time{}, false
		}
		if date <= DateOnlyGrandfatheredThrough {
			return t, true
		}
		return t.Add(24*time.Hour - time.Second), true
	}
	for _, layout := range []string{"2006-01-02T15:04Z", "2006-01-02T15:04:05Z"} {
		if t, err := time.Parse(layout, date+clock); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// splitCells splits a Markdown table line on unescaped pipes and unescapes "\|".
func splitCells(line string) []string {
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

// Registered reports whether row id may be reported GAP in a run on runDate whose observed
// deviation is observed. When it may not, reason says why.
func Registered(reg map[string]Entry, id, observed string, runDate time.Time) (bool, string) {
	e, ok := reg[id]
	switch {
	case !ok:
		return false, "not pre-registered in docs/gaps.md"
	case !e.Valid:
		return false, "registered in docs/gaps.md but not signed off (sign-off must read \"<who> <YYYY-MM-DDTHH:MMZ>\" or \"<who> <YYYY-MM-DD>\")"
	case e.Signed.After(runDate):
		return false, "signed off at " + e.Signed.UTC().Format(time.RFC3339) + ", after this run started at " + runDate.UTC().Format(time.RFC3339) + " (a date-only sign-off counts as the end of its day): a gap must be registered before the run"
	case strings.TrimSpace(e.Deviation) != strings.TrimSpace(observed):
		return false, "the observed deviation differs from the one signed off in docs/gaps.md (signed: \"" + e.Deviation + "\"; observed: \"" + observed + "\"); it needs a new sign-off"
	}
	return true, "pre-registered gap, signed off by " + e.Raw
}
