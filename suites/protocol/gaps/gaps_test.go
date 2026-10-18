package gaps

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const register = "# Gaps\n\n| Row | Deviation | Impact | Signed off |\n|---|---|---|---|\n" +
	"| `apikey.43` | ElectLeaders not advertised | x | reviewer 2026-10-05 |\n" +
	"| apikey.45 | not signed yet | x |  |\n" +
	"| apikey.46 | dash means unsigned | x | - |\n" +
	"| apikey.47 | pending is unsigned | x | pending |\n" +
	"| apikey.48 | pipe \\| inside | x | reviewer one 2026-10-01 |\n" +
	"| Row With Spaces | ignored | x | someone 2026-10-01 |\n"

func load(t *testing.T) map[string]Entry {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gaps.md")
	if err := os.WriteFile(p, []byte(register), 0o644); err != nil {
		t.Fatal(err)
	}
	reg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestRegistered(t *testing.T) {
	reg := load(t)
	day := func(s string) time.Time { d, _ := time.Parse("2006-01-02", s); return d }
	cases := []struct {
		id, observed, run string
		want              bool
		reason            string
	}{
		{"apikey.43", "ElectLeaders not advertised", "2026-10-05", true, "signed off by reviewer"},
		{"apikey.43", "ElectLeaders not advertised", "2026-10-04", false, "after this run"},
		{"apikey.43", "ElectLeaders advertised v0-v1", "2026-10-06", false, "differs"},
		{"apikey.45", "not signed yet", "2026-10-06", false, "not signed off"},
		{"apikey.46", "dash means unsigned", "2026-10-06", false, "not signed off"},
		{"apikey.47", "pending is unsigned", "2026-10-06", false, "not signed off"},
		{"apikey.48", "pipe | inside", "2026-10-06", true, "reviewer one"},
		{"apikey.99", "anything", "2026-10-06", false, "not pre-registered"},
	}
	for _, c := range cases {
		got, reason := Registered(reg, c.id, c.observed, day(c.run))
		if got != c.want || !strings.Contains(reason, c.reason) {
			t.Errorf("%s on %s: got %v %q, want %v containing %q", c.id, c.run, got, reason, c.want, c.reason)
		}
	}
	if _, ok := reg["Row With Spaces"]; ok {
		t.Error("an invalid row id must be ignored")
	}
}

func TestMissingRegisterIsEmpty(t *testing.T) {
	reg, err := Load(filepath.Join(t.TempDir(), "missing.md"))
	if err != nil || len(reg) != 0 {
		t.Fatalf("got %v, %v", reg, err)
	}
}

func TestSignOffTime(t *testing.T) {
	p := filepath.Join(t.TempDir(), "gaps.md")
	reg := "| Row | Deviation | Signed off |\n|---|---|---|\n" +
		"| `a.grandfathered` | d | reviewer 2026-10-07 |\n" +
		"| `a.dateonly` | d | reviewer 2026-10-08 |\n" +
		"| `a.timed` | d | reviewer 2026-10-08T12:30Z |\n" +
		"| `a.seconds` | d | reviewer 2026-10-08T12:30:15Z |\n" +
		"| `a.badtime` | d | reviewer 2026-10-08T25:99Z |\n"
	if err := os.WriteFile(p, []byte(reg), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	at := func(s string) time.Time { v, _ := time.Parse(time.RFC3339, s); return v }
	cases := []struct {
		id, run string
		want    bool
	}{
		{"a.grandfathered", "2026-10-07T15:06:00Z", true}, // date-only through 2026-10-07: start of day
		{"a.dateonly", "2026-10-08T23:00:00Z", false},     // later date-only: end of day
		{"a.dateonly", "2026-10-09T00:00:00Z", true},
		{"a.timed", "2026-10-08T12:00:00Z", false}, // signed after the run started
		{"a.timed", "2026-10-08T12:30:00Z", true},  // signed at the start: before is not after
		{"a.seconds", "2026-10-08T12:30:10Z", false},
		{"a.seconds", "2026-10-08T12:31:00Z", true},
		{"a.badtime", "2026-10-09T00:00:00Z", false}, // an unreadable time is not signed
	}
	for _, c := range cases {
		if got, why := Registered(r, c.id, "d", at(c.run)); got != c.want {
			t.Errorf("%s for a run at %s: got %v (%s), want %v", c.id, c.run, got, why, c.want)
		}
	}
}
