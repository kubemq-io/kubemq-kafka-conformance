// Package golib is what every Go client driver shares: the scenario inputs and the ledger writer
// (SCENARIO.md). It never touches a Kafka client, so each driver uses only the library under test.
package golib

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

// Inputs are the scenario's environment variables.
type Inputs struct {
	Bootstrap  string
	Mode       string
	Topic      string
	Group      string
	Partitions int32
	Records    int
	Records2   int
	Keys       int
	Timeout    time.Duration
	Admin      bool
}

// ReadInputs reads the KCERT_* variables; a missing one stops the driver with an error event.
func ReadInputs(l *Ledger) Inputs {
	in := Inputs{
		Bootstrap: os.Getenv("KCERT_BOOTSTRAP"), Mode: os.Getenv("KCERT_MODE"),
		Topic: os.Getenv("KCERT_TOPIC"), Group: os.Getenv("KCERT_GROUP"),
		Partitions: int32(num("KCERT_PARTITIONS", 0)), Records: num("KCERT_RECORDS", 0),
		Records2: num("KCERT_RECORDS2", 0), Keys: num("KCERT_KEYS", 1),
		Timeout: time.Duration(num("KCERT_TIMEOUT_SEC", 60)) * time.Second,
		Admin:   os.Getenv("KCERT_ADMIN") != "0",
	}
	if in.Bootstrap == "" || in.Topic == "" || in.Group == "" || (in.Mode != "scenario" && in.Mode != "stock-defaults") {
		l.Fail("inputs", -1, fmt.Errorf("KCERT_BOOTSTRAP, KCERT_TOPIC, KCERT_GROUP and KCERT_MODE (scenario|stock-defaults) are required"))
	}
	return in
}

func num(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

// Key, Value and Headers are the record content of SCENARIO.md.
func Key(seq, keys int) string { return fmt.Sprintf("key-%02d", seq%keys) }
func Value(seq int) string     { return fmt.Sprintf("value-%06d", seq) }

// SeqOf recovers seq from a value, or -1.
func SeqOf(value []byte) int {
	var n int
	if _, err := fmt.Sscanf(string(value), "value-%d", &n); err != nil {
		return -1
	}
	return n
}

// Ledger writes events, one JSON object per line, on standard output.
type Ledger struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func NewLedger() *Ledger { return &Ledger{enc: json.NewEncoder(os.Stdout)} }

// Emit writes one event. ev is the event kind; fields are added as given.
func (l *Ledger) Emit(ev string, fields map[string]any) {
	m := map[string]any{"ev": ev}
	for k, v := range fields {
		m[k] = v
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = l.enc.Encode(m)
}

// Fail writes an error event and a done event, then exits non-zero.
func (l *Ledger) Fail(step string, code int, err error) {
	l.Emit("error", map[string]any{"step": step, "error_code": code, "error": err.Error()})
	l.Emit("done", nil)
	os.Exit(1)
}

// Logf writes a diagnostic line on standard error (never part of the ledger).
func Logf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, time.Now().UTC().Format("15:04:05.000 ")+format+"\n", a...)
}

// ModuleVersion is the version of a module linked into this binary, read from its build info:
// the version the driver actually loaded, not a configured one.
func ModuleVersion(path string) string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if d.Path == path {
				if d.Replace != nil {
					return d.Replace.Version
				}
				return d.Version
			}
		}
	}
	return "unknown"
}
