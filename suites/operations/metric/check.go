package metric

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// Expectation kinds.
const (
	// DeltaExact: the selected counters grew by exactly Factor x workload[Of] (+ Slack).
	DeltaExact = "delta-exact"
	// DeltaRange: the growth lies in [Min x workload[Of], Max x workload[Of] + Slack].
	DeltaRange = "delta-range"
	// DeltaPositive: the selected counters grew by at least 1 (request and call counters, whose
	// exact count depends on client batching and is not a broker property).
	DeltaPositive = "delta-positive"
	// GaugeEquals: after the workload the selected gauges sum to Factor x workload[Of] (or to
	// Value when Of is empty).
	GaugeEquals = "gauge-equals"
	// GaugeRange: after the workload the selected gauges sum into [Min, Max] (absolute values,
	// or multiples of workload[Of] when Of is set).
	GaugeRange = "gauge-range"
)

// Expect is a row's expectation, evaluated against workload quantities such as "records"
// (records the workload produced) or "payload_bytes" (sum of key and value bytes).
type Expect struct {
	Kind   string  `yaml:"kind"`
	Of     string  `yaml:"of,omitempty"`
	Factor float64 `yaml:"factor,omitempty"` // DeltaExact, GaugeEquals; 0 means 1
	Min    float64 `yaml:"min,omitempty"`
	Max    float64 `yaml:"max,omitempty"`
	Slack  float64 `yaml:"slack,omitempty"` // added to the upper bound of DeltaRange
	Value  float64 `yaml:"value,omitempty"` // GaugeEquals without Of
}

// Validate rejects an expectation the verifier cannot evaluate.
func (e Expect) Validate() error {
	switch e.Kind {
	case DeltaExact:
		if e.Of == "" {
			return fmt.Errorf("%s needs of", e.Kind)
		}
	case DeltaRange:
		if e.Of == "" || e.Max <= 0 || e.Min > e.Max {
			return fmt.Errorf("%s needs of and 0 <= min <= max, max > 0", e.Kind)
		}
	case DeltaPositive:
	case GaugeEquals:
	case GaugeRange:
		if e.Min > e.Max {
			return fmt.Errorf("%s needs min <= max", e.Kind)
		}
	default:
		return fmt.Errorf("unknown expectation kind %q", e.Kind)
	}
	return nil
}

func (e Expect) isDelta() bool {
	return e.Kind == DeltaExact || e.Kind == DeltaRange || e.Kind == DeltaPositive
}

// Result is one leg's judgement. Present, Monotonic and Met are the deterministic facts that
// go into the observation and are compared between the brokers; Measured and Want explain them.
type Result struct {
	Present   bool    // the selector matched at least one series after the workload
	Monotonic bool    // no selected counter decreased (always true for gauges)
	Met       bool    // the expectation holds
	Measured  float64 // the delta (counters) or the summed value (gauges)
	Want      string  // the expectation in words, with the workload quantity filled in
}

// Observation is what the leg contributes to the row's broker-vs-broker comparison.
func (r Result) Observation() map[string]any {
	return map[string]any{"series_present": r.Present, "counter_monotonic": r.Monotonic, "expectation_met": r.Met}
}

// String explains the result in one line.
func (r Result) String() string {
	switch {
	case !r.Present:
		return "series absent"
	case !r.Monotonic:
		return "counter went backwards"
	case !r.Met:
		return fmt.Sprintf("measured %s, want %s", num(r.Measured), r.Want)
	default:
		return fmt.Sprintf("measured %s, %s", num(r.Measured), r.Want)
	}
}

// Check judges one broker. before and after are scrapes taken around the workload; workload
// holds its quantities. A series present only after the workload counts from 0 (a counter that
// the broker creates on first use).
func Check(before, after Scrape, sel Selector, exp Expect, workload map[string]float64) (Result, error) {
	if err := exp.Validate(); err != nil {
		return Result{}, err
	}
	var base float64
	if exp.Of != "" {
		v, ok := workload[exp.Of]
		if !ok {
			return Result{}, fmt.Errorf("workload quantity %q not recorded", exp.Of)
		}
		base = v
	}
	factor := exp.Factor
	if factor == 0 {
		factor = 1
	}
	b, a := before.Select(sel), after.Select(sel)
	r := Result{Present: len(a) > 0, Monotonic: true}
	if !r.Present {
		r.Want = describe(exp, base, factor)
		return r, nil
	}
	if exp.isDelta() {
		for _, k := range sortedKeys(a) {
			d := a[k] - b[k]
			if d < 0 || math.IsNaN(d) {
				r.Monotonic = false
			}
			r.Measured += d
		}
		// A series that vanished counts as a reset: a counter must not disappear mid-run.
		for k := range b {
			if _, ok := a[k]; !ok {
				r.Monotonic = false
			}
		}
	} else {
		for _, v := range a {
			r.Measured += v
		}
	}
	r.Want = describe(exp, base, factor)
	const eps = 1e-9
	m := r.Measured
	switch exp.Kind {
	case DeltaExact:
		r.Met = math.Abs(m-factor*base-exp.Slack) < eps
	case DeltaRange:
		r.Met = m >= exp.Min*base-eps && m <= exp.Max*base+exp.Slack+eps
	case DeltaPositive:
		r.Met = m >= 1
	case GaugeEquals:
		want := exp.Value
		if exp.Of != "" {
			want = factor * base
		}
		r.Met = math.Abs(m-want) < eps
	case GaugeRange:
		lo, hi := exp.Min, exp.Max
		if exp.Of != "" {
			lo, hi = lo*base, hi*base
		}
		r.Met = m >= lo-eps && m <= hi+eps
	}
	r.Met = r.Met && r.Monotonic
	return r, nil
}

func describe(e Expect, base, factor float64) string {
	of := func(f float64) string {
		if e.Of == "" {
			return num(f)
		}
		return fmt.Sprintf("%s (%s x %s)", num(f*base), num(f), e.Of)
	}
	switch e.Kind {
	case DeltaExact:
		s := "growth of " + of(factor)
		if e.Slack != 0 {
			s += " + " + num(e.Slack)
		}
		return s
	case DeltaRange:
		return fmt.Sprintf("growth between %s and %s + %s", of(e.Min), of(e.Max), num(e.Slack))
	case DeltaPositive:
		return "growth of at least 1"
	case GaugeEquals:
		if e.Of == "" {
			return "value " + num(e.Value)
		}
		return "value " + of(factor)
	case GaugeRange:
		return fmt.Sprintf("value between %s and %s", of(e.Min), of(e.Max))
	}
	return e.Kind
}

func num(f float64) string {
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return fmt.Sprintf("%.0f", f)
	}
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.3f", f), "0"), ".")
}

func sortedKeys(m map[string]float64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
