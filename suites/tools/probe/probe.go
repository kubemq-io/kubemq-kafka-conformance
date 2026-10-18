// Package probe holds the tools-track probes (Track D): Apache Kafka's own CLI tools, Connect,
// Streams and MirrorMaker 2, each run from the pinned Kafka image.
//
// A probe runs one scenario against one broker and returns an Observation of deterministic facts
// (exit codes, exception classes, counts, offsets), never timings, identifiers or message text.
// The track runs every probe on the Kafka oracle first, then on KubeMQ with only the bootstrap
// address changed, and compares the two observations field by field (features/probe.Diff).
package probe

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	fprobe "github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/box"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
)

// Observation is what a probe saw; the same type and diff as the features track.
type Observation = fprobe.Observation

// Env is what a probe gets: one broker, reached from containers, and a toolbox container.
type Env struct {
	Bootstrap string // host-gateway alias address of the broker under test
	Target    string // "real-kafka" or "kubemq", for names only
	Box       *box.Box
}

// Probe runs one scenario against one broker. An error means the probe could not complete
// (oracle: VOID; KubeMQ: FAIL); a behaviour difference must be an Observation field.
type Probe func(ctx context.Context, env Env) (Observation, error)

// Registry maps the probe names used in matrix/tools.yaml to probes.
var Registry = map[string]Probe{}

func register(name string, p Probe) { Registry[name] = p }

// name returns a short unique resource name. KubeMQ v3.6.16 crashes on topic names of 96+
// characters (server task 4), so names stay well under that.
func name(env Env, tag string) string {
	t := "k"
	if env.Target == "real-kafka" {
		t = "o"
	}
	return fmt.Sprintf("kc-%s-%s%d", tag, t, time.Now().UnixNano()%1_000_000_000_000)
}

// toolLimit bounds one tool invocation; a tool still running then is killed in its container.
const toolLimit = 120 * time.Second

func (e Env) tool(ctx context.Context, stdin, tool string, args ...string) box.Result {
	return e.Box.Tool(ctx, stdin, toolLimit, tool, args...)
}

// bs prefixes args with --bootstrap-server.
func (e Env) bs(args ...string) []string {
	return append([]string{"--bootstrap-server", e.Bootstrap}, args...)
}

// waitFor polls cond every interval until it holds or limit passes.
func waitFor(ctx context.Context, limit, interval time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(limit)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(interval):
		}
	}
}

var errorLine = regexp.MustCompile(`(Exception|Error|ERROR|does not|not supported|Failed)`)

// firstErrorLine is the first output line carrying an exception message, for error text.
func firstErrorLine(s string) string {
	first := ""
	for _, l := range strings.Split(s, "\n") {
		l = strings.TrimSpace(l)
		if l == "" || strings.HasPrefix(l, "at ") {
			continue
		}
		if strings.Contains(l, "Exception: ") {
			return l
		}
		if first == "" && errorLine.MatchString(l) {
			first = l
		}
	}
	if first == "" {
		ls := kout.Lines(s)
		if len(ls) > 0 {
			return ls[len(ls)-1]
		}
	}
	return first
}

// mustTool runs a tool that must succeed for the scenario to continue; a non-zero exit is an
// error (the probe could not complete), not an observation.
func (e Env) mustTool(ctx context.Context, step, stdin, tool string, args ...string) (box.Result, error) {
	r := e.tool(ctx, stdin, tool, args...)
	if r.Exit != 0 {
		return r, fmt.Errorf("%s: exit %d: %s", step, r.Exit, firstErrorLine(r.Combined()))
	}
	return r, nil
}

// put merges the JSON fields of v into obs as "<prefix>_<field>", so a verifier's summary is
// compared field by field and a deviation names the exact field.
func put(obs Observation, prefix string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		obs[prefix] = fmt.Sprintf("unencodable: %v", err)
		return
	}
	var m map[string]any
	if json.Unmarshal(b, &m) != nil {
		obs[prefix] = string(b)
		return
	}
	for k, x := range m {
		obs[prefix+"_"+k] = x
	}
}
