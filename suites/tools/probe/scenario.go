package probe

// Scenarios: a long, stateful run of one platform tool (Connect, Streams, MirrorMaker 2) whose
// phases each feed one matrix row. The scenario runs once per broker, the first time one of its
// rows asks for it; every row then reads its own section. A scenario that stops at a phase
// leaves that phase's section and every later one without an observation, so those rows report
// the scenario's error (oracle: VOID; KubeMQ: FAIL) and earlier rows keep their verdicts.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Sections is a scenario's per-phase observations, filled in order.
type Sections struct {
	ctx   context.Context
	obs   map[string]Observation
	order []string
	err   error  // why the scenario stopped, if it did
	at    string // the section it stopped in
	errs  map[string]error
}

// fail records that one section could not be observed, without stopping the scenario (for
// scenarios with independent parts, such as MirrorMaker 2's two directions).
func (s *Sections) fail(section string, err error) {
	if s.errs == nil {
		s.errs = map[string]error{}
	}
	if _, ok := s.errs[section]; !ok {
		s.errs[section] = err
	}
}

func (s *Sections) set(section string, o Observation) {
	if s.ctx != nil && s.ctx.Err() != nil {
		s.fail(section, fmt.Errorf("the scenario ran out of its %s budget before this phase was recorded", scenarioBudget))
		return
	}
	if s.obs == nil {
		s.obs = map[string]Observation{}
	}
	s.obs[section] = o
	s.order = append(s.order, section)
}

// stop records that the scenario could not continue past section.
func (s *Sections) stop(section string, err error) {
	if s.err == nil {
		s.err, s.at = err, section
	}
}

type scenarioFunc func(ctx context.Context, env Env, s *Sections)

var (
	scenarioMu    sync.Mutex
	scenarioCache = map[string]*Sections{}
)

// scenarioBudget bounds one whole scenario on one broker, independent of the row that starts it.
// Sections recorded after it ran out are failed, never compared (calls after a deadline fail
// fast and would read as behaviour).
const scenarioBudget = 50 * time.Minute

// sectionProbe returns a probe for one section of a scenario.
func sectionProbe(scenario string, run scenarioFunc, section string) Probe {
	return func(_ context.Context, env Env) (Observation, error) {
		scenarioMu.Lock()
		key := scenario + "/" + env.Target
		s, ok := scenarioCache[key]
		if !ok {
			ctx, cancel := context.WithTimeout(context.Background(), scenarioBudget)
			s = &Sections{ctx: ctx}
			scenarioCache[key] = s
			scenarioMu.Unlock()
			run(ctx, env, s)
			cancel()
		} else {
			scenarioMu.Unlock()
		}
		if o, ok := s.obs[section]; ok {
			return o, nil
		}
		if err, ok := s.errs[section]; ok {
			return nil, err
		}
		if s.err != nil {
			if s.at == section {
				return nil, s.err
			}
			return nil, fmt.Errorf("the %s scenario stopped in its %s phase, before this one: %v", scenario, s.at, s.err)
		}
		return nil, fmt.Errorf("the %s scenario produced no %s observation", scenario, section)
	}
}

// workDir is a fresh host directory for one scenario on one broker, inside the run directory
// (never committed), mounted into the containers that need files. It is world-writable because
// the image's tools run as a non-root user.
func workDir(env Env, scenario string) (string, error) {
	base := os.Getenv("CERT_RUN_DIR")
	if base == "" {
		base = os.TempDir()
	}
	d := filepath.Join(base, "tools", fmt.Sprintf("%s-%s-%d", scenario, env.Target, time.Now().Unix()))
	for _, sub := range []string{"conf", "data", "logs"} {
		if err := os.MkdirAll(filepath.Join(d, sub), 0o777); err != nil {
			return "", err
		}
		if err := os.Chmod(filepath.Join(d, sub), 0o777); err != nil {
			return "", err
		}
	}
	return d, nil
}

// runLabel is this run's container label; container names start with it so runs never collide.
func runLabel() string {
	if l := os.Getenv("TOOLS_RUN_LABEL"); l != "" {
		return l
	}
	return "manual"
}

// containerName is a unique, readable container name for one scenario role.
func containerName(env Env, scenario, role string) string {
	t := "k"
	if env.Target == "real-kafka" {
		t = "o"
	}
	return strings.ToLower(fmt.Sprintf("kct-%s-%s-%s-%s", runLabel(), scenario, t, role))
}

// waitLog polls a followed log file for any of the given texts; returns the one seen first.
func waitLog(ctx context.Context, path string, limit time.Duration, texts ...string) string {
	var found string
	waitFor(ctx, limit, time.Second, func() bool {
		b, _ := os.ReadFile(path)
		for _, t := range texts {
			if strings.Contains(string(b), t) {
				found = t
				return true
			}
		}
		return false
	})
	return found
}

// countLines returns how many lines of the file contain text.
func countLines(path, text string) int {
	b, _ := os.ReadFile(path)
	n := 0
	for _, l := range strings.Split(string(b), "\n") {
		if strings.Contains(l, text) {
			n++
		}
	}
	return n
}

// clientVersionsInLog returns the distinct AppInfoParser "Kafka version:" banners of a log, so
// a row proves which kafka-clients the program actually loaded.
func clientVersionsInLog(path string) string {
	b, _ := os.ReadFile(path)
	seen := map[string]bool{}
	for _, l := range strings.Split(string(b), "\n") {
		if i := strings.Index(l, "Kafka version: "); i >= 0 {
			f := strings.Fields(l[i+len("Kafka version: "):])
			if len(f) > 0 {
				seen[f[0]] = true
			}
		}
	}
	return strings.Join(sortedKeys(seen), ",")
}
