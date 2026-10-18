// Package box runs Apache Kafka's own tools inside client containers of the pinned Kafka image.
//
// Every container is started and driven through suites/tools/ctr.sh, which calls only the engine
// abstraction (runner/lib/engine.sh), so this package never names an engine. Each container
// reaches both brokers through the host-gateway alias.
package box

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Ctr is the helper script; set by the track from its own location.
var Ctr = "suites/tools/ctr.sh"

// Box is one long-lived container of the Kafka image, used to exec tools.
type Box struct {
	Name string
}

// Result is one tool invocation.
type Result struct {
	Stdout  string
	Stderr  string
	Exit    int
	Elapsed time.Duration
}

// Combined is stdout then stderr: the Kafka tools print some errors to each.
func (r Result) Combined() string { return r.Stdout + "\n" + r.Stderr }

// JavaEnv is the engine run options every JVM container gets. On arm64 the image's JRE can die
// with SIGILL in some engine VMs unless SVE is off; harmless elsewhere.
func JavaEnv() []string {
	if os.Getenv("CERT_ENGINE_ARCH") == "arm64" {
		return []string{"-e", "JAVA_TOOL_OPTIONS=-XX:UseSVE=0"}
	}
	return nil
}

// Image is the pinned Kafka image the kit uses for the oracle and every tool.
func Image() string {
	return os.Getenv("KAFKA_IMAGE_REPO") + "@" + os.Getenv("KAFKA_IMAGE_DIGEST")
}

// ctr runs the helper script with args.
func ctr(ctx context.Context, stdin string, args ...string) Result {
	var in io.Reader
	if stdin != "" {
		in = strings.NewReader(stdin)
	}
	return ctrReader(ctx, in, args...)
}

// ctrReader runs the helper script with args, streaming stdin from in (nil for none).
func ctrReader(ctx context.Context, in io.Reader, args ...string) Result {
	start := time.Now()
	cmd := exec.CommandContext(ctx, Ctr, args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if in != nil {
		cmd.Stdin = in
	}
	err := cmd.Run()
	r := Result{Stdout: out.String(), Stderr: stripJVMNoise(errb.String()), Elapsed: time.Since(start)}
	if err != nil {
		r.Exit = -1
		if ee, ok := err.(*exec.ExitError); ok {
			r.Exit = ee.ExitCode()
		}
		if r.Exit == -1 {
			r.Stderr += "\n" + err.Error()
		}
	}
	logCall(args, r)
	if r.Exit == 2 && strings.Contains(r.Stderr, "the container engine did not answer") {
		engineDown.Store(true)
	}
	if why := infraReason(args, r, ctx.Err()); why != "" {
		infra.Lock()
		infra.n++
		infra.last = why
		infra.Unlock()
	}
	return r
}

var infra struct {
	sync.Mutex
	n    int
	last string
}

// infraReason says why a helper call failed for a reason that is not the tool's own answer: the
// call ran out of time, the engine refused to start the command (125: engine error, 126: not
// executable, 127: not found), the container was gone, or the engine daemon errored. Such a
// result is never evidence about a broker.
func infraReason(args []string, r Result, ctxErr error) string {
	if len(args) == 0 || args[0] == "exists" || args[0] == "running" || args[0] == "kill" || args[0] == "rm" {
		return ""
	}
	cmd := strings.Join(args, " ")
	if len(cmd) > 160 {
		cmd = cmd[:160] + "..."
	}
	switch {
	case ctxErr != nil:
		return fmt.Sprintf("%q did not finish before its deadline (%v)", cmd, ctxErr)
	case r.Exit == -1:
		return fmt.Sprintf("%q could not be started: %s", cmd, lastLine(r.Stderr))
	case args[0] == "exec" && (r.Exit == 125 || r.Exit == 126 || r.Exit == 127):
		return fmt.Sprintf("%q: the engine could not run the command (exit %d): %s", cmd, r.Exit, lastLine(r.Stderr))
	case strings.Contains(r.Stderr, "Error response from daemon") || strings.Contains(r.Stderr, "is not running"):
		return fmt.Sprintf("%q: engine error: %s", cmd, lastLine(r.Stderr))
	}
	return ""
}

// InfraCount is how many helper calls so far failed for infrastructure reasons; InfraLast names
// the latest. A leg whose count grew is not evidence (see infraReason).
func InfraCount() int { infra.Lock(); defer infra.Unlock(); return infra.n }

// InfraLast describes the latest infrastructure failure.
func InfraLast() string { infra.Lock(); defer infra.Unlock(); return infra.last }

var engineDown atomic.Bool

// EngineDown reports whether the container engine stopped answering during the run (an
// infrastructure failure: nothing after that point is evidence about a broker).
func EngineDown() bool { return engineDown.Load() }

var callLog struct {
	sync.Mutex
	n int
}

// logCall appends one helper call (command, exit code, output) to logs/tools-calls.log in the run
// directory, which goes into the raw-logs tarball, never into git. Polling calls (exists,
// running) are not logged.
func logCall(args []string, r Result) {
	dir := os.Getenv("CERT_RUN_DIR")
	if dir == "" || len(args) == 0 || args[0] == "exists" || args[0] == "running" || args[0] == "logs" {
		return
	}
	callLog.Lock()
	defer callLog.Unlock()
	callLog.n++
	f, err := os.OpenFile(filepath.Join(dir, "logs", "tools-calls.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "=== call %d at %s: %s\n--- exit %d after %s\n--- stdout\n%s\n--- stderr\n%s\n",
		callLog.n, time.Now().UTC().Format(time.RFC3339), strings.Join(args, " "), r.Exit, r.Elapsed.Round(time.Millisecond), r.Stdout, r.Stderr)
}

// Start starts a detached container. opts are engine run options (labels, env, entrypoint,
// network); cmd is the image command.
func Start(ctx context.Context, name string, opts []string, image string, cmd ...string) (*Box, error) {
	args := append([]string{"start", name}, opts...)
	args = append(append(args, "--", image), cmd...)
	r := ctr(ctx, "", args...)
	if r.Exit != 0 {
		return nil, fmt.Errorf("start container %s: exit %d: %s", name, r.Exit, lastLine(r.Stderr))
	}
	return &Box{Name: name}, nil
}

// StartToolbox starts an idle container of the Kafka image to exec tools in.
func StartToolbox(ctx context.Context, name string) (*Box, error) {
	opts := append(JavaEnv(), "--entrypoint", "sleep")
	b, err := Start(ctx, name, opts, Image(), "86400")
	if err != nil {
		return nil, err
	}
	// The container is up when exec works.
	for i := 0; i < 30; i++ {
		if r := ctr(ctx, "", "exec", name, "true"); r.Exit == 0 {
			return b, nil
		}
		time.Sleep(time.Second)
	}
	return nil, fmt.Errorf("toolbox %s never accepted exec", name)
}

// Remove stops and removes the container.
func (b *Box) Remove() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ctr(ctx, "", "rm", b.Name)
}

// Kill sends a signal (KILL, TERM).
func (b *Box) Kill(ctx context.Context, signal string) error {
	if r := ctr(ctx, "", "kill", b.Name, signal); r.Exit != 0 {
		return fmt.Errorf("kill %s %s: %s", signal, b.Name, lastLine(r.Stderr))
	}
	return nil
}

// Running reports whether the container is running.
func (b *Box) Running(ctx context.Context) bool { return ctr(ctx, "", "running", b.Name).Exit == 0 }

// Logs returns the container log so far.
func (b *Box) Logs(ctx context.Context) string { return ctr(ctx, "", "logs", b.Name).Stdout }

// Sh runs a shell command in the container, bounded by limit.
func (b *Box) Sh(ctx context.Context, stdin string, limit time.Duration, script string) Result {
	return ctr(ctx, stdin, "exec", b.Name, "timeout", "-s", "KILL", secs(limit), "sh", "-c", script)
}

// Tool runs /opt/kafka/bin/<tool> with args in the container, bounded by limit (the process is
// killed inside the container, so a hung tool never outlives its step).
func (b *Box) Tool(ctx context.Context, stdin string, limit time.Duration, tool string, args ...string) Result {
	argv := append([]string{"exec", b.Name, "timeout", "-s", "KILL", secs(limit), "/opt/kafka/bin/" + tool}, args...)
	r := ctr(ctx, stdin, argv...)
	if r.Exit == 137 {
		r.Stderr += fmt.Sprintf("\n[box] %s killed after %s", tool, limit)
	}
	return r
}

// ToolStream is Tool with stdin streamed from in, for a feeder that paces its input.
func (b *Box) ToolStream(ctx context.Context, in io.Reader, limit time.Duration, tool string, args ...string) Result {
	argv := append([]string{"exec", b.Name, "timeout", "-s", "KILL", secs(limit), "/opt/kafka/bin/" + tool}, args...)
	return ctrReader(ctx, in, argv...)
}

// WriteFile writes content to path inside the container.
func (b *Box) WriteFile(ctx context.Context, path, content string) error {
	r := b.Sh(ctx, content, 30*time.Second, "cat > "+shQuote(path))
	if r.Exit != 0 {
		return fmt.Errorf("write %s in %s: %s", path, b.Name, lastLine(r.Stderr))
	}
	return nil
}

func secs(d time.Duration) string {
	s := int(d.Seconds())
	if s < 1 {
		s = 1
	}
	return strconv.Itoa(s)
}

func shQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

var jvmNoise = regexp.MustCompile(`(?m)^Picked up JAVA_TOOL_OPTIONS:.*\n?`)

func stripJVMNoise(s string) string { return jvmNoise.ReplaceAllString(s, "") }

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}

// exceptionPattern finds a Kafka or Java exception class in tool output.
var exceptionPattern = regexp.MustCompile(`(?:org\.apache\.kafka\.common\.errors|java\.lang|org\.apache\.kafka\.common\.config)\.([A-Za-z]+(?:Exception|Error))`)

// Exception is the simple name of the first Kafka or Java exception a tool printed, "" if none.
// Rows compare exception classes, never message text.
func Exception(r Result) string {
	if m := exceptionPattern.FindStringSubmatch(r.Combined()); m != nil {
		return m[1]
	}
	return ""
}

// Follow streams a container's log into path until the container stops (or stop is called).
// Containers are removed when they stop, so the file is the only lasting copy of the log.
func Follow(name, path string) (stop func(), err error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(Ctr, "follow", name)
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		f.Close()
		return nil, err
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); f.Close(); close(done) }()
	return func() {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}, nil
}

// Gone waits until no container of this name exists (a stopped container is removed
// asynchronously), so the name can be reused.
func (b *Box) Gone(ctx context.Context, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if r := ctr(ctx, "", "exists", b.Name); r.Exit == 1 {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// Network creates a private network for helper containers; remove it with the returned func.
func Network(ctx context.Context, name string) (func(), error) {
	if r := ctr(ctx, "", "net-create", name); r.Exit != 0 {
		return nil, fmt.Errorf("network %s: %s", name, lastLine(r.Stderr))
	}
	return func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		ctr(c, "", "net-rm", name)
	}, nil
}
