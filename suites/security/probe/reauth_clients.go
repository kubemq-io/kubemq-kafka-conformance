package probe

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
)

func init() {
	register("reauth-java-long-session", reauthJava)
	register("reauth-librdkafka-long-session", reauthLibrdkafka)
}

// clientRun runs a client container in the network namespace of the broker's container, so the
// broker's advertised 127.0.0.1 address is the broker. Through runner/client-run.sh only (the
// engine abstraction); the posture directory is mounted at /posture.
func clientRun(ctx context.Context, env Env, extra []string, image string, cmd ...string) ([]byte, error) {
	root := os.Getenv("CERT_ROOT")
	if root == "" || env.Container == "" {
		return nil, fmt.Errorf("container client rows need CERT_ROOT and the broker container name (run them from runner/certify.sh)")
	}
	args := append([]string{"--network", "container:" + env.Container, "-v", env.PostureDir + ":/posture:ro"}, extra...)
	args = append(append(args, "--", image), cmd...)
	c := exec.CommandContext(ctx, filepath.Join(root, "runner", "client-run.sh"), args...)
	var stderr bytes.Buffer
	c.Stderr = &stderr
	out, err := c.Output()
	if err != nil {
		return out, fmt.Errorf("client container: %w: %s", err, lastLines(stderr.String(), 5))
	}
	return out, nil
}

// clientRunPlain runs a container through runner/client-run.sh on its own network and returns
// its combined output; the exit status is not an error (the caller judges the output).
func clientRunPlain(ctx context.Context, extra []string, image string, cmd ...string) ([]byte, error) {
	root := os.Getenv("CERT_ROOT")
	if root == "" {
		return nil, fmt.Errorf("container rows need CERT_ROOT (run them from runner/certify.sh)")
	}
	args := append(append(extra, "--", image), cmd...)
	return exec.CommandContext(ctx, filepath.Join(root, "runner", "client-run.sh"), args...).CombinedOutput()
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}

// pin reads NAME=value from an env-style pins file.
func pin(file, name string) (string, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	for _, l := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(l), name+"="); ok {
			return v, nil
		}
	}
	return "", fmt.Errorf("%s: no %s", file, name)
}

func longSessionTopic(ctx context.Context, env Env, label string) (string, error) {
	cl, err := saslClient(env, "plain", mechanism("PLAIN", adminUser, adminPass))
	if err != nil {
		return "", err
	}
	defer cl.Close()
	topic := topicName(env, label)
	return topic, createTopic(ctx, kadm.NewClient(cl), topic, 1)
}

// VerifiableProducerCounts counts the send outcomes in kafka-verifiable-producer's JSON lines.
// Pure, for the tests.
func VerifiableProducerCounts(out []byte) (acked, failed int) {
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		var ev struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			continue
		}
		switch ev.Name {
		case "producer_send_success":
			acked++
		case "producer_send_error":
			failed++
		}
	}
	return acked, failed
}

// reauthJava: Kafka's own kafka-verifiable-producer (Java kafka-clients 4.3.1, SCRAM-SHA-512,
// SASL_PLAINTEXT) sends 25 records at one per second, two and a half re-authentication windows.
func reauthJava(ctx context.Context, env Env) (Observation, error) {
	topic, err := longSessionTopic(ctx, env, "reauth-java")
	if err != nil {
		return nil, err
	}
	image := os.Getenv("KAFKA_IMAGE_REPO") + "@" + os.Getenv("KAFKA_IMAGE_DIGEST")
	out, err := clientRun(ctx, env, nil, image, "/opt/kafka/bin/kafka-verifiable-producer.sh",
		"--bootstrap-server", env.PlainInContainer, "--topic", topic, "--max-messages", "25", "--throughput", "1",
		"--acks", "-1", "--producer.config", "/posture/reauth-java.properties")
	if err != nil {
		return nil, err
	}
	acked, failed := VerifiableProducerCounts(out)
	return Observation{"all_acked": acked == 25 && failed == 0, "failed_sends": failed}, nil
}

// reauthLibrdkafka: confluent-kafka-python 2.15.1 (librdkafka 2.15.1, the wheel pinned by the
// clients track) sends one record every 2 seconds for 25 seconds with SCRAM-SHA-512.
func reauthLibrdkafka(ctx context.Context, env Env) (Observation, error) {
	topic, err := longSessionTopic(ctx, env, "reauth-rdk")
	if err != nil {
		return nil, err
	}
	root := os.Getenv("CERT_ROOT")
	image, err := pin(filepath.Join(root, "suites", "clients", "clients.env"), "PYTHON_RUNTIME_IMAGE")
	if err != nil {
		return nil, err
	}
	extra := []string{
		"-v", filepath.Join(root, "suites", "clients", "drivers", "python", "requirements.txt") + ":/req/requirements.txt:ro",
		"-v", filepath.Join(root, "suites", "security", "drivers") + ":/drivers:ro",
	}
	out, err := clientRun(ctx, env, extra, image, "sh", "-c",
		"pip install -q --no-cache-dir --require-hashes -r /req/requirements.txt >/dev/null 2>&1 && python /drivers/reauth_librdkafka.py "+
			env.PlainInContainer+" "+topic+" SCRAM-SHA-512 25")
	if err != nil {
		return nil, err
	}
	var r struct {
		Acked, Failed int
		Librdkafka    string
	}
	if err := json.Unmarshal(bytes.TrimSpace(out), &r); err != nil {
		return nil, fmt.Errorf("driver output %q: %w", out, err)
	}
	fmt.Fprintf(os.Stderr, "[security] %s librdkafka %s acked %d failed %d\n", env.Target, r.Librdkafka, r.Acked, r.Failed)
	return Observation{"all_acked": r.Acked > 0 && r.Failed == 0, "failed_sends": r.Failed}, nil
}
