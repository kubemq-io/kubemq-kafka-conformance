package probe

// Schema Registry rows (opt-in track "schema-registry"): Confluent Schema Registry from the
// confluentinc/cp-schema-registry image, which is under the Confluent Community License, not open
// source. The image is pulled at run time only when the operator accepts that license
// (CERT_ACCEPT_CONFLUENT_COMMUNITY_LICENSE=yes, checked by
// suites/schema-registry/track.sh) and is never redistributed. Ported from kubemq-server
// tests/kafka-ecosystem/schema-registry.sh.
//
// The registry keeps every schema in one compacted Kafka topic and rebuilds its state from that
// topic alone on restart, so the broker must store and serve it faithfully.
//   register-and-compat  two backward-compatible versions register with distinct ids; an
//                        incompatible third is judged incompatible; the store topic has one
//                        partition and cleanup.policy=compact
//   restart-rebuild      after a registry restart, the subject has versions [1,2] and schema id 2
//                        reads back byte-identical, rebuilt from the store topic
// The registry runs its own bundled kafka-clients, recorded from its log banner.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/box"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
)

func init() {
	for _, s := range []string{"register-and-compat", "restart-rebuild"} {
		register("sr-"+s, sectionProbe("schema-registry", srScenario, s))
	}
	Expectations["sr-register-and-compat"] = Observation{"registry_answers": true, "v1_registered": true, "v2_registered": true,
		"ids_distinct": true, "incompatible_rejected": true, "store_topic_partitions": 1, "store_topic_cleanup_policy": "compact"}
	Expectations["sr-restart-rebuild"] = Observation{"registry_answers_after_restart": true, "versions_after_restart": "[1,2]",
		"schema_v2_reads_back_identical": true, "registry_logs_kafka_clients_banner": true}
}

const (
	srV1  = `{"type":"record","name":"Order","fields":[{"name":"id","type":"int"}]}`
	srV2  = `{"type":"record","name":"Order","fields":[{"name":"id","type":"int"},{"name":"amount","type":"double","default":0.0}]}`
	srBad = `{"type":"record","name":"Order","fields":[{"name":"sku","type":"string"}]}`
)

type srRun struct {
	env      Env
	dir      string
	net      string
	name     string
	store    string
	client   *box.Box
	reg      *box.Box
	stops    []func()
	logs     []string
	restarts int
}

func (r *srRun) startRegistry(ctx context.Context) error {
	r.restarts++
	opts := append(box.JavaEnv(), "--network", r.net,
		"-e", "SCHEMA_REGISTRY_HOST_NAME="+r.name,
		"-e", "SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081",
		"-e", "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://"+r.env.Bootstrap,
		"-e", "SCHEMA_REGISTRY_KAFKASTORE_TOPIC="+r.store,
		"-e", "SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR=1")
	b, err := box.Start(ctx, r.name, opts, os.Getenv("SCHEMA_REGISTRY_IMAGE"))
	if err != nil {
		return err
	}
	r.reg = b
	logPath := filepath.Join(r.dir, "logs", fmt.Sprintf("registry-%d.log", r.restarts))
	stop, err := box.Follow(r.name, logPath)
	if err != nil {
		b.Remove()
		return fmt.Errorf("follow the log of %s: %w", r.name, err)
	}
	r.logs = append(r.logs, logPath)
	r.stops = append(r.stops, stop)
	return nil
}

// call sends a GET (body "") or a POST with a JSON body to the registry from the client container.
func (r *srRun) call(ctx context.Context, path, body string) (string, error) {
	url := "http://" + r.name + ":8081" + path
	script := "wget -qO- '" + url + "'"
	if body != "" {
		script = "cat > /tmp/sr-body.json && wget -qO- --header 'Content-Type: application/vnd.schemaregistry.v1+json' --post-file=/tmp/sr-body.json '" + url + "'"
	}
	res := r.client.Sh(ctx, body, 30*time.Second, script)
	if res.Exit != 0 {
		return res.Stdout, fmt.Errorf("%s: %s", path, strings.TrimSpace(res.Stderr))
	}
	return res.Stdout, nil
}

func (r *srRun) answers(ctx context.Context, limit time.Duration) bool {
	return waitFor(ctx, limit, 2*time.Second, func() bool {
		_, err := r.call(ctx, "/subjects", "")
		return err == nil
	})
}

func (r *srRun) post(ctx context.Context, path, schema string) (map[string]any, error) {
	b, _ := json.Marshal(map[string]string{"schema": schema})
	out, err := r.call(ctx, path, string(b))
	if err != nil {
		return nil, err
	}
	var m map[string]any
	return m, json.Unmarshal([]byte(out), &m)
}

func (r *srRun) cleanup() {
	if r.reg != nil {
		r.reg.Remove()
	}
	if r.client != nil {
		r.client.Remove()
	}
	for _, s := range r.stops {
		s()
	}
}

func srScenario(ctx context.Context, env Env, s *Sections) {
	if os.Getenv("SCHEMA_REGISTRY_IMAGE") == "" {
		s.stop("register-and-compat", fmt.Errorf("SCHEMA_REGISTRY_IMAGE is not set (run the opt-in schema-registry track)"))
		return
	}
	dir, err := workDir(env, "sr")
	if err != nil {
		s.stop("register-and-compat", err)
		return
	}
	suffix := name(env, "sr")
	r := &srRun{env: env, dir: dir, net: containerName(env, "sr", "net"), name: containerName(env, "sr", "registry"),
		store: "_schemas_" + strings.ReplaceAll(suffix, "-", "_")}
	rmNet, err := box.Network(ctx, r.net)
	if err != nil {
		s.stop("register-and-compat", err)
		return
	}
	defer rmNet()
	defer r.cleanup()
	if r.client, err = box.Start(ctx, containerName(env, "sr", "http"), []string{"--network", r.net, "--entrypoint", "sleep"}, box.Image(), "3600"); err != nil {
		s.stop("register-and-compat", err)
		return
	}
	if err := r.startRegistry(ctx); err != nil {
		s.stop("register-and-compat", err)
		return
	}
	subject := suffix + "-value"
	o := Observation{"registry_answers": r.answers(ctx, 180*time.Second)}
	v1, err1 := r.post(ctx, "/subjects/"+subject+"/versions", srV1)
	v2, err2 := r.post(ctx, "/subjects/"+subject+"/versions", srV2)
	bad, err3 := r.post(ctx, "/compatibility/subjects/"+subject+"/versions/latest", srBad)
	o["v1_registered"] = err1 == nil && v1["id"] != nil
	o["v2_registered"] = err2 == nil && v2["id"] != nil
	o["ids_distinct"] = err1 == nil && err2 == nil && fmt.Sprint(v1["id"]) != fmt.Sprint(v2["id"])
	o["incompatible_rejected"] = err3 == nil && bad["is_compatible"] == false
	d := kout.ParseTopicDescribe(env.tool(ctx, "", "kafka-topics.sh", env.bs("--describe", "--topic", r.store)...).Stdout, r.store)
	o["store_topic_partitions"] = d.PartitionCount
	o["store_topic_cleanup_policy"] = kout.ParseConfigsDescribe(env.tool(ctx, "", "kafka-configs.sh",
		env.bs("--describe", "--all", "--entity-type", "topics", "--entity-name", r.store)...).Stdout)["cleanup.policy"]
	s.set("register-and-compat", o)

	// Restart: a fresh container with the same store topic must rebuild both versions from it.
	if err := r.reg.Kill(ctx, "TERM"); err != nil {
		s.stop("restart-rebuild", err)
		return
	}
	if !r.reg.Gone(ctx, 60*time.Second) {
		s.stop("restart-rebuild", fmt.Errorf("registry %s did not stop within 60 s", r.name))
		return
	}
	r.reg = nil
	if err := r.startRegistry(ctx); err != nil {
		s.stop("restart-rebuild", err)
		return
	}
	o = Observation{"registry_answers_after_restart": r.answers(ctx, 180*time.Second)}
	versions, _ := r.call(ctx, "/subjects/"+subject+"/versions", "")
	o["versions_after_restart"] = strings.TrimSpace(versions)
	id2 := fmt.Sprint(v2["id"])
	if f, ok := v2["id"].(float64); ok {
		id2 = fmt.Sprintf("%d", int64(f))
	}
	got, _ := r.call(ctx, "/schemas/ids/"+id2, "")
	var sch struct {
		Schema string `json:"schema"`
	}
	o["schema_v2_reads_back_identical"] = json.Unmarshal([]byte(got), &sch) == nil && canonicalJSON(sch.Schema) == canonicalJSON(srV2)
	banner := false
	for _, l := range r.logs {
		if clientVersionsInLog(l) != "" {
			banner = true
		}
	}
	o["registry_logs_kafka_clients_banner"] = banner
	s.set("restart-rebuild", o)
}

func canonicalJSON(s string) string {
	var v any
	if json.Unmarshal([]byte(s), &v) != nil {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}
