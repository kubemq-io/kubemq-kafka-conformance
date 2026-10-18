package probe

// Kafka CLI rows for administration tools whose requests are broker- or cluster-level: leader
// election, KRaft quorum, transactions inspection, partition reassignment, log directories,
// feature levels, delegation tokens, broker configs and cluster registration. KubeMQ does not
// advertise most of these requests (docs/gaps.md, apikey.* entries); these rows record what each
// tool actually does for a user on both brokers. Steps follow the Kafka CLI admin-tools sweep of
// kubemq-server tests/kafka-conformance.
//
// Nothing here changes the shared oracle's behaviour for other rows: broker config changes are
// set to the broker's own default value and removed again, feature upgrades are dry runs, and
// reassignments keep every partition on the broker it is on.

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/box"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
)

func init() {
	register("cli-leader-election", cliLeaderElection)
	register("cli-metadata-quorum", cliMetadataQuorum)
	register("cli-transactions", cliTransactions)
	register("cli-reassign-partitions", cliReassignPartitions)
	register("cli-log-dirs", cliLogDirs)
	register("cli-features", cliFeatures)
	register("cli-delegation-tokens", cliDelegationTokens)
	register("cli-configs-broker", cliConfigsBroker)
	register("cli-cluster-id", cliClusterID)
	register("cli-unregister-broker", cliUnregisterBroker)
}

// step records one tool invocation as <key>_exit, <key>_exception, <key>_says_unsupported (the
// tool reports that the broker does not support the request) and, for each marker,
// <key>_<markerName> = whether the output contains its text.
func step(obs Observation, key string, r box.Result, markers map[string]string) {
	obs[key+"_exit"] = r.Exit
	obs[key+"_exception"] = box.Exception(r)
	obs[key+"_says_unsupported"] = strings.Contains(r.Combined(), "does not support")
	for name, text := range markers {
		obs[key+"_"+name] = strings.Contains(r.Combined(), text)
	}
}

var brokerIDPattern = regexp.MustCompile(`(?m)^\S+:\d+ \(id: (-?\d+) rack: `)

// brokerID is the node id the broker reports about itself (identifier, used for arguments only).
func (e Env) brokerID(ctx context.Context) (string, error) {
	r := e.tool(ctx, "", "kafka-broker-api-versions.sh", e.bs()...)
	m := brokerIDPattern.FindStringSubmatch(r.Stdout)
	if m == nil {
		return "", fmt.Errorf("broker id: no broker block in kafka-broker-api-versions.sh output: %s", firstErrorLine(r.Combined()))
	}
	return m[1], nil
}

func cliLeaderElection(ctx context.Context, env Env) (Observation, error) {
	t := name(env, "elect")
	if err := env.createTopic(ctx, t, 1); err != nil {
		return nil, err
	}
	obs := Observation{}
	step(obs, "preferred", env.tool(ctx, "", "kafka-leader-election.sh", env.bs("--election-type", "preferred", "--topic", t, "--partition", "0")...),
		map[string]string{"says_already_elected": "Valid replica already elected for partitions"})
	step(obs, "unclean", env.tool(ctx, "", "kafka-leader-election.sh", env.bs("--election-type", "unclean", "--topic", t, "--partition", "0")...),
		map[string]string{"says_already_elected": "Valid replica already elected for partitions"})
	return obs, nil
}

func cliMetadataQuorum(ctx context.Context, env Env) (Observation, error) {
	obs := Observation{}
	step(obs, "describe_status", env.tool(ctx, "", "kafka-metadata-quorum.sh", env.bs("describe", "--status")...),
		map[string]string{"shows_leader_id": "LeaderId:", "shows_cluster_id": "ClusterId:"})
	step(obs, "describe_replication", env.tool(ctx, "", "kafka-metadata-quorum.sh", env.bs("describe", "--replication")...),
		map[string]string{"shows_leader_row": "Leader"})
	return obs, nil
}

func cliTransactions(ctx context.Context, env Env) (Observation, error) {
	t := name(env, "txtool")
	if err := env.createTopic(ctx, t, 1); err != nil {
		return nil, err
	}
	if err := env.produceKeyed(ctx, t, keyedLines(0, 5, 1, "v")); err != nil {
		return nil, err
	}
	obs := Observation{}
	step(obs, "list", env.tool(ctx, "", "kafka-transactions.sh", env.bs("list")...),
		map[string]string{"prints_header": "TransactionalId"})
	step(obs, "describe_unknown_id", env.tool(ctx, "", "kafka-transactions.sh", env.bs("describe", "--transactional-id", name(env, "notx"))...), nil)
	step(obs, "describe_producers", env.tool(ctx, "", "kafka-transactions.sh", env.bs("describe-producers", "--topic", t, "--partition", "0")...),
		map[string]string{"prints_header": "ProducerId"})
	id, err := env.brokerID(ctx)
	if err != nil {
		return nil, err
	}
	step(obs, "find_hanging", env.tool(ctx, "", "kafka-transactions.sh", env.bs("find-hanging", "--broker-id", id)...),
		map[string]string{"prints_header": "Topic"})
	return obs, nil
}

func cliReassignPartitions(ctx context.Context, env Env) (Observation, error) {
	t := name(env, "reassign")
	if err := env.createTopic(ctx, t, 2); err != nil {
		return nil, err
	}
	id, err := env.brokerID(ctx)
	if err != nil {
		return nil, err
	}
	obs := Observation{}
	move := "/tmp/" + t + "-move.json"
	plan := "/tmp/" + t + "-plan.json"
	if err := env.Box.WriteFile(ctx, move, fmt.Sprintf(`{"topics":[{"topic":%q}],"version":1}`, t)); err != nil {
		return nil, err
	}
	// The same placement each partition already has: a no-op reassignment.
	if err := env.Box.WriteFile(ctx, plan, fmt.Sprintf(`{"version":1,"partitions":[{"topic":%q,"partition":0,"replicas":[%s]},{"topic":%q,"partition":1,"replicas":[%s]}]}`, t, id, t, id)); err != nil {
		return nil, err
	}
	step(obs, "generate", env.tool(ctx, "", "kafka-reassign-partitions.sh", env.bs("--generate", "--topics-to-move-json-file", move, "--broker-list", id)...),
		map[string]string{"prints_proposal": "Proposed partition reassignment configuration"})
	step(obs, "list", env.tool(ctx, "", "kafka-reassign-partitions.sh", env.bs("--list")...),
		map[string]string{"says_none": "No partition reassignments found."})
	step(obs, "execute", env.tool(ctx, "", "kafka-reassign-partitions.sh", env.bs("--execute", "--reassignment-json-file", plan)...),
		map[string]string{"says_started": "Successfully started partition reassignments"})
	step(obs, "verify", env.tool(ctx, "", "kafka-reassign-partitions.sh", env.bs("--verify", "--reassignment-json-file", plan)...),
		map[string]string{"says_complete": "is completed"})
	return obs, nil
}

func cliLogDirs(ctx context.Context, env Env) (Observation, error) {
	t := name(env, "logdirs")
	if err := env.createTopic(ctx, t, 1); err != nil {
		return nil, err
	}
	obs := Observation{}
	step(obs, "describe", env.tool(ctx, "", "kafka-log-dirs.sh", env.bs("--describe", "--topic-list", t)...),
		map[string]string{"prints_json": `"brokers":`, "lists_topic_partition": `"partition":"` + t + `-0"`})
	return obs, nil
}

func cliFeatures(ctx context.Context, env Env) (Observation, error) {
	obs := Observation{}
	step(obs, "describe", env.tool(ctx, "", "kafka-features.sh", env.bs("describe")...),
		map[string]string{"lists_metadata_version": "Feature: metadata.version", "lists_group_version": "Feature: group.version",
			"lists_transaction_version": "Feature: transaction.version"})
	step(obs, "upgrade_dry_run", env.tool(ctx, "", "kafka-features.sh", env.bs("upgrade", "--feature", "group.version=1", "--dry-run")...),
		map[string]string{"says_can_be_upgraded": "can be upgraded", "says_already": "is already"})
	return obs, nil
}

func cliDelegationTokens(ctx context.Context, env Env) (Observation, error) {
	// The tool insists on a client config file; the listener under test is plaintext.
	conf := "/tmp/" + name(env, "dtconf") + ".properties"
	if err := env.Box.WriteFile(ctx, conf, "security.protocol=PLAINTEXT\n"); err != nil {
		return nil, err
	}
	obs := Observation{}
	step(obs, "describe", env.tool(ctx, "", "kafka-delegation-tokens.sh", env.bs("--command-config", conf, "--describe")...), nil)
	step(obs, "create", env.tool(ctx, "", "kafka-delegation-tokens.sh", env.bs("--command-config", conf, "--create", "--max-life-time-period", "-1")...), nil)
	return obs, nil
}

func cliConfigsBroker(ctx context.Context, env Env) (Observation, error) {
	id, err := env.brokerID(ctx)
	if err != nil {
		return nil, err
	}
	obs := Observation{}
	cfg := func(args ...string) box.Result {
		return env.tool(ctx, "", "kafka-configs.sh", env.bs(args...)...)
	}
	r := cfg("--describe", "--entity-type", "brokers", "--entity-name", id)
	step(obs, "describe_broker", r, map[string]string{"prints_header": "Dynamic configs for broker " + id + " are:"})
	r = cfg("--describe", "--all", "--entity-type", "brokers", "--entity-name", id)
	step(obs, "describe_broker_all", r, nil)
	all := kout.ParseConfigsDescribe(r.Stdout)
	obs["describe_broker_all_shows_message_max_bytes"] = all["message.max.bytes"] != ""
	obs["describe_broker_all_shows_num_partitions"] = all["num.partitions"] != ""
	step(obs, "describe_broker_default", cfg("--describe", "--entity-type", "brokers", "--entity-default"),
		map[string]string{"prints_header": "Default configs for brokers in the cluster are:"})
	step(obs, "describe_broker_loggers", cfg("--describe", "--entity-type", "broker-loggers", "--entity-name", id),
		map[string]string{"lists_root_logger": "root="})
	// Set a dynamic broker config to Kafka's own default, then remove it: the oracle's behaviour
	// for every other row stays as it was.
	step(obs, "alter_broker", cfg("--alter", "--entity-type", "brokers", "--entity-name", id, "--add-config", "message.max.bytes=1048588"),
		map[string]string{"says_completed": "Completed updating config for broker " + id + "."})
	time.Sleep(time.Second)
	after := kout.ParseConfigsDescribe(cfg("--describe", "--entity-type", "brokers", "--entity-name", id).Stdout)
	obs["after_alter_dynamic_message_max_bytes"] = after["message.max.bytes"]
	step(obs, "alter_broker_delete", cfg("--alter", "--entity-type", "brokers", "--entity-name", id, "--delete-config", "message.max.bytes"),
		map[string]string{"says_completed": "Completed updating config for broker " + id + "."})
	return obs, nil
}

// kafkaClusterID matches Kafka's cluster id form: a random UUID in 22 characters of URL-safe
// base64, as kafka-storage.sh random-uuid prints it.
var kafkaClusterID = regexp.MustCompile(`(?m)^Cluster ID: [A-Za-z0-9_-]{22}$`)

func cliClusterID(ctx context.Context, env Env) (Observation, error) {
	obs := Observation{}
	r := env.tool(ctx, "", "kafka-cluster.sh", "cluster-id", "--bootstrap-server", env.Bootstrap)
	step(obs, "cluster_id", r, map[string]string{"prints_cluster_id": "Cluster ID: "})
	obs["cluster_id_has_kafka_uuid_form"] = kafkaClusterID.MatchString(r.Stdout)
	return obs, nil
}

func cliUnregisterBroker(ctx context.Context, env Env) (Observation, error) {
	obs := Observation{}
	step(obs, "unregister_unknown", env.tool(ctx, "", "kafka-cluster.sh", "unregister", "--bootstrap-server", env.Bootstrap, "--id", "99"),
		map[string]string{"says_api_unsupported": "does not support the broker unregistration API"})
	return obs, nil
}
