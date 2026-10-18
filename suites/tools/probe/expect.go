package probe

// Oracle expectations: what real Apache Kafka 4.3.1 must show for each probe. A row whose oracle
// leg misses its expectation is VOID (the instrument or the oracle is broken), never compared:
// two equally broken legs must not make a PASS. Every probe declares one; a probe without one
// makes its row VOID. Values are compared as JSON, so 3 and 3.0 are the same.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Expectations maps a probe name to the observation fields the oracle must show.
var Expectations = map[string]Observation{
	"cli-topics-lifecycle": {"create_exit": 0, "describe_partitions": 3, "describe_replication_factor": 1, "describe_all_partitions_led": true,
		"describe_shows_retention_ms": "86400000", "listed_after_create": true, "create_duplicate_exit": 1, "create_duplicate_exception": "TopicExistsException",
		"alter_increase_exit": 0, "partitions_after_increase": 5, "all_led_after_increase": true, "alter_decrease_exit": 1,
		"alter_decrease_exception": "InvalidPartitionsException", "describe_missing_exit": 1, "delete_exit": 0, "gone_from_list_after_delete": true},
	"cli-console-roundtrip": {"producer_exit": 0, "consumer_processed_total": 120, "consumer_malformed_lines": 0, "roundtrip_produced": 120,
		"roundtrip_consumed": 120, "roundtrip_missing": 0, "roundtrip_duplicates": 0, "roundtrip_unexpected": 0, "roundtrip_out_of_order": 0,
		"roundtrip_offsets_from_zero": true, "roundtrip_keys_single_partition": true, "roundtrip_content_matches": true},
	"cli-groups-describe": {"consumer_processed_total": 60, "listed": true, "describe_exit": 0, "describe_partitions": 3, "describe_topic_matches": true,
		"committed_sum": 60, "all_partitions_lag_zero": true, "says_no_active_members": true, "state_parsed": true, "state": "Empty", "state_members": 0},
	"cli-groups-reset": {"committed_before": 60, "committed_after_dry_run": 60, "committed_after_to_earliest": "0=0 1=0 2=0",
		"committed_after_to_offset_4": "0=4 1=4 2=4", "committed_after_shift_by_3": "0=7 1=7 2=7", "committed_after_shift_by_minus_2": "0=5 1=5 2=5",
		"committed_after_to_latest": "0=20 1=20 2=20", "to_datetime_equals_first_batch_end": true, "committed_after_to_datetime": 30,
		"to_future_datetime_new_offsets": "0=20 1=20 2=20"},
	"cli-groups-active": {"holder_seen_stable_one_member": true, "reset_active_refused_inactive_required": true, "committed_after_refused_reset": 30,
		"delete_active_exception": "GroupNotEmptyException", "listed_after_refused_delete": true, "still_stable_after_refusals": true},
	"cli-groups-delete": {"delete_exit": 0, "delete_says_successful": true, "gone_from_list": true,
		"describe_deleted_exception": "GroupIdNotFoundException", "delete_again_exception": "GroupIdNotFoundException"},
	"cli-configs-topic": {"dynamic_configs_at_create": 0, "add_exit": 0, "add_completed": true, "after_add_retention_ms": "3600000",
		"after_add_max_message_bytes": "500000", "delete_exit": 0, "after_delete_retention_ms_present": false, "after_delete_max_message_bytes": "500000",
		"unknown_name_exit": 1, "unknown_name_exception": "InvalidConfigurationException", "invalid_value_exit": 1,
		"invalid_value_exception": "InvalidConfigurationException", "describe_all_exit": 0, "describe_all_shows_cleanup_policy_delete": true},
	"cli-get-offsets": {"latest_exit": 0, "all_partitions_latest_sum": 10, "earliest": "0=0 1=0", "max_timestamp_offsets_inside_log": true,
		"by_old_timestamp": "0=0 1=0", "by_future_timestamp": ""},
	"cli-delete-records": {"delete_to_7_exit": 0, "delete_to_7_low_watermark_reported": true, "earliest_after": "0=7", "latest_after": "0=20",
		"consumed_after": 13, "first_offset_after": 7, "first_value_after": "v-0007", "delete_beyond_end_exception": "OffsetOutOfRangeException"},
	"cli-verifiable": {"producer_exit": 0, "consumer_exit": 0, "check_producer_acked": 300, "check_success_events": 300, "check_distinct_ack_offsets": true,
		"check_ack_offsets_dense": true, "check_consumed": 300, "check_consumed_covers_acks": true, "check_commits_succeeded": true, "check_final_commit_at_end": true, "check_records_exactly_acked": true},
	"cli-perf-test":           {"producer_exit": 0, "producer_records_sent": 20000, "consumer_exit": 0, "consumer_records": 20000},
	"cli-broker-api-versions": {"exit": 0, "brokers_listed": 1, "unparseable_lines": 0},
	"cli-leader-election":     {"preferred_exit": 0, "preferred_says_already_elected": true, "unclean_exit": 0, "unclean_says_already_elected": true},
	"cli-metadata-quorum": {"describe_status_exit": 0, "describe_status_shows_leader_id": true, "describe_status_shows_cluster_id": true,
		"describe_replication_exit": 0, "describe_replication_shows_leader_row": true},
	"cli-transactions": {"list_exit": 0, "list_prints_header": true, "describe_producers_exit": 0, "describe_producers_prints_header": true,
		"find_hanging_exit": 0, "find_hanging_prints_header": true, "describe_unknown_id_says_unsupported": false},
	"cli-reassign-partitions": {"generate_exit": 0, "generate_prints_proposal": true, "list_exit": 0, "list_says_none": true,
		"execute_exit": 0, "execute_says_started": true, "verify_exit": 0, "verify_says_complete": true},
	"cli-log-dirs": {"describe_exit": 0, "describe_prints_json": true, "describe_lists_topic_partition": true},
	"cli-features": {"describe_exit": 0, "describe_lists_metadata_version": true, "describe_lists_group_version": true,
		"describe_lists_transaction_version": true, "upgrade_dry_run_exit": 0},
	"cli-delegation-tokens": {"describe_exception": "UnsupportedByAuthenticationException", "create_exception": "UnsupportedByAuthenticationException"},
	"cli-configs-broker": {"describe_broker_exit": 0, "describe_broker_prints_header": true, "describe_broker_all_shows_message_max_bytes": true,
		"describe_broker_all_shows_num_partitions": true, "describe_broker_loggers_lists_root_logger": true, "alter_broker_exit": 0,
		"alter_broker_says_completed": true, "after_alter_dynamic_message_max_bytes": "1048588", "alter_broker_delete_exit": 0},
	"cli-cluster-id":        {"cluster_id_exit": 0, "cluster_id_prints_cluster_id": true, "cluster_id_has_kafka_uuid_form": true},
	"cli-unregister-broker": {"unregister_unknown_exit": 1, "unregister_unknown_exception": "BrokerIdNotRegisteredException"},

	"connect-rf-refusal": {"worker_outcome": "gave-up", "refused_creation_retried_at_least_2": true, "every_refusal_names_replication": true,
		"gave_up_with_creation_timeout": true, "internal_topics_created": 0},
	"connect-internal-topics": {"configs_exists": true, "configs_partitions": 1, "configs_replication_factor": 1, "configs_cleanup_policy": "compact",
		"offsets_exists": true, "offsets_partitions": 25, "offsets_replication_factor": 1, "offsets_cleanup_policy": "compact",
		"status_exists": true, "status_partitions": 5, "status_replication_factor": 1, "status_cleanup_policy": "compact", "worker_rest_version": "4.3.1"},
	"connect-file-roundtrip":   {"sink_expected": 200, "sink_identical": true, "topic_records_equal_lines": true},
	"connect-second-worker":    {"work_moved_to_second_worker": true, "sink_expected": 300, "sink_identical": true},
	"connect-graceful-restart": {"sink_expected": 400, "sink_identical": true},
	"connect-sigkill-sink":     {"sink_expected": 500, "sink_missing": 0, "sink_unexpected": 0, "sink_duplicates": 0, "sink_order_violations": 0},
	"connect-final":            {"topic_records_equal_lines": true, "sink_group_committed_at_end": true, "workers_loaded_kafka_clients": "4.3.1"},

	"ops-consumer-offsets-topic": {"offsets_topic_listed": true, "offsets_topic_readable": true, "group_commits_found": true,
		"group_commits_match_committed": true, "group_committed_partitions_describe": 3},
	"ops-transaction-state-topic": {"transactional_records_sent": 100, "transaction_state_listed": true, "transaction_state_readable": true,
		"transactional_id_record_found": true},

	// Burrow v1.9.4 cannot decode Kafka 4.x offset-commit records, so on Kafka itself it lists the
	// group but has no lag for it (see lagtools.go); lag availability is informational (below).
	"lag-burrow":             {"api_answers": true, "cluster_listed": true, "topic_end_offsets_match": true, "group_listed": true},
	"lag-kafka-lag-exporter": {"metrics_page_answers": true, "group_lag_series_present": true, "lag_equals_kafka_consumer_groups_per_partition": true},
	"lag-kafbat-ui": {"api_answers": true, "cluster_status": "ONLINE", "cluster_broker_count": 1, "brokers_listed": 1, "topic_listed": true,
		"topic_partitions": 3, "topic_messages_count": 60, "group_listed": true, "group_state": "EMPTY", "group_lag_equals_kafka_consumer_groups": true},

	"streams-topology-topics": {"repartition_exists": true, "repartition_partitions": 3, "repartition_replication_factor": 1, "repartition_cleanup_policy": "delete",
		"changelog_exists": true, "changelog_partitions": 3, "changelog_replication_factor": 1, "changelog_cleanup_policy": "compact"},
	"streams-word-counts":        {"counts_words": 40, "counts_wrong_final": 0, "counts_sequence_violations": 0, "counts_unexpected_words": 0},
	"streams-exactly-once":       {"kill_landed_mid_flight": true, "counts_words": 40, "counts_wrong_final": 0, "counts_sequence_violations": 0, "counts_unexpected_words": 0},
	"streams-continued-counting": {"counts_words": 40, "counts_wrong_final": 0, "counts_sequence_violations": 0, "counts_unexpected_words": 0, "apps_loaded_kafka_clients": "4.3.1"},
}

func init() {
	for _, dir := range []string{"into", "from"} {
		Expectations["mm2-"+dir+"-mirror"] = Observation{"remote_topic_complete": true, "remote_topic_exists": true, "remote_partitions_equal_source": true,
			"end_offsets_equal_per_partition": true, "remote_drain_complete": true, "records_identical": true, "remote_topic_has_topic_id": true,
			"source_holds_what_was_produced": true}
		Expectations["mm2-"+dir+"-internal"] = Observation{"offset_syncs_topic_exists": true, "offset_syncs_cleanup_policy": "compact", "offset_syncs_not_empty": true,
			"checkpoints_exists": true, "checkpoints_cleanup_policy": "compact", "heartbeats_exists": true, "heartbeats_written_by_this_run": true,
			"mm2_configs_exists": true, "mm2_configs_cleanup_policy": "compact", "mm2_offsets_exists": true, "mm2_offsets_cleanup_policy": "compact",
			"mm2_status_exists": true, "mm2_status_cleanup_policy": "compact"}
		Expectations["mm2-"+dir+"-group-offsets"] = Observation{"source_group_committed_at_end": true, "translated_exactly": true, "partitions_missing": 0, "partitions_ahead": 0,
			"partitions_behind": 0, "mirror_maker_loaded_kafka_clients": "4.3.1"}
	}
}

// Informational maps a probe name to observation fields that are recorded in both legs but never
// compared and never required of the oracle. A field belongs here only when its value says which
// format a broker writes rather than whether the broker behaves as Kafka does.
var Informational = map[string][]string{
	// Burrow v1.9.4 decodes offset-commit values up to schema v3. Kafka 4.3.1 writes v4, so on Kafka
	// itself Burrow has no lag for the group; KubeMQ's __consumer_offsets projects v3, so Burrow has
	// lag there. The row compares group_listed (and the cluster and topic fields); lag availability
	// is reported, not compared.
	"lag-burrow": {"group_lag_available"},
}

// Compared returns obs without the probe's informational fields: what the row compares. obs is
// not modified; a probe with no informational fields gets obs back.
func Compared(probe string, obs Observation) Observation {
	skip := Informational[probe]
	if len(skip) == 0 || obs == nil {
		return obs
	}
	out := make(Observation, len(obs))
	for k, v := range obs {
		out[k] = v
	}
	for _, k := range skip {
		delete(out, k)
	}
	return out
}

// InformationalNote describes the probe's informational fields as each leg observed them, for the
// row detail; empty when the probe has none.
func InformationalNote(probe string, oracle, kubemq Observation) string {
	var parts []string
	for _, k := range Informational[probe] {
		parts = append(parts, fmt.Sprintf("%s: Kafka %s, KubeMQ %s", k, infoValue(oracle, k), infoValue(kubemq, k)))
	}
	if len(parts) == 0 {
		return ""
	}
	return "informational, not compared: " + strings.Join(parts, "; ")
}

func infoValue(obs Observation, k string) string {
	v, ok := obs[k]
	if !ok {
		return "absent"
	}
	return js(v)
}

// CheckOracle returns an error naming every expected field the oracle's observation misses.
func CheckOracle(probe string, obs Observation) error {
	want, ok := Expectations[probe]
	if !ok {
		return fmt.Errorf("probe %q declares no oracle expectation", probe)
	}
	var bad []string
	for k, v := range want {
		got, present := obs[k]
		if !present {
			bad = append(bad, k+" absent")
			continue
		}
		if js(got) != js(v) {
			bad = append(bad, fmt.Sprintf("%s=%s, expected %s", k, js(got), js(v)))
		}
	}
	if len(bad) == 0 {
		return nil
	}
	sort.Strings(bad)
	return fmt.Errorf("real Kafka did not show the documented behaviour: %s", strings.Join(bad, "; "))
}

func js(v any) string {
	b, _ := json.Marshal(v)
	var x any
	if json.Unmarshal(b, &x) == nil {
		b, _ = json.Marshal(x)
	}
	return string(b)
}
