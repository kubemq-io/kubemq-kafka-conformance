package probe

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Oracle expectations. A probe proves something only if the Kafka oracle shows the documented
// Kafka behaviour; otherwise two brokers that both fail to show a feature would compare equal
// and read PASS. Every registered probe declares what the oracle must show. When the oracle's
// observation does not satisfy it, the probe itself is not measuring anything and the row is
// VOID (docs/methodology.md, "Oracle preconditions").

// Expectation checks an oracle observation and explains the first violation.
type Expectation func(Observation) error

// Expectations maps probe names to what the oracle must show.
var Expectations = map[string]Expectation{}

func expect(name string, e Expectation) { Expectations[name] = e }

// CheckOracle applies the probe's expectation to an oracle observation. A probe without a
// registered expectation is reported, so none can be added without one.
func CheckOracle(name string, obs Observation) error {
	e, ok := Expectations[name]
	if !ok {
		return fmt.Errorf("probe %q declares no oracle expectation", name)
	}
	return e(obs)
}

// Fields requires exactly these field values (JSON-compared) in the observation.
func Fields(want Observation) Expectation {
	return func(obs Observation) error {
		keys := make([]string, 0, len(want))
		for k := range want {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			got, ok := obs[k]
			wj, _ := json.Marshal(want[k])
			if !ok {
				return fmt.Errorf("oracle observation lacks %s (expected %s)", k, wj)
			}
			gj, _ := json.Marshal(got)
			if string(gj) != string(wj) {
				return fmt.Errorf("oracle shows %s = %s, documented Kafka behaviour is %s", k, gj, wj)
			}
		}
		return nil
	}
}

// NoFailedRequests requires that no field value reports a failed request.
func NoFailedRequests() Expectation {
	return func(obs Observation) error {
		for k, v := range obs {
			if s, ok := v.(string); ok && (strings.HasPrefix(s, "request error") || s == "malformed" || s == "absent") {
				return fmt.Errorf("oracle request for %s failed (%s)", k, s)
			}
		}
		return nil
	}
}

// All combines expectations.
func All(es ...Expectation) Expectation {
	return func(obs Observation) error {
		for _, e := range es {
			if err := e(obs); err != nil {
				return err
			}
		}
		return nil
	}
}

func roundTripClean(n int) Observation {
	return Observation{"produce_error_code": 0, "produced": n, "consumed": n, "missing": 0, "duplicates": 0,
		"out_of_order": 0, "offsets_from_zero": true, "content_matches": true}
}

func init() {
	// Produce
	for _, n := range []string{"produce-acks-0", "produce-acks-1", "produce-acks-all"} {
		expect(n, Fields(roundTripClean(300)))
	}
	for _, c := range []string{"gzip", "snappy", "lz4", "zstd"} {
		o := roundTripClean(200)
		o["stored_codecs"] = []string{c}
		expect("produce-codec-"+c, Fields(o))
	}
	rc := roundTripClean(200)
	rc["stored_codecs"] = []string{"gzip"}
	expect("produce-broker-recompression-gzip", Fields(rc))
	expect("produce-headers", Fields(roundTripClean(60)))
	expect("produce-default-partitioner", Fields(Observation{"keyed_records": 300, "unkeyed_records": 300, "consumed": 600,
		"keyed_partition_mismatches": 0, "consumed_keyed_partition_mismatches": 0}))
	expect("produce-max-message-bytes", Fields(Observation{"limited_5KiB_error_code": 0, "limited_20KiB_error_code": 10, "default_1200KiB_error_code": 10}))
	// Log semantics
	expect("log-timestamp-createtime", Fields(Observation{"consumed": 20, "timestamps_as_sent": 20, "timestamp_types": []string{"CreateTime"}}))
	expect("log-timestamp-logappendtime", Fields(Observation{"create_error_code": 0, "consumed": 10, "timestamps_at_append_time": 10,
		"producer_timestamps_kept": 0, "timestamp_types": []string{"LogAppendTime"}}))
	expect("log-timestamp-validation", Fields(Observation{"strict_now_error_code": 0, "strict_2h_past_error_code": 32,
		"strict_2h_future_error_code": 32, "default_2h_future_error_code": 32, "default_30d_past_error_code": 0}))
	expect("log-listoffsets-specs", Fields(Observation{"earliest": 0, "latest": 10, "by_timestamp_t0_plus_5_5s": 4,
		"by_timestamp_after_all": 10, "max_timestamp_error_code": 0, "max_timestamp_offset": 4, "max_timestamp_is_largest": true}))
	compacted := Observation{"compacted_within_120s": true, "latest_value_per_key_kept": true, "tombstone_removed_within_120s": true}
	expect("log-compaction", Fields(compacted))
	expect("log-compaction-batched-compressed", Fields(compacted))
	javaCompacted := Observation{"java_consumer_ended_cleanly": true, "java_exceptions": []string{}, "java_records_read": 9,
		"java_latest_value_per_key": true, "java_tombstoned_key_absent": true}
	for k, v := range compacted {
		javaCompacted[k] = v
	}
	for _, c := range []string{"gzip", "snappy", "lz4", "zstd"} {
		expect("log-compaction-java-"+c, Fields(javaCompacted))
	}
	expect("log-compaction-dirty-ratio", Fields(Observation{"first_cleaning_pass_within_120s": true, "raise_ratio_error_code": 0, "small_dirty_share_left_uncompacted_for_45s": true}))
	expect("log-compaction-max-lag", Fields(Observation{"first_cleaning_pass_within_120s": true, "raise_ratio_error_code": 0, "small_dirty_share_compacted_by_lag_within_60s": true}))
	expect("log-retention-ms", Fields(Observation{"log_start_advanced_past_first_records_within_120s": true}))
	expect("log-retention-ms-zero", Fields(Observation{"create_error_code": 0, "retention_ms_described": "value=0 source=DYNAMIC_TOPIC_CONFIG",
		"log_start_advanced_past_first_records_within_120s": true, "newest_record_still_in_log": true}))
	expect("log-retention-bytes", Fields(Observation{"create_error_code": 0, "log_start_advanced_within_120s": true}))
	expect("log-retention-bytes-compact-delete", Fields(Observation{"create_error_code": 0, "log_start_advanced_within_120s": true}))
	// Fetch
	expect("fetch-sessions", Fields(Observation{"full_fetch_error_code": 0, "session_created": true, "incremental_fetch_error_code": 0, "unknown_session_error_code": 70}))
	expect("fetch-kip74-oversized-first-record", Fields(Observation{"error_code": 0, "first_batch_returned": true, "second_record_returned": false}))
	expect("fetch-long-poll", Fields(Observation{"empty_poll_waited_at_least_1500ms": true, "empty_poll_returned_within_4000ms": true,
		"data_answered_before_max_wait": true, "data_returned": true}))
	// Consumer groups
	for _, n := range []string{"range", "roundrobin", "sticky", "cooperative-sticky"} {
		o := Observation{"single_member_got_all_6": true, "two_members_balanced_3_and_3": true, "partition_counts": []int{3, 3}}
		if n == "range" {
			o["each_member_contiguous"] = true
		}
		if n == "roundrobin" {
			o["each_member_alternating"] = true
		}
		expect("groups-assignor-"+n, Fields(o))
	}
	expect("groups-rebalance-join-leave-crash", Fields(Observation{"first_member_got_all_6": true, "join_rebalanced_3_and_3": true,
		"leave_reassigned_all_6": true, "leave_reassigned_within_4s": true, "second_join_rebalanced_3_and_3": true,
		"crash_reassigned_all_6_within_30s": true, "crash_reassignment_waited_for_session_timeout": true}))
	expect("groups-session-timeout-bounds", Fields(Observation{"session_1s_error_code": 26, "session_10s_error_code": 0, "session_2h_error_code": 26}))
	expect("groups-rebalance-timeout-expiry", Fields(Observation{"member1_join_error_code": 0, "member2_join_error_code": 0,
		"member2_join_completed_after_member1_rebalance_timeout": true, "member2_join_completed_within_15s": true,
		"member2_is_only_member": true, "member1_heartbeat_error_code": 25}))
	expect("groups-static-membership", Fields(Observation{"restart_kept_generation": true, "leader_heartbeat_error_code": 0,
		"replaced_member_id_heartbeat_error_code": 82, "members_share_generation": true}))
	expect("groups-offset-reset-policies", Fields(Observation{"earliest_first_record": "old-0", "latest_first_record": "new"}))
	// Offsets
	expect("offsets-commit-fetch", Fields(Observation{"commit_error_code": 0, "p0": `offset=2 metadata="meta-p0" error=0`,
		"p1": `offset=3 metadata="" error=0`, "p2": `offset=-1 metadata="" error=0`}))
	expect("offsets-commit-generation-checks", Fields(Observation{"member_commit_error_code": 0, "stale_generation_error_code": 22, "unknown_member_error_code": 25}))
	expect("offsets-multi-group-fetch", Fields(Observation{"group1": "offset=1 error=0", "group2": "offset=2 error=0", "group3": "offset=3 error=0"}))
	expect("offsets-delete-records", Fields(Observation{"delete_records_error_code": 0, "low_watermark": 5, "earliest": 5, "fetch_from_0_error_code": 1}))
	expect("offsets-retention", Fields(Observation{"empty_group_offset_expired_within_180s": true, "active_group_offset_kept": true}))
	expect("fetch-min-bytes", Fields(Observation{"small_fetch_waited_at_least_400ms": true, "small_fetch_returned_data": true,
		"large_fetch_answered_within_400ms": true, "large_fetch_returned_at_least_64KiB": true}))
	expect("offsets-delete-group-offsets", Fields(Observation{"delete_error_code": 0, "delete_partition_error_code": 0, "offset_after_delete": -1}))
	// Admin
	expect("admin-topics", Fields(Observation{"create_error_code": 0, "duplicate_create_error_code": 36, "validate_only_error_code": 0,
		"validate_only_created_topic": false, "zero_partitions_error_code": 37, "rf_above_broker_count_error_code": 38,
		"delete_error_code": 0, "deleted_topic_gone_within_10s": true, "delete_unknown_error_code": 3}))
	expect("admin-partitions", Fields(Observation{"increase_to_4_error_code": 0, "partitions_after_increase": 4,
		"produce_to_new_partition_error_code": 0, "same_count_error_code": 37, "decrease_error_code": 37}))
	expect("admin-partition-limit", Fields(Observation{"create_256_partitions_error_code": 0, "create_300_partitions_error_code": 0}))
	expect("admin-configs", Fields(Observation{"retention_ms_after_create": "value=3600000 source=DYNAMIC_TOPIC_CONFIG",
		"set_cleanup_policy_error_code": 0, "cleanup_policy_after_set": "value=compact source=DYNAMIC_TOPIC_CONFIG",
		"delete_retention_ms_error_code": 0, "retention_ms_after_delete": "value=604800000 source=DEFAULT_CONFIG"}))
	expect("admin-groups", Fields(Observation{"member_assigned": true, "list_contains_group": true, "describe_state": "Stable",
		"describe_protocol_type": "consumer", "describe_protocol": "range", "describe_members": 1, "delete_non_empty_error_code": 68,
		"state_after_member_left": "Empty", "delete_empty_error_code": 0, "list_contains_group_after_delete": false}))
	expect("admin-long-topic-names", Fields(Observation{"create_96_chars_error_code": 0, "create_249_chars_error_code": 0,
		"create_250_chars_error_code": 17, "broker_answers_after_96_chars": true, "broker_answers_after_249_chars": true,
		"broker_answers_after_250_chars": true}))
	// Wire
	expect("wire-apiversions-downgrade", Fields(Observation{"v0_error_code": 0, "above_max_error_code": 35, "above_max_body_is_v0": true,
		"above_max_lists_own_max": true, "above_max_correlation_echoed": true, "connection_open_after_unsupported": true,
		"at_max_error_code": 0, "at_max_correlation_echoed": true, "v3_error_code": 0}))
	expect("wire-listoffsets-in-batch", All(NoFailedRequests(), Fields(Observation{
		"gzip_before_ts_2500": "offset 2 at 3000", "gzip_before_ts_4500": "offset 3 at 5000", "gzip_before_ts_-3": "offset 5 at 6000"})))
	expect("wire-listoffsets-layouts", All(NoFailedRequests(), Fields(Observation{"belowstart_after_delete_to_2_ts_-3": "offset 1 at 9000"})))
	// After the abort, a by-timestamp lookup at 2000 must still land on the aborted record at
	// offset 1 (timestamp 5000), under both isolation levels. Kafka 4.3.1 once answered the abort
	// marker instead (offset 2, gate run on kafka-cert-w1, 2026-10-07; three reruns answered
	// offset 1), so the oracle is held to the documented answer and a deviating oracle is VOID.
	expect("wire-listoffsets-transactions", All(NoFailedRequests(), Fields(Observation{
		"open_committed_ts_2000": "offset -1 at -1", "open_uncommitted_ts_2000": "offset 1 at 5000",
		"aborted_committed_ts_2000": "offset 1 at 5000", "aborted_uncommitted_ts_2000": "offset 1 at 5000"})))
	expect("wire-out-of-range-reset", Fields(Observation{"default_reset_resumes_at": 0, "reset_after_milli_T_resumes_at": 10}))
	expect("wire-topic-ids", Fields(Observation{"topic_id_present": true, "topic_id_stable": true, "recreated": true,
		"recreated_topic_has_new_id": true, "delete_by_id_error_code": 0, "deleted_by_id_gone_within_10s": true}))
	expect("wire-error-codes", Fields(Observation{"fetch_negative_offset_error_code": 1, "fetch_past_end_error_code": 1,
		"fetch_unknown_partition_error_code": 3, "listoffsets_unknown_partition_error_code": 3, "create_invalid_name_error_code": 17,
		"create_dot_name_error_code": 17, "join_empty_group_id_error_code": 24, "unknown_api_key": "closed", "produce_version_99": "closed"}))
}
