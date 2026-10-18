package probe

import "testing"

func TestEveryProbeDeclaresAnOracleExpectation(t *testing.T) {
	for name := range Registry {
		if _, ok := Expectations[name]; !ok {
			t.Errorf("probe %q has no oracle expectation", name)
		}
	}
	for name := range Expectations {
		if _, ok := Registry[name]; !ok {
			t.Errorf("expectation for unknown probe %q", name)
		}
	}
}

func TestMutation_CheckOracleFlagsEveryDeparture(t *testing.T) {
	good := Observation{"producer_exit": 0, "producer_records_sent": float64(20000), "consumer_exit": 0, "consumer_records": 20000}
	if err := CheckOracle("cli-perf-test", good); err != nil {
		t.Fatalf("control (3 and 3.0 equal): %v", err)
	}
	for name, obs := range map[string]Observation{
		"wrong count":      {"producer_exit": 0, "producer_records_sent": 19999, "consumer_exit": 0, "consumer_records": 20000},
		"wrong exit code":  {"producer_exit": 1, "producer_records_sent": 20000, "consumer_exit": 0, "consumer_records": 20000},
		"missing field":    {"producer_exit": 0, "producer_records_sent": 20000, "consumer_exit": 0},
		"wrong type value": {"producer_exit": "0", "producer_records_sent": 20000, "consumer_exit": 0, "consumer_records": 20000},
	} {
		if CheckOracle("cli-perf-test", obs) == nil {
			t.Errorf("%s: not flagged", name)
		}
	}
	if CheckOracle("no-such-probe", good) == nil {
		t.Error("a probe without an expectation must be flagged")
	}
}

func TestInformationalFieldsAreNeitherExpectedNorCompared(t *testing.T) {
	for name, fields := range Informational {
		if _, ok := Registry[name]; !ok {
			t.Errorf("informational fields for unknown probe %q", name)
		}
		for _, k := range fields {
			if _, ok := Expectations[name][k]; ok {
				t.Errorf("%s: %s is informational but the oracle is still required to show it", name, k)
			}
		}
	}
	obs := Observation{"group_listed": true, "group_lag_available": true}
	got := Compared("lag-burrow", obs)
	if _, ok := got["group_lag_available"]; ok {
		t.Error("lag-burrow: group_lag_available still compared")
	}
	if got["group_listed"] != true {
		t.Error("lag-burrow: group_listed must stay compared")
	}
	if _, ok := obs["group_lag_available"]; !ok {
		t.Error("Compared modified the leg's own observation")
	}
	if len(Compared("lag-kafbat-ui", Observation{"group_listed": true})) != 1 {
		t.Error("a probe without informational fields lost a field")
	}
}
