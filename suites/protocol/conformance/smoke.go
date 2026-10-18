package conformance

import (
	"fmt"
	"time"
)

// firstProduceStep and smokePhysicalTopic are shared by the smoke drivers. In kubemq-server they
// live in driver_kcat.go; the kcat driver is ported with the client track, so they live here.

func firstProduceStep(scenario Scenario) (Step, []Record, error) {
	for _, step := range scenario.Steps {
		if step.Action == ActionProduce {
			return step, step.Records, nil
		}
	}
	return Step{}, nil, fmt.Errorf("kafkaconformance: scenario %q has no Produce step for the smoke driver subset", scenario.Name)
}

// smokePhysicalTopic derives a per-run-unique physical topic name for a smoke driver's own
// isolated topic, so a smoke run never collides with the full-transcript driver's topic.
func smokePhysicalTopic(driverName, logicalTopic string) string {
	return fmt.Sprintf("%s-smoke-%s-%d", logicalTopic, driverName, time.Now().UnixNano())
}

// checkSmokeOffset: a smoke driver produces to a fresh one-partition topic and consumes from
// the start, so the record at position i must sit at offset i. Without this check a broker
// that answered with wrong or gapped offsets would pass a smoke row (found by the verifier
// mutation tests; the kubemq-server drivers do not check it).
func checkSmokeOffset(driver string, i int, got int64) error {
	if got != int64(i) {
		return fmt.Errorf("kafkaconformance: %s round-trip offset mismatch at position %d: consumed offset %d, want %d on a fresh one-partition topic", driver, i, got, i)
	}
	return nil
}
