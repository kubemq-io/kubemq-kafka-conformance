package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/apikeys"
)

type apiVersionsPair struct {
	oracle apikeys.Versions
	kubemq apikeys.Versions
}

func TestApiVersionsSettlerResetsForKeyAndFeatureChanges(t *testing.T) {
	feature := func(keyMax, finalized int16) apikeys.Versions {
		return apikeys.Versions{
			Keys: map[int16][2]int16{0: {0, keyMax}},
			Features: map[string]apikeys.Feature{
				"transaction.version": {SupportedMin: 0, SupportedMax: 2, Finalized: &finalized},
			},
		}
	}
	oracle := feature(2, 2)
	samples := []apiVersionsPair{
		{oracle: oracle, kubemq: feature(1, 1)},
		{oracle: oracle, kubemq: feature(2, 1)},
		{oracle: oracle, kubemq: feature(2, 2)},
		{oracle: oracle, kubemq: feature(2, 2)},
		{oracle: oracle, kubemq: feature(2, 2)},
		{oracle: oracle, kubemq: feature(2, 2)},
		{oracle: oracle, kubemq: feature(2, 2)},
		{oracle: oracle, kubemq: feature(2, 2)},
	}

	var now time.Time
	calls := 0
	settler := apiVersionsSettler{
		query: func(_ context.Context, addr string) (apikeys.Versions, error) {
			i := calls / 2
			calls++
			if i >= len(samples) {
				t.Fatalf("unexpected observation %d", i)
			}
			if addr == "oracle" {
				return samples[i].oracle, nil
			}
			return samples[i].kubemq, nil
		},
		now: func() time.Time { return now },
		wait: func(_ context.Context, d time.Duration) error {
			now = now.Add(d)
			return nil
		},
		stableFor: 5 * time.Second,
		pollEvery: time.Second,
	}

	gotOracle, gotKubeMQ, err := settler.settle(context.Background(), "oracle", "kubemq")
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := now.Sub(time.Time{}); elapsed != 7*time.Second {
		t.Fatalf("settled after %s, want 7s after the feature change", elapsed)
	}
	if calls != 16 {
		t.Fatalf("made %d observations, want 16", calls)
	}
	if gotOracle.Keys[0] != [2]int16{0, 2} || gotKubeMQ.Keys[0] != [2]int16{0, 2} {
		t.Fatalf("returned unstable key observations: oracle=%v kubemq=%v", gotOracle.Keys, gotKubeMQ.Keys)
	}
	if gotKubeMQ.Features["transaction.version"].Finalized == nil || *gotKubeMQ.Features["transaction.version"].Finalized != 2 {
		t.Fatalf("returned unstable feature observation: %+v", gotKubeMQ.Features)
	}
}

func TestApiVersionsSettlerRejectsEmptyAndInvalidObservations(t *testing.T) {
	valid := apikeys.Versions{Keys: map[int16][2]int16{0: {0, 1}}, Features: map[string]apikeys.Feature{}}
	for _, tc := range []struct {
		name string
		got  apikeys.Versions
		want string
	}{
		{name: "empty", got: apikeys.Versions{}, want: "no API keys"},
		{name: "invalid key range", got: apikeys.Versions{Keys: map[int16][2]int16{0: {2, 1}}}, want: "invalid range"},
		{name: "invalid finalized feature", got: apikeys.Versions{Keys: valid.Keys, Features: map[string]apikeys.Feature{"transaction.version": {SupportedMin: 0, SupportedMax: 1, Finalized: int16Ptr(2)}}}, want: "invalid finalized level"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settler := apiVersionsSettler{
				query:     func(_ context.Context, _ string) (apikeys.Versions, error) { return tc.got, nil },
				now:       func() time.Time { return time.Time{} },
				wait:      func(context.Context, time.Duration) error { return nil },
				stableFor: time.Second,
				pollEvery: time.Second,
			}
			_, _, err := settler.settle(context.Background(), "oracle", "kubemq")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("settle error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestApiVersionsSettlerFailsWhenObservationsKeepChanging(t *testing.T) {
	valid := func(max int16) apikeys.Versions {
		return apikeys.Versions{Keys: map[int16][2]int16{0: {0, max}}, Features: map[string]apikeys.Feature{}}
	}
	var now time.Time
	queries := 0
	settler := apiVersionsSettler{
		query: func(_ context.Context, addr string) (apikeys.Versions, error) {
			i := queries / 2
			queries++
			if addr == "oracle" {
				return valid(2), nil
			}
			return valid(int16(i)), nil
		},
		now: func() time.Time { return now },
		wait: func(_ context.Context, d time.Duration) error {
			now = now.Add(d)
			if now.Sub(time.Time{}) >= 3*time.Second {
				return context.DeadlineExceeded
			}
			return nil
		},
		stableFor: 5 * time.Second,
		pollEvery: time.Second,
	}

	_, _, err := settler.settle(context.Background(), "oracle", "kubemq")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("settle error = %v, want deadline exceeded", err)
	}
	if !strings.Contains(err.Error(), "did not settle") {
		t.Fatalf("settle error = %v, want settling failure", err)
	}
}

func TestApiVersionsSettlerSettlesWithoutFeatures(t *testing.T) {
	versions := apikeys.Versions{Keys: map[int16][2]int16{0: {0, 1}}}
	var now time.Time
	settler := apiVersionsSettler{
		query:     func(_ context.Context, _ string) (apikeys.Versions, error) { return versions, nil },
		now:       func() time.Time { return now },
		wait:      func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil },
		stableFor: time.Second,
		pollEvery: time.Second,
	}

	gotOracle, gotKubeMQ, err := settler.settle(context.Background(), "oracle", "kubemq")
	if err != nil {
		t.Fatal(err)
	}
	if gotOracle.Features != nil || gotKubeMQ.Features != nil {
		t.Fatalf("settled featureless observations were normalized: oracle=%v kubemq=%v", gotOracle.Features, gotKubeMQ.Features)
	}
}

func TestApiVersionsSettlerRejectsDeadlineAfterFinalObservation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	versions := apikeys.Versions{Keys: map[int16][2]int16{0: {0, 1}}, Features: map[string]apikeys.Feature{}}
	queries := 0
	settler := apiVersionsSettler{
		query: func(_ context.Context, _ string) (apikeys.Versions, error) {
			queries++
			if queries == 2 {
				cancel()
			}
			return versions, nil
		},
		now:       func() time.Time { return time.Time{} },
		wait:      func(context.Context, time.Duration) error { return nil },
		stableFor: time.Second,
		pollEvery: time.Second,
	}

	_, _, err := settler.settle(ctx, "oracle", "kubemq")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("settle error = %v, want context canceled", err)
	}
	if !strings.Contains(err.Error(), "did not settle") {
		t.Fatalf("settle error = %v, want settling failure", err)
	}
}

func int16Ptr(v int16) *int16 { return &v }
