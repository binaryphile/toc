package adapter

import (
	"log/slog"
	"testing"
	"time"

	"github.com/binaryphile/toc/core"
)

var discardLogger = slog.New(slog.DiscardHandler)

func TestDeltaTracker_Baseline(t *testing.T) {
	dt := NewDeltaTracker(5, discardLogger)

	// First poll establishes baseline — no observations returned.
	obs := dt.Step(time.Now(), map[string]StageMetrics{
		"stage-a": {
			Mask:        HasCompletions | HasQueueDepth,
			Completions: 100,
			QueueDepth:  5,
		},
	})

	if obs != nil {
		t.Fatalf("first poll should return nil, got %d observations", len(obs))
	}
}

func TestDeltaTracker_Delta(t *testing.T) {
	dt := NewDeltaTracker(5, discardLogger)
	t0 := time.Now()
	t1 := t0.Add(10 * time.Second)

	dt.Step(t0, map[string]StageMetrics{
		"stage-a": {
			Mask:        HasCompletions | HasFailures | HasQueueDepth | HasWorkers,
			Completions: 100,
			Failures:    5,
			QueueDepth:  10,
			Workers:     4,
		},
	})

	obs := dt.Step(t1, map[string]StageMetrics{
		"stage-a": {
			Mask:        HasCompletions | HasFailures | HasQueueDepth | HasWorkers,
			Completions: 150,
			Failures:    7,
			QueueDepth:  3,
			Workers:     4,
		},
	})

	if len(obs) != 1 {
		t.Fatalf("got %d observations, want 1", len(obs))
	}

	o := obs[0]
	if o.Stage != "stage-a" {
		t.Errorf("stage = %q, want stage-a", o.Stage)
	}
	if o.Completions != 50 {
		t.Errorf("completions delta = %d, want 50", o.Completions)
	}
	if o.Failures != 2 {
		t.Errorf("failures delta = %d, want 2", o.Failures)
	}
	if o.QueueDepth != 3 {
		t.Errorf("queue depth = %d, want 3 (gauge)", o.QueueDepth)
	}
	if o.Workers != 4 {
		t.Errorf("workers = %d, want 4", o.Workers)
	}

	// CapacityWork = avg(4,4) * 10s = 4 * 10e9 = 40e9
	wantCap := core.Work(4 * 10e9)
	if o.CapacityWork != wantCap {
		t.Errorf("capacity work = %d, want %d", o.CapacityWork, wantCap)
	}

	// Mask should have completed, failed, queue.
	if o.Mask&core.HasCompleted == 0 {
		t.Error("mask missing HasCompleted")
	}
	if o.Mask&core.HasFailed == 0 {
		t.Error("mask missing HasFailed")
	}
	if o.Mask&core.HasQueue == 0 {
		t.Error("mask missing HasQueue")
	}
}

func TestDeltaTracker_CounterReset(t *testing.T) {
	dt := NewDeltaTracker(5, discardLogger)
	t0 := time.Now()

	dt.Step(t0, map[string]StageMetrics{
		"s": {Mask: HasCompletions | HasFailures, Completions: 100, Failures: 10},
	})

	// Completions reset (went backward), failures still increasing.
	obs := dt.Step(t0.Add(10*time.Second), map[string]StageMetrics{
		"s": {Mask: HasCompletions | HasFailures, Completions: 50, Failures: 12},
	})

	if len(obs) != 1 {
		t.Fatalf("got %d observations, want 1", len(obs))
	}
	if obs[0].Completions != 0 {
		t.Errorf("completions = %d, want 0 (clamped on reset)", obs[0].Completions)
	}
	if obs[0].Failures != 2 {
		t.Errorf("failures = %d, want 2 (unaffected by completions reset)", obs[0].Failures)
	}
}

func TestDeltaTracker_MissingStage(t *testing.T) {
	dt := NewDeltaTracker(5, discardLogger)
	t0 := time.Now()

	dt.Step(t0, map[string]StageMetrics{
		"s": {Mask: HasCompletions, Completions: 100},
	})

	// Stage missing from poll — no observation, baseline preserved.
	obs := dt.Step(t0.Add(10*time.Second), map[string]StageMetrics{})

	if len(obs) != 0 {
		t.Fatalf("got %d observations for missing stage, want 0", len(obs))
	}

	// Stage returns — delta computed from original baseline.
	obs = dt.Step(t0.Add(20*time.Second), map[string]StageMetrics{
		"s": {Mask: HasCompletions, Completions: 150},
	})

	if len(obs) != 1 {
		t.Fatalf("got %d observations, want 1", len(obs))
	}
	if obs[0].Completions != 50 {
		t.Errorf("completions = %d, want 50", obs[0].Completions)
	}
}

func TestDeltaTracker_StalenessEviction(t *testing.T) {
	dt := NewDeltaTracker(3, discardLogger)
	t0 := time.Now()

	dt.Step(t0, map[string]StageMetrics{
		"s": {Mask: HasCompletions, Completions: 100},
	})

	// Miss 3 polls — should evict.
	for i := 1; i <= 3; i++ {
		dt.Step(t0.Add(time.Duration(i)*10*time.Second), map[string]StageMetrics{})
	}

	// Stage returns — treated as new baseline (evicted).
	obs := dt.Step(t0.Add(40*time.Second), map[string]StageMetrics{
		"s": {Mask: HasCompletions, Completions: 200},
	})

	if len(obs) != 0 {
		t.Fatalf("got %d observations after eviction, want 0 (re-baseline)", len(obs))
	}
}

func TestDeltaTracker_PartialMask(t *testing.T) {
	dt := NewDeltaTracker(5, discardLogger)
	t0 := time.Now()

	// First poll has completions + failures.
	dt.Step(t0, map[string]StageMetrics{
		"s": {Mask: HasCompletions | HasFailures, Completions: 100, Failures: 5},
	})

	// Second poll only has completions (failures unavailable).
	obs := dt.Step(t0.Add(10*time.Second), map[string]StageMetrics{
		"s": {Mask: HasCompletions, Completions: 150},
	})

	if len(obs) != 1 {
		t.Fatalf("got %d observations, want 1", len(obs))
	}
	if obs[0].Mask&core.HasCompleted == 0 {
		t.Error("should have HasCompleted (valid in both)")
	}
	if obs[0].Mask&core.HasFailed != 0 {
		t.Error("should NOT have HasFailed (missing in curr)")
	}
	if obs[0].Completions != 50 {
		t.Errorf("completions = %d, want 50", obs[0].Completions)
	}
}

func TestDeltaTracker_MultiStage(t *testing.T) {
	dt := NewDeltaTracker(5, discardLogger)
	t0 := time.Now()

	dt.Step(t0, map[string]StageMetrics{
		"a": {Mask: HasCompletions, Completions: 10},
		"b": {Mask: HasCompletions, Completions: 20},
	})

	obs := dt.Step(t0.Add(10*time.Second), map[string]StageMetrics{
		"a": {Mask: HasCompletions, Completions: 15},
		"b": {Mask: HasCompletions, Completions: 30},
	})

	if len(obs) != 2 {
		t.Fatalf("got %d observations, want 2", len(obs))
	}

	byName := make(map[string]core.StageObservation)
	for _, o := range obs {
		byName[o.Stage] = o
	}

	if byName["a"].Completions != 5 {
		t.Errorf("a completions = %d, want 5", byName["a"].Completions)
	}
	if byName["b"].Completions != 10 {
		t.Errorf("b completions = %d, want 10", byName["b"].Completions)
	}
}

func TestDeltaTracker_WorkerAvgCapacity(t *testing.T) {
	dt := NewDeltaTracker(5, discardLogger)
	t0 := time.Now()

	dt.Step(t0, map[string]StageMetrics{
		"s": {Mask: HasWorkers | HasCompletions, Workers: 2, Completions: 0},
	})

	// Workers scaled from 2 to 6.
	obs := dt.Step(t0.Add(10*time.Second), map[string]StageMetrics{
		"s": {Mask: HasWorkers | HasCompletions, Workers: 6, Completions: 10},
	})

	if len(obs) != 1 {
		t.Fatalf("got %d observations, want 1", len(obs))
	}

	// CapacityWork = avg(2, 6) * 10e9 = 4 * 10e9 = 40e9
	wantCap := core.Work(4 * 10e9)
	if obs[0].CapacityWork != wantCap {
		t.Errorf("capacity = %d, want %d", obs[0].CapacityWork, wantCap)
	}
}

func TestDeltaTracker_GaugeOnly(t *testing.T) {
	dt := NewDeltaTracker(5, discardLogger)
	t0 := time.Now()

	// Only gauge fields — still useful.
	dt.Step(t0, map[string]StageMetrics{
		"s": {Mask: HasQueueDepth, QueueDepth: 10},
	})

	obs := dt.Step(t0.Add(10*time.Second), map[string]StageMetrics{
		"s": {Mask: HasQueueDepth, QueueDepth: 25},
	})

	if len(obs) != 1 {
		t.Fatalf("got %d observations, want 1", len(obs))
	}
	if obs[0].QueueDepth != 25 {
		t.Errorf("queue depth = %d, want 25", obs[0].QueueDepth)
	}
	if obs[0].Mask&core.HasQueue == 0 {
		t.Error("should have HasQueue")
	}
}
