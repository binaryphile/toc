package adapter_test

import (
	"context"
	"log/slog"
	"sort"
	"testing"
	"time"

	"codeberg.org/binaryphile/toc"
	"codeberg.org/binaryphile/toc/core"
	"codeberg.org/binaryphile/toc/internal/adapter"
)

// mockSource returns canned StageMetrics per poll invocation.
type mockSource struct {
	polls   []map[string]adapter.StageMetrics
	callN   int
	closeFn func()
}

func (m *mockSource) Poll(_ context.Context) (map[string]adapter.StageMetrics, error) {
	if m.callN >= len(m.polls) {
		return m.polls[len(m.polls)-1], nil // repeat last
	}
	result := m.polls[m.callN]
	m.callN++
	return result, nil
}

func (m *mockSource) Close() error {
	if m.closeFn != nil {
		m.closeFn()
	}
	return nil
}

// capturingPublisher records published ObservationBatches.
type capturingPublisher struct {
	batches []toc.ObservationBatch
}

func (p *capturingPublisher) PublishObservations(_ context.Context, batch toc.ObservationBatch) error {
	p.batches = append(p.batches, batch)
	return nil
}

var discardLogger = slog.New(slog.DiscardHandler)

func TestIntegration_FullFlow(t *testing.T) {
	// Two stages across one source. Three polls:
	// Poll 0: baseline (no publish)
	// Poll 1: deltas computed (published)
	// Poll 2: stage-b missing optional field, stage-a counter reset

	src := &mockSource{
		polls: []map[string]adapter.StageMetrics{
			// Poll 0: baseline
			{
				"stage-a": {
					Mask:        adapter.HasCompletions | adapter.HasFailures | adapter.HasQueueDepth,
					Completions: 100,
					Failures:    5,
					QueueDepth:  10,
				},
				"stage-b": {
					Mask:        adapter.HasCompletions | adapter.HasWorkers,
					Completions: 200,
					Workers:     4,
				},
			},
			// Poll 1: normal deltas
			{
				"stage-a": {
					Mask:        adapter.HasCompletions | adapter.HasFailures | adapter.HasQueueDepth,
					Completions: 150,
					Failures:    7,
					QueueDepth:  3,
				},
				"stage-b": {
					Mask:        adapter.HasCompletions | adapter.HasWorkers,
					Completions: 280,
					Workers:     6,
				},
			},
			// Poll 2: stage-a counter reset (completions went backward),
			// stage-b loses workers field (partial mask)
			{
				"stage-a": {
					Mask:        adapter.HasCompletions | adapter.HasFailures | adapter.HasQueueDepth,
					Completions: 50, // reset!
					Failures:    9,
					QueueDepth:  1,
				},
				"stage-b": {
					Mask:        adapter.HasCompletions, // workers missing this poll
					Completions: 350,
				},
			},
		},
	}

	pub := &capturingPublisher{}
	dt := adapter.NewDeltaTracker(5, discardLogger)

	t0 := time.Now()
	times := []time.Time{t0, t0.Add(10 * time.Second), t0.Add(20 * time.Second)}

	for i := 0; i < 3; i++ {
		metrics, err := src.Poll(context.Background())
		if err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
		obs := dt.Step(times[i], metrics)
		if len(obs) > 0 {
			sort.Slice(obs, func(a, b int) bool {
				return obs[a].Stage < obs[b].Stage
			})
			batch := toc.ObservationBatch{
				PipelineID:         "test-pipeline",
				TimestampUnixNano:  times[i].UnixNano(),
				WindowDurationNano: (10 * time.Second).Nanoseconds(),
				Observations:       obs,
			}
			if err := pub.PublishObservations(context.Background(), batch); err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
		}
	}

	// Should have 2 published batches (poll 0 was baseline).
	if len(pub.batches) != 2 {
		t.Fatalf("got %d batches, want 2", len(pub.batches))
	}

	// --- Batch 1 (poll 1 deltas) ---
	b1 := pub.batches[0]
	if b1.PipelineID != "test-pipeline" {
		t.Errorf("batch 1 pipeline = %q", b1.PipelineID)
	}
	if len(b1.Observations) != 2 {
		t.Fatalf("batch 1: %d observations, want 2", len(b1.Observations))
	}

	// Sorted: stage-a, stage-b
	a1 := b1.Observations[0]
	b1s := b1.Observations[1]
	if a1.Stage != "stage-a" || b1s.Stage != "stage-b" {
		t.Fatalf("batch 1 stages: %q, %q", a1.Stage, b1s.Stage)
	}

	// stage-a: completions 150-100=50, failures 7-5=2, queue=3
	if a1.Completions != 50 {
		t.Errorf("batch1 stage-a completions = %d, want 50", a1.Completions)
	}
	if a1.Failures != 2 {
		t.Errorf("batch1 stage-a failures = %d, want 2", a1.Failures)
	}
	if a1.QueueDepth != 3 {
		t.Errorf("batch1 stage-a queue = %d, want 3", a1.QueueDepth)
	}

	// stage-b: completions 280-200=80, workers=6 (gauge),
	// capacity = avg(4,6)*10e9 = 50e9
	if b1s.Completions != 80 {
		t.Errorf("batch1 stage-b completions = %d, want 80", b1s.Completions)
	}
	if b1s.Workers != 6 {
		t.Errorf("batch1 stage-b workers = %d, want 6", b1s.Workers)
	}
	wantCap := core.Work(5 * 10e9) // avg(4,6) = 5
	if b1s.CapacityWork != wantCap {
		t.Errorf("batch1 stage-b capacity = %d, want %d", b1s.CapacityWork, wantCap)
	}

	// --- Batch 2 (poll 2 deltas) ---
	b2 := pub.batches[1]
	if len(b2.Observations) != 2 {
		t.Fatalf("batch 2: %d observations, want 2", len(b2.Observations))
	}

	a2 := b2.Observations[0]
	b2s := b2.Observations[1]

	// stage-a: completions reset (50 < 150) → clamped to 0
	if a2.Completions != 0 {
		t.Errorf("batch2 stage-a completions = %d, want 0 (reset)", a2.Completions)
	}
	// failures still increasing: 9-7=2
	if a2.Failures != 2 {
		t.Errorf("batch2 stage-a failures = %d, want 2", a2.Failures)
	}
	// queue gauge = 1
	if a2.QueueDepth != 1 {
		t.Errorf("batch2 stage-a queue = %d, want 1", a2.QueueDepth)
	}

	// stage-b: completions 350-280=70, workers missing this poll
	if b2s.Completions != 70 {
		t.Errorf("batch2 stage-b completions = %d, want 70", b2s.Completions)
	}
	// Workers not in curr mask → no CapacityWork
	if b2s.CapacityWork != 0 {
		t.Errorf("batch2 stage-b capacity = %d, want 0 (workers missing)", b2s.CapacityWork)
	}
}

func TestIntegration_SourceCloseCalledOnExit(t *testing.T) {
	closed := false
	src := &mockSource{
		polls:   []map[string]adapter.StageMetrics{{}},
		closeFn: func() { closed = true },
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediate cancel

	cfg := adapter.Config{
		PipelineID:       "test",
		PollInterval:     time.Second,
		StalenessWindows: 5,
	}

	pub := &capturingPublisher{}
	sources := []adapter.NamedSource{
		{Source: src, Type: "mock", URL: "mock://test"},
	}
	adapter.Run(ctx, cfg, sources, pub, discardLogger)

	if !closed {
		t.Error("Source.Close() was not called")
	}
}
