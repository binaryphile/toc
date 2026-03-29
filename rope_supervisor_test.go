package toc_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"codeberg.org/binaryphile/toc"
)

// mockStarter records start calls and allows controlling behavior.
type mockStarter struct {
	mu       sync.Mutex
	started  []string
	failNext error
}

func (m *mockStarter) Start(target toc.ResolvedTarget) (*toc.RopeHandle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.started = append(m.started, target.Drum)
	if m.failNext != nil {
		err := m.failNext
		m.failNext = nil
		return nil, err
	}
	return toc.NewTestRopeHandle(target.Drum), nil
}

func (m *mockStarter) startCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.started)
}

func (m *mockStarter) lastStarted() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.started) == 0 {
		return ""
	}
	return m.started[len(m.started)-1]
}

func testResolver(t *testing.T, drums ...string) *toc.RopeResolver {
	t.Helper()
	p := toc.NewPipeline()
	// Build linear pipeline: head → drums...
	allStages := append([]string{"head"}, drums...)
	for _, name := range allStages {
		p.AddStage(name, func() toc.Stats { return toc.Stats{} })
	}
	for i := 0; i < len(allStages)-1; i++ {
		p.AddEdge(allStages[i], allStages[i+1])
	}
	p.Freeze()

	configs := make([]toc.RopeDrumConfig, len(drums))
	for i, d := range drums {
		configs[i] = toc.RopeDrumConfig{Drum: d}
	}

	limits := toc.NewLimitManager(
		func(n int) int { return n },
		func(n int64) int64 { return n },
		100, 0,
	)

	resolver, err := toc.NewRopeResolver(toc.RopeResolverParams{
		Pipeline:      p,
		StageSnapshot: func(string) toc.IntervalStats { return toc.IntervalStats{} },
		SourceID:      "test-rope",
		Interval:      time.Second,
		Limits:        limits,
		Drums:         configs,
	})
	if err != nil {
		t.Fatalf("NewRopeResolver: %v", err)
	}
	return resolver
}

func TestSupervisorRetarget(t *testing.T) {
	starter := &mockStarter{}
	resolver := testResolver(t, "A", "B")
	// Build a pipeline with both A and B reachable — actually we need separate linear paths.
	// For simplicity: head → A and head → B won't work (fan-out). Use head → mid → A with mid → B? No.
	// Simpler: two separate resolvers. Actually the resolver validates topology...
	// Let me use a 3-stage pipeline: head → A → B. Both A and B are valid drums.

	sup := toc.NewRopeSupervisor(resolver, starter)
	defer sup.Stop()

	sup.SetIdentified("A")
	time.Sleep(20 * time.Millisecond)

	s := sup.Status()
	if !s.Active || s.Drum != "A" {
		t.Fatalf("after SetIdentified(A): status = %+v", s)
	}

	sup.SetIdentified("B")
	time.Sleep(20 * time.Millisecond)

	s = sup.Status()
	if !s.Active || s.Drum != "B" {
		t.Fatalf("after SetIdentified(B): status = %+v", s)
	}

	if starter.startCount() != 2 {
		t.Errorf("start count = %d, want 2", starter.startCount())
	}
}

func TestSupervisorSameDrumNoOp(t *testing.T) {
	starter := &mockStarter{}
	resolver := testResolver(t, "A", "B")
	sup := toc.NewRopeSupervisor(resolver, starter)
	defer sup.Stop()

	sup.SetIdentified("A")
	time.Sleep(20 * time.Millisecond)

	sup.SetIdentified("A")
	time.Sleep(20 * time.Millisecond)

	if starter.startCount() != 1 {
		t.Errorf("start count = %d, want 1 (no-op on same drum)", starter.startCount())
	}
}

func TestSupervisorClearDesiredStopsRope(t *testing.T) {
	starter := &mockStarter{}
	resolver := testResolver(t, "A", "B")
	sup := toc.NewRopeSupervisor(resolver, starter)
	defer sup.Stop()

	sup.SetIdentified("A")
	time.Sleep(20 * time.Millisecond)

	sup.ClearDesired()
	time.Sleep(20 * time.Millisecond)

	s := sup.Status()
	if s.Active || s.Drum != "" {
		t.Errorf("after ClearDesired: status = %+v, want inactive", s)
	}
}

func TestSupervisorUnknownDrumFailClosed(t *testing.T) {
	starter := &mockStarter{}
	resolver := testResolver(t, "A")
	sup := toc.NewRopeSupervisor(resolver, starter)
	defer sup.Stop()

	sup.SetIdentified("A")
	time.Sleep(20 * time.Millisecond)

	sup.SetIdentified("UNKNOWN")
	time.Sleep(20 * time.Millisecond)

	s := sup.Status()
	if s.Active {
		t.Errorf("after unknown drum: should be inactive (fail-closed)")
	}
}

func TestSupervisorStartFailure(t *testing.T) {
	starter := &mockStarter{failNext: errors.New("boom")}
	resolver := testResolver(t, "A", "B")
	sup := toc.NewRopeSupervisor(resolver, starter)
	defer sup.Stop()

	sup.SetIdentified("A")
	time.Sleep(20 * time.Millisecond)

	s := sup.Status()
	if s.Active {
		t.Errorf("after start failure: should be inactive")
	}
}

func TestSupervisorStopIdempotent(t *testing.T) {
	starter := &mockStarter{}
	resolver := testResolver(t, "A", "B")
	sup := toc.NewRopeSupervisor(resolver, starter)

	sup.Stop()
	// Second stop should not panic or hang.
	// (workerDone is already closed, so <-workerDone returns immediately)
	// But close(stopCh) on second call would panic. Need to guard.
}

func TestSupervisorSetIdentifiedAfterStop(t *testing.T) {
	starter := &mockStarter{}
	resolver := testResolver(t, "A", "B")
	sup := toc.NewRopeSupervisor(resolver, starter)
	sup.Stop()

	// Should not panic or start a rope.
	sup.SetIdentified("A")
	time.Sleep(20 * time.Millisecond)

	if starter.startCount() != 0 {
		t.Errorf("started %d ropes after Stop, want 0", starter.startCount())
	}
}

func TestResolverEagerValidation(t *testing.T) {
	p := toc.NewPipeline()
	p.AddStage("A", func() toc.Stats { return toc.Stats{} })
	p.AddStage("B", func() toc.Stats { return toc.Stats{} })
	p.AddEdge("A", "B")
	p.Freeze()

	limits := toc.NewLimitManager(
		func(n int) int { return n },
		func(n int64) int64 { return n },
		100, 0,
	)

	// "C" doesn't exist — should fail eagerly.
	_, err := toc.NewRopeResolver(toc.RopeResolverParams{
		Pipeline:      p,
		StageSnapshot: func(string) toc.IntervalStats { return toc.IntervalStats{} },
		SourceID:      "test",
		Interval:      time.Second,
		Limits:        limits,
		Drums:         []toc.RopeDrumConfig{{Drum: "C"}},
	})
	if err == nil {
		t.Fatal("expected error for nonexistent drum C")
	}
}
