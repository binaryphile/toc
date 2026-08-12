package toc

import (
	"context"
	"testing"
	"time"
)

// TestOversizeAllow_GrantsQueuedWaiterOnceDebtClears verifies OversizeAllow's
// documented contract: "While oversize debt exists (AdmittedWeight >
// MaxWIPWeight), subsequent oversize items block like OversizeWait" implies
// they eventually unblock once debt clears. Before this fix, grantWaitersLocked
// only ever checked weightAllows(w.weight) for a queued waiter, which can
// never pass for an oversize item (w.weight > maxWIPWeight by definition, so
// maxWIPWeight-admittedWeight < w.weight for any admittedWeight >= 0) — a
// waiter that missed acquireAdmission's fast-path exemption because debt
// already existed at submission time deadlocked permanently, and per FIFO
// head-of-line blocking, stalled every waiter behind it too. Anchor: era#45731
// (era-package's embed-stage livelock indexing era's own repo).
func TestOversizeAllow_GrantsQueuedWaiterOnceDebtClears(t *testing.T) {
	ctx := context.Background()

	release := make(chan struct{})
	started := make(chan struct{}, 1)

	fn := func(ctx context.Context, n int) (int, error) {
		started <- struct{}{}
		<-release
		return n, nil
	}

	s := Start[int, int](ctx, fn, Options[int]{
		Workers:        2,
		Capacity:       10,
		MaxWIP:         10,
		MaxWIPWeight:   10,
		OversizePolicy: OversizeAllow,
		Weight:         func(n int) int64 { return int64(n) },
	})
	go func() {
		for range s.Out() { // justified:WL -- drain so workers can release admission
		}
	}()

	// A: oversize (weight 20 > limit 10), no debt yet -> fast-path exempt.
	go func() {
		if err := s.Submit(ctx, 20); err != nil {
			t.Errorf("submit A: %v", err)
		}
	}()
	<-started // A is now in fn, holding admittedWeight=20 (debt: 20>10)

	// B: oversize too, submitted while debt exists -> falls into the waiter
	// queue as non-exempt.
	bDone := make(chan error, 1)
	go func() { bDone <- s.Submit(ctx, 20) }()

	time.Sleep(100 * time.Millisecond) // let B enqueue as a waiter

	close(release) // let A finish -> releaseAdmission -> admittedWeight back to 0

	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("B not admitted cleanly: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("B never admitted after A's debt cleared -- permanent deadlock")
	}
}
