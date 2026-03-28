// Package main demonstrates Tee lockstep fan-out behavior.
//
// A single upstream feeds a Tee with two branches: one fast (5ms) and one
// slow (50ms). Because Tee is lockstep, every item must be accepted by both
// branches before the next can flow. The slow branch dictates throughput.
//
// Run:
//
//	go run ./examples/tee-demo/
//	go run ./examples/tee-demo/ -reverse
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"codeberg.org/binaryphile/toc"
)

func main() {
	reverse := flag.Bool("reverse", false, "swap which branch is fast/slow")
	items := flag.Int("items", 200, "number of items to process")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	totalItems := *items

	// Stage functions.
	upstreamFn := func(_ context.Context, n int) (int, error) {
		time.Sleep(1 * time.Millisecond)
		return n, nil
	}
	fastFn := func(_ context.Context, n int) (int, error) {
		time.Sleep(5 * time.Millisecond)
		return n, nil
	}
	slowFn := func(_ context.Context, n int) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return n, nil
	}

	opts := toc.Options[int]{Capacity: 1, Workers: 1}

	// Build pipeline.
	upstream := toc.Start[int, int](ctx, upstreamFn, opts)
	teeNode := toc.NewTee[int](ctx, upstream.Out(), 2)

	// Assign branches based on -reverse flag.
	// fastIdx/slowIdx track which Tee branch index gets which function.
	var fastIdx, slowIdx int
	if *reverse {
		fastIdx = 1
		slowIdx = 0
	} else {
		fastIdx = 0
		slowIdx = 1
	}

	fast := toc.Pipe[int, int](ctx, teeNode.Branch(fastIdx), fastFn, opts)
	slow := toc.Pipe[int, int](ctx, teeNode.Branch(slowIdx), slowFn, opts)

	// Print topology header.
	if *reverse {
		fmt.Println("Topology:")
		fmt.Println("  upstream (1ms) -> Tee --> branch 0: slow (50ms)")
		fmt.Println("                       +--> branch 1: fast (5ms)")
	} else {
		fmt.Println("Topology:")
		fmt.Println("  upstream (1ms) -> Tee --> branch 0: fast (5ms)")
		fmt.Println("                       +--> branch 1: slow (50ms)")
	}
	fmt.Println()
	fmt.Printf("  Workers: 1, Capacity: 1 (all stages), Items: %d\n", totalItems)
	fmt.Println()

	// Print table header.
	fmt.Printf("%-6s  %7s  %12s  %12s  %10s  %10s\n",
		"tick", "tee/s", "tee-blk-fast", "tee-blk-slow", "delivered", "fast-idle%")
	fmt.Println("------  -------  ------------  ------------  ----------  ----------")

	// Submit items.
	var submitted atomic.Int64
	go func() {
		for i := range totalItems {
			if err := upstream.Submit(ctx, i); err != nil {
				break
			}
			submitted.Add(1)
		}
		upstream.CloseInput()
	}()

	// Drain both branches concurrently.
	var fastDone, slowDone atomic.Int64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range fast.Out() {
			fastDone.Add(1)
		}
	}()
	go func() {
		defer wg.Done()
		for range slow.Out() {
			slowDone.Add(1)
		}
	}()

	// Tick loop.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	start := time.Now()
	prevTeeStats := teeNode.Stats()
	prevFastStats := fast.Stats()
	prevTime := start
	tickNum := 0

	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			goto done
		}
		tickNum++

		now := time.Now()
		tickDur := now.Sub(prevTime)
		prevTime = now

		currTee := teeNode.Stats()
		currFast := fast.Stats()

		// Tee throughput delta.
		deliveredDelta := currTee.FullyDelivered - prevTeeStats.FullyDelivered
		teeThroughput := float64(deliveredDelta) / tickDur.Seconds()

		// Blocked time deltas.
		fastBlkDelta := currTee.BranchBlockedTime[fastIdx] - prevTeeStats.BranchBlockedTime[fastIdx]
		slowBlkDelta := currTee.BranchBlockedTime[slowIdx] - prevTeeStats.BranchBlockedTime[slowIdx]

		// Cumulative delivered per branch (lockstep invariant: should be equal).
		del0 := currTee.BranchDelivered[0]
		del1 := currTee.BranchDelivered[1]

		// Fast branch starved time delta.
		starvedDelta := currFast.StarvedTime - prevFastStats.StarvedTime
		fastIdlePct := starvedDelta.Seconds() / tickDur.Seconds() * 100

		fmt.Printf("%-6d  %7.1f  %12s  %12s  %5d==%d  %9.1f%%\n",
			tickNum, teeThroughput,
			fastBlkDelta.Truncate(time.Millisecond),
			slowBlkDelta.Truncate(time.Millisecond),
			del0, del1,
			fastIdlePct,
		)

		prevTeeStats = currTee
		prevFastStats = currFast

		// Check if done.
		if currTee.FullyDelivered >= int64(totalItems) {
			break
		}
	}

done:
	// Wait for drains and stages.
	wg.Wait()
	fast.Wait()
	slow.Wait()
	teeNode.Wait()

	totalElapsed := time.Since(start)
	finalTee := teeNode.Stats()
	finalFast := fast.Stats()

	// Summary.
	fmt.Println()
	fmt.Println("--- Summary ---")

	throughput := float64(totalItems) / totalElapsed.Seconds()
	fmt.Printf("Final throughput: %.1f items/sec\n", throughput)

	totalFastBlk := finalTee.BranchBlockedTime[fastIdx]
	totalSlowBlk := finalTee.BranchBlockedTime[slowIdx]
	var blockedRatio float64
	if totalSlowBlk+totalFastBlk > 0 {
		blockedRatio = float64(totalSlowBlk) / float64(totalSlowBlk+totalFastBlk) * 100
	}
	fmt.Printf("Tee blocked time ratio (slow / total): %.1f%%\n", blockedRatio)

	totalStarved := finalFast.StarvedTime
	fastStarvedPct := totalStarved.Seconds() / totalElapsed.Seconds() * 100
	fmt.Printf("Fast branch total starved: %.1f%%\n", fastStarvedPct)

	// Lockstep invariant: both branches received the same count.
	branch0Del := finalTee.BranchDelivered[0]
	branch1Del := finalTee.BranchDelivered[1]
	if branch0Del == branch1Del {
		fmt.Printf("Lockstep invariant: PASS (both branches received %d items)\n", branch0Del)
	} else {
		fmt.Printf("Lockstep invariant: FAIL (branch 0: %d, branch 1: %d)\n", branch0Del, branch1Del)
	}

	// Teaching narrative.
	fmt.Println()
	fmt.Printf(`Tee is lockstep: every item must be accepted by every branch before the
next item can flow. The slowest branch sets the pace for the entire
fan-out -- in this demo, ~%.0f items/sec regardless of how fast
the other branch is.

The Tee spent %.0f%% of its send time blocked on the slow branch.
Meanwhile, the fast branch was idle %.0f%% of the time -- starved
for input because the Tee couldn't deliver the next item until the slow
branch accepted the current one.

Note: Tee sends to branches in index order (0 first, 1 second).
Run with %s to see the same throughput with a different
blocked-time distribution.

What to try:
  - Speed up the slow branch (the only way to improve lockstep throughput)
  - Replace Tee with an async fan-out (decouples branches, loses ordering)
  - Add a buffered intermediary (absorbs bursts, doesn't change steady-state)
`, throughput, blockedRatio, fastStarvedPct, reverseHint(*reverse))
}

func reverseHint(reversed bool) string {
	if reversed {
		return "-reverse=false (default)"
	}
	return "-reverse"
}
