package adapter

import (
	"log/slog"
	"time"

	"github.com/binaryphile/toc/core"
)

// stageState holds the previous poll's metrics and staleness counter
// for one stage.
type stageState struct {
	prev     StageMetrics
	prevTime time.Time
	missedN  int // consecutive polls where this stage was absent
}

// DeltaTracker computes per-stage deltas from consecutive StageMetrics
// polls and produces core.StageObservation values. First poll per stage
// establishes a baseline; subsequent polls compute deltas.
//
// Mirrors adapt.go:Adapt but operates on StageMetrics from external
// sources instead of toc.Stats from in-process stages.
type DeltaTracker struct {
	stages         map[string]*stageState
	stalenessLimit int // evict after this many consecutive misses
	logger         *slog.Logger
}

// NewDeltaTracker creates a tracker. stalenessLimit is the number of
// consecutive missed polls before a stage's state is evicted (forcing
// re-baseline on return). Use 0 for no eviction.
func NewDeltaTracker(stalenessLimit int, logger *slog.Logger) *DeltaTracker {
	if logger == nil {
		logger = slog.Default()
	}
	return &DeltaTracker{
		stages:         make(map[string]*stageState),
		stalenessLimit: stalenessLimit,
		logger:         logger,
	}
}

// Step takes the current poll results and returns observations for
// stages that have a baseline. Returns nil on first call (baseline
// establishment) or when no deltas are computable.
func (d *DeltaTracker) Step(now time.Time, current map[string]StageMetrics) []core.StageObservation {
	var obs []core.StageObservation

	// Process stages present in this poll.
	for name, curr := range current {
		st, exists := d.stages[name]
		if !exists {
			// First observation — establish baseline.
			d.stages[name] = &stageState{prev: curr, prevTime: now}
			continue
		}

		// Reset miss counter — stage is present.
		st.missedN = 0

		elapsed := now.Sub(st.prevTime)
		if elapsed <= 0 {
			st.prev = curr
			st.prevTime = now
			continue
		}

		o := d.computeObservation(name, st, curr, elapsed)
		obs = append(obs, o)

		st.prev = curr
		st.prevTime = now
	}

	// Track missing stages and evict stale ones.
	for name, st := range d.stages {
		if _, present := current[name]; present {
			continue
		}
		st.missedN++
		if d.stalenessLimit > 0 && st.missedN >= d.stalenessLimit {
			d.logger.Warn("evicting stale stage",
				"stage", name,
				"missed", st.missedN,
			)
			delete(d.stages, name)
		}
	}

	return obs
}

func (d *DeltaTracker) computeObservation(
	name string,
	st *stageState,
	curr StageMetrics,
	elapsed time.Duration,
) core.StageObservation {
	prev := st.prev

	// Only set mask bits for fields valid in both prev and curr.
	validMask := prev.Mask & curr.Mask

	obs := core.StageObservation{
		Stage: name,
	}

	// Item counter deltas.
	if validMask&HasCompletions != 0 {
		obs.Completions = safeDelta(prev.Completions, curr.Completions, name, "completions", d.logger)
		obs.Mask |= core.HasCompleted
	}
	if validMask&HasFailures != 0 {
		obs.Failures = safeDelta(prev.Failures, curr.Failures, name, "failures", d.logger)
		obs.Mask |= core.HasFailed
	}
	if validMask&HasArrivals != 0 {
		obs.Arrivals = safeDelta(prev.Arrivals, curr.Arrivals, name, "arrivals", d.logger)
	}

	// Work deltas (nanoseconds).
	if validMask&HasBusyNs != 0 {
		obs.BusyWork = core.Work(safeDelta(prev.BusyNs, curr.BusyNs, name, "busy_ns", d.logger))
	}
	if validMask&HasIdleNs != 0 {
		obs.IdleWork = core.Work(safeDelta(prev.IdleNs, curr.IdleNs, name, "idle_ns", d.logger))
		obs.Mask |= core.HasIdle
	}
	if validMask&HasBlockedNs != 0 {
		obs.BlockedWork = core.Work(safeDelta(prev.BlockedNs, curr.BlockedNs, name, "blocked_ns", d.logger))
		obs.Mask |= core.HasBlocked
	}

	// Gauges (point-in-time from current poll).
	if curr.Mask&HasQueueDepth != 0 {
		obs.QueueDepth = curr.QueueDepth
		obs.Mask |= core.HasQueue
	}
	if curr.Mask&HasWorkers != 0 {
		obs.Workers = int32(curr.Workers)
	}

	// CapacityWork: avg(prev, curr) workers × elapsed.
	if validMask&HasWorkers != 0 {
		avgWorkers := float64(prev.Workers+curr.Workers) / 2.0
		if avgWorkers > 0 {
			obs.CapacityWork = core.Work(avgWorkers * float64(elapsed.Nanoseconds()))
		}
	}

	return obs
}

// safeDelta computes curr - prev, clamping to 0 on counter decrease
// (reset). Logs a warning on reset detection.
func safeDelta(prev, curr int64, stage, field string, logger *slog.Logger) int64 {
	d := curr - prev
	if d < 0 {
		logger.Warn("counter reset detected",
			"stage", stage,
			"field", field,
			"prev", prev,
			"curr", curr,
		)
		return 0
	}
	return d
}
