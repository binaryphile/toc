package toc

import (
	"context"
	"log"
	"math"
	"sync/atomic"
	"time"
)

const (
	defaultSafetyFactor      = 1.5
	defaultInitialRopeLength = 1
	ewmaAlpha                = 0.3
	maxYieldInflation        = 10.0
)

// RopeController is a periodic controller that bounds aggregate WIP
// in a controlled segment of the pipeline by adjusting the control
// stage's MaxWIP (or MaxWIPWeight for weight-aware mode).
//
// The controlled segment runs from a control stage to the drum
// (constraint). By default, the control stage is the pipeline's
// topological head; use [WithControlStage] to start at a different
// stage (e.g., for unit-consistent rope when the head produces
// variable-sized outputs).
//
// It computes rope length from drum goodput, segment flow time, and a
// safety factor using a Little's Law heuristic. This is an approximate
// soft control — SetMaxWIP cannot revoke existing permits and has a
// floor of 1. After a target decrease, already-admitted items persist
// until completion.
//
// Create with [NewRopeController], configure with [RopeOption]
// functions, then call [RopeController.Run].
type RopeController struct {
	pipeline      *Pipeline
	drum          string
	controlStage  string   // start of the rope-controlled segment
	segmentStages []string // ordered stages from controlStage to drum (exclusive of drum)

	limits     *LimitManager
	source     string // proposal source name
	weightMode bool   // true = weight-based rope

	stageSnapshot func(string) IntervalStats
	interval      time.Duration
	safetyFactor  float64
	initialLength int
	logger        *log.Logger

	started bool
	lastLog ropeLogState // suppress duplicate log lines

	// EWMA state — written only by adjust goroutine.
	warmedUp bool
	ewmaGoodput   float64
	ewmaErrorRate float64
	ewmaFlowTime  map[string]float64

	// Atomic stats for lock-free reads.
	ropeLengthA      atomic.Int64
	ropeWIPA         atomic.Int64
	adjustmentCountA atomic.Int64
	controlStageAppliedWIPA atomic.Int64
	drumGoodputA     atomic.Int64 // float64 bits
	drumErrorRateA   atomic.Int64 // float64 bits
}

type ropeLogState struct {
	length  int
	wip     int64
	applied int
}

// RopeOption configures a [RopeController].
type RopeOption func(*RopeController)

// WithRopeSafetyFactor sets the safety multiplier for rope length.
// Default is 1.5. Panics if factor <= 0.
func WithRopeSafetyFactor(factor float64) RopeOption {
	if factor <= 0 {
		panic("toc.WithRopeSafetyFactor: factor must be positive")
	}
	return func(rc *RopeController) {
		rc.safetyFactor = factor
	}
}

// WithRopeLogger sets the logger. If nil, [log.Default] is used.
func WithRopeLogger(l *log.Logger) RopeOption {
	return func(rc *RopeController) {
		if l != nil {
			rc.logger = l
		}
	}
}

// WithInitialRopeLength sets the rope length used before the first
// valid goodput measurement. Default is 1 (conservative). Must be >= 1.
func WithInitialRopeLength(n int) RopeOption {
	if n < 1 {
		panic("toc.WithInitialRopeLength: n must be >= 1")
	}
	return func(rc *RopeController) {
		rc.initialLength = n
	}
}

// WithControlStage overrides the inferred segment start. By default
// the rope starts at the pipeline's topological head (zero in-degree
// stage feeding the drum). When set, the rope measures and limits only
// the segment from this stage to the drum. The named stage must exist
// in the pipeline and must reach the drum via a unique linear path.
//
// Stages upstream of the control stage are not measured by the rope
// but may still experience backpressure through channel blocking.
func WithControlStage(name string) RopeOption {
	return func(rc *RopeController) {
		rc.controlStage = name
	}
}

// NewRopeController creates a count-based rope controller.
//
// By default the controller infers the control stage from the
// pipeline's topological head (zero in-degree stage feeding the drum).
// Use [WithControlStage] to start the controlled segment at a
// non-head stage — for example, to get unit-consistent rope when
// the pipeline head produces variable-sized outputs.
//
// The controlled segment (from control stage to drum) must be a
// linear chain: no fan-out from any segment stage, no side fan-in
// to internal nodes, and the drum must have exactly one predecessor.
// The control stage may have upstream predecessors outside the segment.
//
// limits is the [LimitManager] for the control stage. The controller
// proposes count limits via limits.ProposeCount("processing-rope", n).
// stageSnapshot returns the latest [IntervalStats] for a named stage.
//
// Panics if pipeline is not frozen, drum is unknown, the controlled
// segment is not linear, limits or stageSnapshot is nil, or
// interval <= 0.
func NewRopeController(
	pipeline *Pipeline,
	drum string,
	limits *LimitManager,
	stageSnapshot func(string) IntervalStats,
	interval time.Duration,
	opts ...RopeOption,
) *RopeController {
	return newRopeController(pipeline, drum, limits, LimitSourceProcessingRope, false, stageSnapshot, interval, opts)
}

// NewWeightRopeController creates a weight-aware rope controller.
// Same as [NewRopeController] but limits aggregate WEIGHT between
// control stage and drum instead of item count. Items with variable
// processing cost are properly accounted.
//
// limits is the [LimitManager] for the control stage. The controller
// proposes weight limits via limits.ProposeWeight("processing-weight-rope", n).
//
// Same segment topology requirements as [NewRopeController].
// Supports [WithControlStage].
func NewWeightRopeController(
	pipeline *Pipeline,
	drum string,
	limits *LimitManager,
	stageSnapshot func(string) IntervalStats,
	interval time.Duration,
	opts ...RopeOption,
) *RopeController {
	return newRopeController(pipeline, drum, limits, LimitSourceWeightRope, true, stageSnapshot, interval, opts)
}

func newRopeController(
	pipeline *Pipeline,
	drum string,
	limits *LimitManager,
	source string,
	weightMode bool,
	stageSnapshot func(string) IntervalStats,
	interval time.Duration,
	opts []RopeOption,
) *RopeController {
	if pipeline == nil {
		panic("toc.NewRopeController: pipeline must not be nil")
	}
	pipeline.mustFrozen()
	pipeline.mustStage(drum)

	if limits == nil {
		panic("toc.NewRopeController: limits must not be nil")
	}
	if stageSnapshot == nil {
		panic("toc.NewRopeController: stageSnapshot must not be nil")
	}
	if interval <= 0 {
		panic("toc.NewRopeController: interval must be positive")
	}

	// Build with non-overridable fields + option defaults.
	rc := &RopeController{
		pipeline:      pipeline,
		drum:          drum,
		limits:        limits,
		source:        source,
		weightMode:    weightMode,
		stageSnapshot: stageSnapshot,
		interval:      interval,
		safetyFactor:  defaultSafetyFactor,
		initialLength: defaultInitialRopeLength,
		logger:        log.Default(),
	}

	// Apply options (may set controlStage, safetyFactor, etc.).
	for _, opt := range opts {
		opt(rc)
	}

	// Derive control stage if not set by WithControlStage.
	if rc.controlStage == "" {
		heads := pipeline.HeadsTo(drum)
		if len(heads) != 1 {
			panic("toc.NewRopeController: exactly one head must feed the drum (or use WithControlStage)")
		}
		rc.controlStage = heads[0]
	}
	pipeline.mustStage(rc.controlStage)

	// Derive and validate the controlled segment.
	rc.segmentStages = deriveSegment(pipeline, rc.controlStage, drum)
	validateSegment(pipeline, rc.controlStage, drum, rc.segmentStages)
	rc.ewmaFlowTime = make(map[string]float64, len(rc.segmentStages))
	rc.ropeLengthA.Store(int64(rc.initialLength))
	return rc
}

// deriveSegment walks forward from start to drum, collecting the
// ordered stage list (including start, excluding drum). Panics if
// start doesn't reach drum or if any stage branches.
func deriveSegment(p *Pipeline, start, drum string) []string {
	var segment []string
	visited := make(map[string]bool, 8)
	current := start
	for current != drum {
		if visited[current] {
			panic("toc.NewRopeController: cycle detected at stage: " + current)
		}
		visited[current] = true
		segment = append(segment, current)

		succs := p.forward[current]
		if len(succs) != 1 {
			panic("toc.NewRopeController: stage " + current + " has out-degree != 1 (non-linear)")
		}
		current = succs[0]
	}
	if len(segment) == 0 {
		panic("toc.NewRopeController: controlStage must not equal drum")
	}
	return segment
}

// validateSegment checks invariants on the controlled segment.
//
// The control stage may have upstream predecessors outside the segment
// (its in-degree is unchecked). All other segment stages and the drum
// must satisfy exclusivity: no side fan-in from outside the segment.
// Every stage on the segment must have out-degree=1 (no fan-out).
// The drum must have in-degree=1 (no mixed-source goodput).
func validateSegment(p *Pipeline, start, drum string, segment []string) {
	for i, name := range segment {
		// In-degree check: internal nodes (not the control stage) must
		// have exactly one predecessor. Items from outside the segment
		// would corrupt per-stage sojourn and WIP metrics.
		if i > 0 && len(p.reverse[name]) != 1 {
			panic("toc.NewRopeController: stage " + name + " has in-degree != 1 (side fan-in)")
		}
	}

	// Drum in-degree check: must have exactly one predecessor.
	// External inputs to the drum would contribute goodput that the
	// rope didn't release, breaking the sizing formula.
	if len(p.reverse[drum]) != 1 {
		panic("toc.NewRopeController: drum " + drum + " has in-degree != 1 (external inputs)")
	}
}

// Run blocks, adjusting rope length every interval until ctx is
// canceled. Panics if called twice.
func (rc *RopeController) Run(ctx context.Context) {
	rc.checkAndSetStarted()
	rc.runLoop(ctx, nil)
}

// RunWithTicker is like [RopeController.Run] but uses the provided
// tick channel instead of creating a real ticker. For testing.
// Panics if called twice or after Run.
func (rc *RopeController) RunWithTicker(ctx context.Context, ticks <-chan time.Time) {
	rc.checkAndSetStarted()
	rc.runLoop(ctx, ticks)
}

func (rc *RopeController) runLoop(ctx context.Context, ticks <-chan time.Time) {
	if ticks == nil {
		ticker := time.NewTicker(rc.interval)
		defer ticker.Stop()
		ticks = ticker.C
	}

	for {
		select {
		case <-ticks:
			rc.adjust()
		case <-ctx.Done():
			return
		}
	}
}

func (rc *RopeController) adjust() {
	// 1. Read drum snapshot.
	drumSnap := rc.stageSnapshot(rc.drum)
	rawGoodput := drumSnap.Goodput
	rawErrorRate := drumSnap.ErrorRate

	// 2. EWMA smooth the signals.
	// Goodput and error rate are updated independently: error rate updates
	// whenever the drum has completions (even if all failed), so the
	// controller sees quality collapse even when goodput is zero.
	hasCompletions := drumSnap.ItemsCompleted > 0

	var goodput, errorRate float64
	if !rc.warmedUp {
		if rawGoodput > 0 {
			// First valid goodput — seed both EWMAs.
			rc.warmedUp = true
			rc.ewmaGoodput = rawGoodput
			rc.ewmaErrorRate = rawErrorRate
			goodput = rawGoodput
			errorRate = rawErrorRate
		} else if hasCompletions {
			// Completions but no goodput (all failures) — seed error rate only.
			rc.ewmaErrorRate = rawErrorRate
			rc.drumErrorRateA.Store(int64(math.Float64bits(rawErrorRate)))
			rc.applyRopeLength(rc.initialLength)
			return
		} else {
			// No signal at all — use initial rope length.
			rc.applyRopeLength(rc.initialLength)
			return
		}
	} else {
		// Update goodput EWMA only on valid goodput signal.
		if rawGoodput > 0 {
			rc.ewmaGoodput = ewmaAlpha*rawGoodput + (1-ewmaAlpha)*rc.ewmaGoodput
		}
		// Update error rate EWMA whenever drum has completions.
		if hasCompletions {
			rc.ewmaErrorRate = ewmaAlpha*rawErrorRate + (1-ewmaAlpha)*rc.ewmaErrorRate
		}
		goodput = rc.ewmaGoodput
		errorRate = rc.ewmaErrorRate
	}

	// Store for observability.
	rc.drumGoodputA.Store(int64(math.Float64bits(goodput)))
	rc.drumErrorRateA.Store(int64(math.Float64bits(errorRate)))

	// 3. On near-total error rate, tighten to minimum — don't hold an
	// inflated rope while the drum fails almost everything. Threshold
	// is < 1.0 because EWMA asymptotically approaches but never reaches
	// raw=1.0.
	if errorRate >= 0.95 {
		rc.applyRopeLength(1)
		return
	}

	// 4. Compute required release rate with yield adjustment.
	// Cap inflation at 10× to prevent blow-up from noisy error rates.
	requiredRate := goodput
	if errorRate > 0 && errorRate < 1.0 {
		yieldAdjusted := goodput / (1 - errorRate)
		maxInflated := goodput * maxYieldInflation
		if yieldAdjusted > maxInflated {
			requiredRate = maxInflated
		} else {
			requiredRate = yieldAdjusted
		}
	}

	// 5. Compute upstream flow time (EWMA-smoothed per ancestor).
	var totalFlowTime float64
	for _, name := range rc.segmentStages {
		snap := rc.stageSnapshot(name)

		var rawFlow float64
		if snap.ItemsCompleted > 0 {
			// Sojourn time estimate: service + output-blocked per completion.
			rawFlow = (snap.ServiceTimeDelta + snap.OutputBlockedDelta).Seconds() /
				float64(snap.ItemsCompleted)
		}

		prev, hasPrev := rc.ewmaFlowTime[name]
		if !hasPrev && rawFlow > 0 {
			rc.ewmaFlowTime[name] = rawFlow
		} else if rawFlow > 0 {
			rc.ewmaFlowTime[name] = ewmaAlpha*rawFlow + (1-ewmaAlpha)*prev
		}
		// Zero completions: hold previous flow time.

		totalFlowTime += rc.ewmaFlowTime[name]
	}

	// 6. Compute rope length: L = λ × W × safety.
	ropeLengthF := requiredRate * totalFlowTime * rc.safetyFactor
	ropeLength := int(math.Ceil(ropeLengthF))
	if ropeLength < 1 {
		ropeLength = 1
	}

	// 7. Apply.
	rc.applyRopeLength(ropeLength)
}

func (rc *RopeController) applyRopeLength(ropeLength int) {
	rc.ropeLengthA.Store(int64(ropeLength))

	// Compute aggregate WIP across segment stages (includes control stage).
	// Stage occupancies are sampled independently, not from a consistent
	// snapshot. The aggregate is approximate.
	var aggregateWIP int64
	var ctrlWIP int64
	for _, name := range rc.segmentStages {
		stats := rc.pipeline.StageStats(name)()
		var wip int64
		if rc.weightMode {
			wip = stats.AdmittedWeight
		} else {
			wip = stats.Admitted
		}
		if wip < 0 {
			wip = 0
		}
		aggregateWIP += wip
		if name == rc.controlStage {
			ctrlWIP = wip
		}
	}

	rc.ropeWIPA.Store(aggregateWIP)

	downstreamWIP := aggregateWIP - ctrlWIP
	if downstreamWIP < 0 {
		downstreamWIP = 0
	}
	ctrlLimit := int64(ropeLength) - downstreamWIP
	if ctrlLimit < 1 {
		ctrlLimit = 1 // floor: 0 disables limiting in both SetMaxWIP and SetMaxWIPWeight
	}

	if rc.weightMode {
		rc.limits.ProposeWeight(rc.source, ctrlLimit)
	} else {
		rc.limits.ProposeCount(rc.source, int(ctrlLimit))
	}
	snap := rc.limits.Effective()
	var applied int64
	if rc.weightMode {
		applied = snap.AppliedWeight
	} else {
		applied = int64(snap.AppliedCount)
	}
	rc.controlStageAppliedWIPA.Store(applied)
	rc.adjustmentCountA.Add(1)

	// Log only on change.
	mode := "rope"
	if rc.weightMode {
		mode = "weight-rope"
	}
	curr := ropeLogState{length: ropeLength, wip: aggregateWIP, applied: int(applied)}
	if curr != rc.lastLog {
		rc.logger.Printf("[%s] length=%d wip=%d ctrl=%d→%d goodput=%.1f err=%.2f",
			mode, ropeLength, aggregateWIP, ctrlLimit, applied,
			math.Float64frombits(uint64(rc.drumGoodputA.Load())),
			math.Float64frombits(uint64(rc.drumErrorRateA.Load())))
		rc.lastLog = curr
	}
}

// RopeStats is a point-in-time snapshot of the controller's state.
type RopeStats struct {
	RopeLength      int     // current computed rope length
	RopeWIP         int     // current aggregate WIP across upstream stages
	RopeUtilization float64 // WIP / Length; 0 if length is 0
	DrumGoodput     float64 // EWMA-smoothed drum goodput (items/sec)
	DrumErrorRate   float64 // EWMA-smoothed drum error rate
	AdjustmentCount int64   // how many times rope was adjusted
	HeadAppliedWIP int // effective WIP limit applied at the control stage (name kept for API compat)
}

func (rc *RopeController) checkAndSetStarted() {
	if rc.started {
		panic("toc.RopeController: already running")
	}
	rc.started = true
}

// Stats returns a snapshot of the rope controller's current state.
// Safe for concurrent calls.
func (rc *RopeController) Stats() RopeStats {
	length := int(rc.ropeLengthA.Load())
	wip := int(rc.ropeWIPA.Load())

	var util float64
	if length > 0 {
		util = float64(wip) / float64(length)
	}

	return RopeStats{
		RopeLength:      length,
		RopeWIP:         wip,
		RopeUtilization: util,
		DrumGoodput:     math.Float64frombits(uint64(rc.drumGoodputA.Load())),
		DrumErrorRate:   math.Float64frombits(uint64(rc.drumErrorRateA.Load())),
		AdjustmentCount: rc.adjustmentCountA.Load(),
		HeadAppliedWIP:  int(rc.controlStageAppliedWIPA.Load()),
	}
}
