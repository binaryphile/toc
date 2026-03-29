package core

import "math"

// Default classification thresholds (from Goldratt/TameFlow conventions).
var DefaultThresholds = Thresholds{
	BrokenError:       0.2,
	StarvedIdle:       0.5,
	BlockedBlocked:    0.3,
	SaturatedBusy:     0.7,
	SaturatedIdle:     0.3,
	SaturatedBlock:    0.2,
	HysteresisWindows: 3,
	DemotionWindows:   3,
	ConfidenceMin:     10,
}

// Thresholds configures classification sensitivity.
type Thresholds struct {
	BrokenError       float64 // error rate above this → Broken
	StarvedIdle       float64 // idle ratio above this → Starved
	BlockedBlocked    float64 // blocked ratio above this → Blocked
	SaturatedBusy     float64 // utilization above this → Saturated candidate
	SaturatedIdle     float64 // idle must be below this for Saturated
	SaturatedBlock    float64 // blocked must be below this for Saturated
	HysteresisWindows int     // consecutive windows before challenger promotes; >= 1
	DemotionWindows   int     // consecutive unsupported windows before incumbent demotes; >= 1
	ConfidenceMin     int64   // minimum completions for confident classification
}

// Option configures an [Analyzer].
type Option func(*Analyzer)

// WithThresholds sets custom classification thresholds.
func WithThresholds(t Thresholds) Option {
	return func(a *Analyzer) { a.thresholds = t }
}

// WithDrum sets a manual constraint override. Bypasses automatic
// identification — the analyzer will report this stage as Identified
// with [ConstraintSourceManualOverride] on every Step.
func WithDrum(name string) Option {
	return func(a *Analyzer) { a.drum = name }
}

// Analyzer is the deterministic constraint identifier. No goroutines,
// no time.Now(), no channels. Same inputs → same outputs.
//
// Internally tracks an incumbent (confirmed constraint) and a
// challenger (candidate building toward promotion). Promotion
// requires [Thresholds.HysteresisWindows] consecutive windows as
// sole top saturated stage. Demotion requires
// [Thresholds.DemotionWindows] consecutive unsupported windows.
//
// Call [Analyzer.Step] once per analysis window with observations for
// all stages. The analyzer maintains hysteresis state between calls.
type Analyzer struct {
	thresholds     Thresholds
	prevQueueDepth map[string]int64
	drum           string // manual override
	prevDrum       string // edge detection for override transitions

	// Incumbent: confirmed constraint.
	incumbent    string
	unsupportedN int // consecutive windows incumbent is NOT top
	supportN     int // consecutive windows incumbent IS top (freshness)

	// Challenger: replacement candidate (never == incumbent while tracking).
	challenger  string
	challengerN int // consecutive windows as sole top

	starvationN int
}

// NewAnalyzer creates a deterministic constraint analyzer.
// Panics if HysteresisWindows < 1 or DemotionWindows < 1.
func NewAnalyzer(opts ...Option) *Analyzer {
	a := &Analyzer{
		thresholds:     DefaultThresholds,
		prevQueueDepth: make(map[string]int64),
	}
	for _, opt := range opts {
		opt(a)
	}
	if a.thresholds.HysteresisWindows < 1 {
		panic("core.NewAnalyzer: HysteresisWindows must be >= 1")
	}
	if a.thresholds.DemotionWindows < 1 {
		panic("core.NewAnalyzer: DemotionWindows must be >= 1")
	}
	return a
}

// SetDrum sets or clears the manual constraint override.
// Pass empty string to revert to automatic identification.
func (a *Analyzer) SetDrum(name string) {
	a.drum = name
}

// Step processes one analysis window. Returns classification for
// each stage and the current constraint identity.
//
// Deterministic: same sequence of inputs produces the same sequence
// of outputs. No wall-clock dependency.
func (a *Analyzer) Step(observations []StageObservation) Diagnosis {
	diag := Diagnosis{
		Stages: make([]StageDiagnosis, 0, len(observations)),
	}

	// Classify stages and collect saturated candidates.
	const tieMargin = 0.05

	type candidate struct {
		name string
		util float64
	}
	var saturated []candidate

	for _, obs := range observations {
		sd := a.classifyStage(obs)
		diag.Stages = append(diag.Stages, sd)

		if sd.State == StateSaturated {
			saturated = append(saturated, candidate{sd.Stage, sd.Utilization})
		}
	}

	// Pick sole top saturated stage. "" if tie or none.
	topName := ""
	tied := false
	if len(saturated) > 0 {
		best := saturated[0]
		for _, c := range saturated[1:] {
			if c.util > best.util {
				best = c
			}
		}
		for _, c := range saturated {
			if c.name != best.name && best.util-c.util < tieMargin {
				tied = true
				break
			}
		}
		if !tied {
			topName = best.name
		}
	}

	// ── Override edge detection ──
	if a.drum != a.prevDrum {
		if a.drum != "" {
			// Entering override: clear challenger.
			a.challenger = ""
			a.challengerN = 0
		}
		a.prevDrum = a.drum
	}

	// ── Override active: emit and skip hysteresis ──
	if a.drum != "" {
		diag.ConstraintState = ConstraintIdentified
		diag.ConstraintSource = ConstraintSourceManualOverride
		diag.Constraint = a.drum
		diag.SupportFreshness = 1.0

		// Starvation still tracked for override target.
		a.trackStarvation(&diag)
		diag.StarvationCount = a.starvationN

		a.updateQueueDepths(observations)
		return diag
	}

	// ── Step 4: Update challenger (only stages != incumbent) ──
	if topName != "" && topName != a.incumbent {
		if topName == a.challenger {
			a.challengerN++
		} else {
			a.challenger = topName
			a.challengerN = 1
		}
	} else {
		// topName == "" (tie/none) or topName == incumbent: gap for challenger.
		a.challengerN = 0
		// challenger name preserved for resume
	}

	// ── Step 5: Promote ──
	if a.challenger != "" && a.challenger != a.incumbent && a.challengerN >= a.thresholds.HysteresisWindows {
		a.incumbent = a.challenger
		a.challenger = ""
		a.challengerN = 0
		a.unsupportedN = 0
		a.supportN = a.thresholds.HysteresisWindows
		a.starvationN = 0
	}

	// ── Step 6: Update incumbent support ──
	if a.incumbent != "" {
		if topName == a.incumbent {
			a.supportN++
			a.unsupportedN = 0
		} else {
			a.supportN = 0
			a.unsupportedN++
		}
	}

	// ── Step 7: Demote ──
	if a.incumbent != "" && a.unsupportedN > 0 && a.unsupportedN >= a.thresholds.DemotionWindows {
		a.incumbent = ""
		a.unsupportedN = 0
		a.supportN = 0
		a.starvationN = 0
		// challenger state preserved — replacement may already be building
	}

	// ── Step 8: Starvation tracking ──
	a.trackStarvation(&diag)

	// ── Emit diagnosis ──
	switch {
	case len(observations) == 0:
		diag.ConstraintState = ConstraintUnknown

	case a.incumbent != "":
		diag.ConstraintState = ConstraintIdentified
		diag.ConstraintSource = ConstraintSourceInferred
		diag.Constraint = a.incumbent
		diag.SupportFreshness = math.Min(float64(a.supportN)/10.0, 1.0)
		diag.UnsupportedCount = a.unsupportedN
		// Emit candidate if challenger is valid and distinct.
		if a.challenger != "" && a.challengerN > 0 && a.challenger != a.incumbent {
			diag.CandidateConstraint = a.challenger
		}

	case a.challenger != "" && a.challengerN > 0:
		diag.ConstraintState = ConstraintEmerging
		diag.CandidateConstraint = a.challenger

	case tied:
		diag.ConstraintState = ConstraintAmbiguous

	default:
		diag.ConstraintState = ConstraintUnconstrained
	}

	diag.StarvationCount = a.starvationN

	a.updateQueueDepths(observations)
	return diag
}

func (a *Analyzer) trackStarvation(diag *Diagnosis) {
	if a.incumbent == "" && a.drum == "" {
		return
	}
	target := a.incumbent
	if a.drum != "" {
		target = a.drum
	}

	starved := false
	for _, sd := range diag.Stages {
		if sd.Stage == target && sd.State == StateStarved {
			starved = true
			break
		}
	}
	if starved {
		a.starvationN++
	} else {
		a.starvationN = 0
	}
}

func (a *Analyzer) updateQueueDepths(observations []StageObservation) {
	for _, obs := range observations {
		a.prevQueueDepth[obs.Stage] = obs.QueueDepth
	}
}

func (a *Analyzer) classifyStage(obs StageObservation) StageDiagnosis {
	sd := StageDiagnosis{
		Stage:       obs.Stage,
		Completions: obs.Completions,
		Failures:    obs.Failures,
		Arrivals:    obs.Arrivals,
	}

	// Queue growth from previous observation.
	if obs.Mask&HasQueue != 0 {
		if prev, ok := a.prevQueueDepth[obs.Stage]; ok {
			sd.QueueGrowth = obs.QueueDepth - prev
		}
	}

	// Compute ratios.
	if obs.CapacityWork > 0 {
		sd.Utilization = float64(obs.BusyWork) / float64(obs.CapacityWork)

		if obs.Mask&HasIdle != 0 {
			sd.IdleRatio = float64(obs.IdleWork) / float64(obs.CapacityWork)
			sd.HasIdleRatio = true
		}
		if obs.Mask&HasBlocked != 0 {
			sd.BlockedRatio = float64(obs.BlockedWork) / float64(obs.CapacityWork)
			sd.HasBlockedRatio = true
		}
	}

	if obs.Mask&HasCompleted != 0 && obs.Completions > 0 {
		if obs.Mask&HasFailed != 0 {
			sd.ErrorRate = float64(obs.Failures) / float64(obs.Completions)
		}
	}

	// Classify.
	sd.State = a.classify(sd, obs)
	return sd
}

func (a *Analyzer) classify(sd StageDiagnosis, obs StageObservation) StageState {
	// Insufficient data gate.
	if obs.CapacityWork == 0 || (obs.Completions == 0 && sd.Utilization == 0) {
		return StateUnknown
	}

	// Broken: high error rate.
	if sd.ErrorRate > a.thresholds.BrokenError {
		return StateBroken
	}

	// Starved: high idle AND queue not growing. Skip if no idle data.
	if sd.HasIdleRatio &&
		sd.IdleRatio > a.thresholds.StarvedIdle &&
		sd.QueueGrowth <= 0 {
		return StateStarved
	}

	// Blocked: high output-blocked. Skip if no blocked data.
	if sd.HasBlockedRatio &&
		sd.BlockedRatio > a.thresholds.BlockedBlocked {
		return StateBlocked
	}

	// Saturated: high busy, low idle, low blocked.
	saturatedIdle := !sd.HasIdleRatio || sd.IdleRatio < a.thresholds.SaturatedIdle
	saturatedBlock := !sd.HasBlockedRatio || sd.BlockedRatio < a.thresholds.SaturatedBlock

	if sd.Utilization > a.thresholds.SaturatedBusy && saturatedIdle && saturatedBlock {
		return StateSaturated
	}

	return StateHealthy
}
