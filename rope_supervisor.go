package toc

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// RopeResolverParams configures a [RopeResolver].
type RopeResolverParams struct {
	Pipeline      *Pipeline
	StageSnapshot func(string) IntervalStats
	SourceID      string // explicit, unique per LimitManager
	Interval      time.Duration
	WeightMode    bool
	Limits        *LimitManager // single LimitManager for all drums
	Drums         []RopeDrumConfig
	Options       []RopeOption
}

// RopeDrumConfig configures one potential drum target.
type RopeDrumConfig struct {
	Drum         string
	ControlStage string // "" = infer from HeadsTo
}

// ResolvedTarget is the immutable resolved configuration for one drum.
// Returned by [RopeResolver] and consumed by [RopeStarter].
type ResolvedTarget struct {
	Drum         string
	ControlStage string
	Segment      []string // defensive copy; safe to read, do not mutate
}

// RopeResolver maps drum names to prevalidated rope targets. Built once
// from a frozen Pipeline. All drums are eagerly validated at construction.
type RopeResolver struct {
	pipeline      *Pipeline
	stageSnapshot func(string) IntervalStats
	sourceID      string
	interval      time.Duration
	weightMode    bool
	limits        *LimitManager
	opts          []RopeOption
	resolved      map[string]ResolvedTarget
}

// NewRopeResolver creates a resolver that eagerly validates all drum
// configurations. Returns an error if any drum has an unsupported
// topology (non-linear segment, drum fan-in, etc.).
func NewRopeResolver(p RopeResolverParams) (*RopeResolver, error) {
	if p.Pipeline == nil {
		return nil, fmt.Errorf("toc.NewRopeResolver: Pipeline must not be nil")
	}
	if p.StageSnapshot == nil {
		return nil, fmt.Errorf("toc.NewRopeResolver: StageSnapshot must not be nil")
	}
	if p.SourceID == "" {
		return nil, fmt.Errorf("toc.NewRopeResolver: SourceID must not be empty")
	}
	if p.Interval <= 0 {
		return nil, fmt.Errorf("toc.NewRopeResolver: Interval must be positive")
	}
	if p.Limits == nil {
		return nil, fmt.Errorf("toc.NewRopeResolver: Limits must not be nil")
	}
	if len(p.Drums) == 0 {
		return nil, fmt.Errorf("toc.NewRopeResolver: at least one drum required")
	}

	resolved := make(map[string]ResolvedTarget, len(p.Drums))
	for _, dc := range p.Drums {
		if dc.Drum == "" {
			return nil, fmt.Errorf("toc.NewRopeResolver: drum name must not be empty")
		}
		if _, dup := resolved[dc.Drum]; dup {
			return nil, fmt.Errorf("toc.NewRopeResolver: duplicate drum %q", dc.Drum)
		}

		// Check drum existence before HeadsTo (which panics on unknown stage).
		if _, ok := p.Pipeline.stages[dc.Drum]; !ok {
			return nil, fmt.Errorf("toc.NewRopeResolver: drum %q not found in pipeline", dc.Drum)
		}

		controlStage := dc.ControlStage
		if controlStage == "" {
			heads := p.Pipeline.HeadsTo(dc.Drum)
			if len(heads) != 1 {
				return nil, fmt.Errorf("toc.NewRopeResolver: drum %q has %d heads (need 1 or use ControlStage)", dc.Drum, len(heads))
			}
			controlStage = heads[0]
		}

		segment, err := p.Pipeline.ResolveSegment(controlStage, dc.Drum)
		if err != nil {
			return nil, fmt.Errorf("toc.NewRopeResolver: drum %q: %w", dc.Drum, err)
		}

		segCopy := make([]string, len(segment))
		copy(segCopy, segment)
		resolved[dc.Drum] = ResolvedTarget{
			Drum:         dc.Drum,
			ControlStage: controlStage,
			Segment:      segCopy,
		}
	}

	return &RopeResolver{
		pipeline:      p.Pipeline,
		stageSnapshot: p.StageSnapshot,
		sourceID:      p.SourceID,
		interval:      p.Interval,
		weightMode:    p.WeightMode,
		limits:        p.Limits,
		opts:          append([]RopeOption(nil), p.Options...), // defensive copy
		resolved:      resolved,
	}, nil
}

// resolve looks up a prevalidated drum target. Returns false for unknown drums.
func (r *RopeResolver) resolve(drum string) (ResolvedTarget, bool) {
	t, ok := r.resolved[drum]
	if !ok {
		return ResolvedTarget{}, false
	}
	// Defensive copy of segment slice.
	seg := make([]string, len(t.Segment))
	copy(seg, t.Segment)
	t.Segment = seg
	return t, true
}

// ── RopeHandle ──────────────────────────────────────────────────────────

// RopeHandle wraps a running RopeController with lifecycle management.
type RopeHandle struct {
	drum   string
	cancel context.CancelFunc
	done   chan struct{}
}

// Drum returns the drum this rope targets.
func (h *RopeHandle) Drum() string { return h.drum }

// Done returns a channel that closes when the rope exits.
func (h *RopeHandle) Done() <-chan struct{} { return h.done }

// Stop cancels the rope and waits for it to exit.
func (h *RopeHandle) Stop() {
	h.cancel()
	<-h.done
}

// ── RopeStarter ─────────────────────────────────────────────────────────

// RopeStarter launches a rope for a resolved target. Inject a mock for testing.
type RopeStarter interface {
	Start(target ResolvedTarget) (*RopeHandle, error)
}

// defaultRopeStarter constructs real RopeControllers.
type defaultRopeStarter struct {
	resolver *RopeResolver
}

func (s *defaultRopeStarter) Start(target ResolvedTarget) (*RopeHandle, error) {
	opts := make([]RopeOption, len(s.resolver.opts))
	copy(opts, s.resolver.opts)
	if target.ControlStage != "" {
		opts = append(opts, WithControlStage(target.ControlStage))
	}

	var rc *RopeController
	if s.resolver.weightMode {
		rc = NewWeightRopeController(
			s.resolver.pipeline,
			target.Drum,
			s.resolver.limits,
			s.resolver.stageSnapshot,
			s.resolver.interval,
			opts...,
		)
	} else {
		rc = NewRopeController(
			s.resolver.pipeline,
			target.Drum,
			s.resolver.limits,
			s.resolver.stageSnapshot,
			s.resolver.interval,
			opts...,
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		rc.Run(ctx)
		close(done)
	}()

	return &RopeHandle{drum: target.Drum, cancel: cancel, done: done}, nil
}

// ── RopeSupervisor ──────────────────────────────────────────────────────

// desiredSnapshot is an immutable desired-state value.
type desiredSnapshot struct {
	identified bool
	drum       string
}

// RopeSupervisorStatus is a point-in-time snapshot of the supervisor.
type RopeSupervisorStatus struct {
	Drum   string // "" when no active rope
	Active bool
}

const (
	defaultStopTimeout    = 5 * time.Second
	crashLoopMinHealthy   = 10 * time.Second // rope must run this long to reset backoff
	crashLoopInitBackoff  = 1 * time.Second
	crashLoopMaxBackoff   = 30 * time.Second
)

// RopeSupervisorOption configures a [RopeSupervisor].
type RopeSupervisorOption func(*RopeSupervisor)

// WithRopeSupervisorLogger sets the logger.
func WithRopeSupervisorLogger(l *log.Logger) RopeSupervisorOption {
	return func(rs *RopeSupervisor) {
		if l != nil {
			rs.logger = l
		}
	}
}

// RopeSupervisor manages rope lifecycle as the constraint shifts.
// It owns an internal goroutine that serializes transitions.
// Call [SetIdentified] / [ClearDesired] to publish desired state (non-blocking).
// Call [Stop] to shut down.
type RopeSupervisor struct {
	starter RopeStarter
	resolver *RopeResolver
	logger  *log.Logger

	desired atomic.Pointer[desiredSnapshot]
	status  atomic.Pointer[RopeSupervisorStatus]
	wake       chan struct{} // buffered(1), non-blocking signal
	stopCh     chan struct{} // closed to stop worker
	stopOnce   sync.Once
	workerDone chan struct{} // closed when worker exits

	stopTimeout time.Duration // max wait for handle.Stop; 0 = 5s default

	// Worker-only state (single writer).
	active      *RopeHandle
	currentDrum string
	lastStartAt time.Time     // for crash-loop detection
	backoff     time.Duration // current backoff delay
}

// NewRopeSupervisor creates a supervisor. Starts the internal worker goroutine.
func NewRopeSupervisor(resolver *RopeResolver, starter RopeStarter, opts ...RopeSupervisorOption) *RopeSupervisor {
	if resolver == nil {
		panic("toc.NewRopeSupervisor: resolver must not be nil")
	}
	if starter == nil {
		starter = &defaultRopeStarter{resolver: resolver}
	}

	rs := &RopeSupervisor{
		starter:    starter,
		resolver:   resolver,
		logger:     log.Default(),
		wake:       make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
		workerDone: make(chan struct{}),
	}
	rs.desired.Store(&desiredSnapshot{})
	rs.status.Store(&RopeSupervisorStatus{})

	for _, opt := range opts {
		opt(rs)
	}

	go rs.run()
	return rs
}

// SetIdentified publishes a desired drum. Non-blocking.
// Empty drum is ignored (use [ClearDesired] to stop).
func (rs *RopeSupervisor) SetIdentified(drum string) {
	if drum == "" {
		return // invalid; use ClearDesired
	}
	rs.desired.Store(&desiredSnapshot{identified: true, drum: drum})
	select {
	case rs.wake <- struct{}{}:
	default:
	}
}

// ClearDesired requests rope shutdown. Non-blocking.
func (rs *RopeSupervisor) ClearDesired() {
	rs.desired.Store(&desiredSnapshot{})
	select {
	case rs.wake <- struct{}{}:
	default:
	}
}

// Stop shuts down the supervisor and any active rope. Blocks until worker exits.
// Safe to call multiple times.
func (rs *RopeSupervisor) Stop() {
	rs.stopOnce.Do(func() { close(rs.stopCh) })
	<-rs.workerDone
}

// Status returns a coherent point-in-time snapshot.
func (rs *RopeSupervisor) Status() RopeSupervisorStatus {
	return *rs.status.Load()
}

func (rs *RopeSupervisor) run() {
	defer close(rs.workerDone)
	for {
		select {
		case <-rs.wake:
			rs.reconcile()
		case <-rs.stopCh:
			rs.stopActive()
			rs.status.Store(&RopeSupervisorStatus{})
			return
		}
	}
}

func (rs *RopeSupervisor) reconcile() {
	// 1. Refresh actual: detect unexpected exit.
	if rs.active != nil {
		select {
		case <-rs.active.Done():
			rs.logger.Print("[rope-supervisor] active rope exited unexpectedly")
			// Crash-loop detection: if rope ran for less than crashLoopMinHealthy, increase backoff.
			if time.Since(rs.lastStartAt) < crashLoopMinHealthy {
				if rs.backoff == 0 {
					rs.backoff = crashLoopInitBackoff
				} else {
					rs.backoff *= 2
					if rs.backoff > crashLoopMaxBackoff {
						rs.backoff = crashLoopMaxBackoff
					}
				}
				rs.logger.Printf("[rope-supervisor] crash-loop backoff: %v", rs.backoff)
			} else {
				rs.backoff = 0 // healthy run, reset backoff
			}
			rs.active = nil
			rs.currentDrum = ""
			rs.status.Store(&RopeSupervisorStatus{})
		default:
		}
	}

	// 2. Read latest desired.
	d := rs.desired.Load()

	// 3. Not identified → stop if active.
	if !d.identified {
		if rs.active != nil {
			rs.stopActive()
			rs.status.Store(&RopeSupervisorStatus{})
		}
		return
	}

	// 4. Same drum, rope alive → no-op.
	if d.drum == rs.currentDrum && rs.active != nil {
		return
	}

	// 5. Stop old rope if any.
	if rs.active != nil {
		rs.stopActive()

		// Re-read desired after blocking phase.
		d2 := rs.desired.Load()
		if d2.identified != d.identified || d2.drum != d.drum {
			// Desired changed during stop. Don't act on stale intent.
			// Update status (rope was stopped above). Will re-reconcile on next wake.
			rs.status.Store(&RopeSupervisorStatus{})
			return
		}
	}

	// 6. Resolve.
	target, ok := rs.resolver.resolve(d.drum)
	if !ok {
		rs.logger.Printf("[rope-supervisor] unknown drum %q, fail-closed", d.drum)
		rs.currentDrum = ""
		rs.status.Store(&RopeSupervisorStatus{})
		return
	}

	// 7. Apply crash-loop backoff.
	if rs.backoff > 0 {
		rs.logger.Printf("[rope-supervisor] waiting %v before restart (crash-loop backoff)", rs.backoff)
		select {
		case <-time.After(rs.backoff):
		case <-rs.stopCh:
			rs.status.Store(&RopeSupervisorStatus{})
			return
		}
		// Re-read desired after backoff (changes during delay trigger re-reconcile via pending wake).
		d3 := rs.desired.Load()
		if d3.identified != d.identified || d3.drum != d.drum {
			rs.status.Store(&RopeSupervisorStatus{})
			return
		}
	}

	// 8. Start.
	handle, err := rs.starter.Start(target)
	if err != nil {
		rs.logger.Printf("[rope-supervisor] start failed for %q: %v", d.drum, err)
		rs.currentDrum = ""
		rs.status.Store(&RopeSupervisorStatus{})
		return
	}

	rs.active = handle
	rs.currentDrum = d.drum
	rs.lastStartAt = time.Now()
	rs.status.Store(&RopeSupervisorStatus{Drum: d.drum, Active: true})

	// Monitor for unexpected exit.
	go rs.watchHandle(handle)
}

func (rs *RopeSupervisor) stopActive() {
	if rs.active == nil {
		return
	}
	rs.active.cancel() // signal stop

	timeout := rs.stopTimeout
	if timeout == 0 {
		timeout = defaultStopTimeout
	}

	select {
	case <-rs.active.Done():
		// Clean shutdown.
	case <-time.After(timeout):
		rs.logger.Printf("[rope-supervisor] handle.Stop timed out after %v for drum %q (goroutine leak)", timeout, rs.active.drum)
		// Proceed anyway — the goroutine is leaked but the supervisor must not wedge.
	}
	rs.active = nil
	rs.currentDrum = ""
}

func (rs *RopeSupervisor) watchHandle(h *RopeHandle) {
	<-h.Done()
	// Don't mutate state — signal the worker.
	select {
	case rs.wake <- struct{}{}:
	default:
	}
}
