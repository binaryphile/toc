// Package sim provides a multi-stage discrete-event simulation for
// RL-based pipeline control experiments.
package sim

import (
	"container/heap"
	"math"
	"math/rand/v2"
)

// ── Event types ─────────────────────────────────────────────────────────

type eventType int

const (
	evServiceComplete eventType = iota // processed before arrivals at equal time
	evExternalArrival
)

type simEvent struct {
	time  float64
	typ   eventType
	stage int // stage index for service completions
	item  int // item index
	seq   int // creation order for FIFO tie-breaking
}

// Priority queue: earliest time first. At equal time: service before arrival,
// downstream stage before upstream, then FIFO by creation order.
type eventQueue []simEvent

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	a, b := q[i], q[j]
	if a.time != b.time {
		return a.time < b.time
	}
	if a.typ != b.typ {
		return a.typ < b.typ // evServiceComplete(0) < evExternalArrival(1)
	}
	if a.typ == evServiceComplete && a.stage != b.stage {
		return a.stage > b.stage // downstream first (higher index)
	}
	return a.seq < b.seq // FIFO
}
func (q eventQueue) Swap(i, j int)       { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)         { *q = append(*q, x.(simEvent)) }
func (q *eventQueue) Pop() any           { old := *q; n := len(old); x := old[n-1]; *q = old[:n-1]; return x }

// ── Item tracking ───────────────────────────────────────────────────────

type itemState int

const (
	itemSourceBacklog itemState = iota
	itemQueued
	itemInService
	itemBlockedAfterService
	itemInBuffer // in dedicated buffer queue before constraint
	itemComplete
)

type item struct {
	state          itemState
	stage          int     // current stage (-1 for source backlog, N for complete)
	arriveAt       float64 // time entered source/system
	constraintCost float64 // expected service time at constraint (for time-buffer)
}

// ── Stage state ─────────────────────────────────────────────────────────

type stageState struct {
	queued              []int // item indices, FIFO
	inServiceCount      int
	blockedAfterService []int // item indices, FIFO (oldest blocked first)
	workers             int
	serviceMean         float64
	rng                 *rand.Rand
}

// wip returns total items at this stage.
// inServiceCount includes BAS items (they hold a server), so don't add BAS separately.
func (s *stageState) wip() int {
	return len(s.queued) + s.inServiceCount
}

// freeServers returns available server capacity.
// inServiceCount includes BAS items, so subtracting it accounts for both.
func (s *stageState) freeServers() int {
	return s.workers - s.inServiceCount
}

func (s *stageState) drawServiceTime() float64 {
	return -s.serviceMean * math.Log(1.0-s.rng.Float64())
}

// ── DES engine ──────────────────────────────────────────────────────────

type desEngine struct {
	stages        []stageState
	items         []item
	sourceBacklog []int // item indices, FIFO
	eq            eventQueue
	seqCounter    int
	simTime       float64
	completed     int
	totalArrivals int

	arrivalRNG          *rand.Rand
	arrivalMean         float64
	constraintCostPerItem float64 // expected service time at constraint per item

	// DBR controls.
	constraintIdx      int     // which stage is the constraint
	bufferQueue        []int   // dedicated buffer in front of constraint, FIFO
	bufferTime         float64 // sum of constraintCost for items in buffer
	bufferMaxTime      float64 // current time cap (set by action)
	ropeRate           int     // items per interval from source
	intervalAdmissions int     // items admitted this interval (rope counter)

	// Safety.
	maxSystemItems int // hard cap; 0 = unlimited
	safetyBreached bool

	// Interval accumulators.
	intervalCompletions int
	intervalWIPArea     float64 // time-integrated total WIP
	intervalBacklogArea float64
	lastWIPUpdateTime   float64
}

func newDES(cfg EnvConfig, seed uint64) *desEngine {
	stages := make([]stageState, len(cfg.Stages))
	for i, sc := range cfg.Stages {
		stages[i] = stageState{
			workers:     sc.Workers,
			serviceMean: sc.ServiceMean,
			rng:         rand.New(rand.NewPCG(seed, uint64(i+1))),
		}
	}

	constraintCost := cfg.Stages[cfg.ConstraintStage].ServiceMean

	d := &desEngine{
		stages:              stages,
		arrivalRNG:          rand.New(rand.NewPCG(seed, 0)),
		arrivalMean:         cfg.ArrivalMean,
		constraintIdx:       cfg.ConstraintStage,
		constraintCostPerItem: constraintCost,
		maxSystemItems:      cfg.MaxSystemItems,
	}

	heap.Init(&d.eq)

	// Schedule first external arrival.
	ia := -cfg.ArrivalMean * math.Log(1.0-d.arrivalRNG.Float64())
	d.scheduleArrival(ia)

	return d
}

// scheduleArrival creates a future arrival item.
func (d *desEngine) scheduleArrival(t float64) {
	idx := len(d.items)
	d.items = append(d.items, item{
		state:          itemSourceBacklog,
		stage:          -1,
		arriveAt:       t,
		constraintCost: d.constraintCostPerItem,
	})
	heap.Push(&d.eq, simEvent{time: t, typ: evExternalArrival, item: idx, seq: d.nextSeq()})
}

func (d *desEngine) scheduleServiceComplete(stageIdx, itemIdx int, t float64) {
	heap.Push(&d.eq, simEvent{time: t, typ: evServiceComplete, stage: stageIdx, item: itemIdx, seq: d.nextSeq()})
}

func (d *desEngine) nextSeq() int {
	d.seqCounter++
	return d.seqCounter
}

// setDBRActions applies rope rate and buffer time cap.
func (d *desEngine) setDBRActions(ropeRate int, bufferMaxTime float64) {
	d.ropeRate = ropeRate
	d.bufferMaxTime = bufferMaxTime
}

// runUntil processes events until simTime reaches boundary (exclusive).
func (d *desEngine) runUntil(boundary float64) {
	for d.eq.Len() > 0 {
		if d.safetyBreached {
			break
		}
		ev := d.eq[0] // peek
		if ev.time >= boundary {
			break
		}
		heap.Pop(&d.eq)

		// Update WIP area before advancing time.
		d.updateWIPArea(ev.time)
		d.simTime = ev.time

		switch ev.typ {
		case evExternalArrival:
			d.handleArrival(ev)
		case evServiceComplete:
			d.handleServiceComplete(ev)
		}
	}

	// Final WIP area update to boundary.
	d.updateWIPArea(boundary)
	d.simTime = boundary
}

func (d *desEngine) updateWIPArea(t float64) {
	dt := t - d.lastWIPUpdateTime
	if dt <= 0 {
		return
	}
	totalWIP := len(d.bufferQueue)
	for i := range d.stages {
		totalWIP += d.stages[i].wip()
	}
	d.intervalWIPArea += float64(totalWIP) * dt
	d.intervalBacklogArea += float64(len(d.sourceBacklog)) * dt
	d.lastWIPUpdateTime = t
}

// handleArrival places arriving items in source backlog, then settles.
func (d *desEngine) handleArrival(ev simEvent) {
	d.totalArrivals++

	// Schedule next arrival.
	ia := -d.arrivalMean * math.Log(1.0-d.arrivalRNG.Float64())
	d.scheduleArrival(ev.time + ia)

	// All arrivals enter source backlog. Rope drains them.
	d.sourceBacklog = append(d.sourceBacklog, ev.item)
	d.drainSourceBacklog(ev.time)

	d.checkSafety()
}

func (d *desEngine) handleServiceComplete(ev simEvent) {
	stageIdx := ev.stage
	it := &d.items[ev.item]

	if stageIdx == len(d.stages)-1 {
		// Last stage: item completes.
		it.state = itemComplete
		it.stage = len(d.stages)
		d.stages[stageIdx].inServiceCount--
		d.completed++
		d.intervalCompletions++
	} else if d.constraintIdx > 0 && stageIdx == d.constraintIdx-1 {
		// Pre-constraint stage: item goes to buffer if time budget allows.
		if d.bufferTime+it.constraintCost <= d.bufferMaxTime {
			d.stages[stageIdx].inServiceCount--
			it.state = itemInBuffer
			it.stage = d.constraintIdx
			d.bufferQueue = append(d.bufferQueue, ev.item)
			d.bufferTime += it.constraintCost
		} else {
			// Buffer time exceeded: block at pre-constraint stage.
			it.state = itemBlockedAfterService
			d.stages[stageIdx].blockedAfterService = append(
				d.stages[stageIdx].blockedAfterService, ev.item)
		}
	} else {
		// Normal inter-stage transfer (no WIP caps, always admits).
		nextStage := stageIdx + 1
		d.stages[stageIdx].inServiceCount--
		d.admitToStage(nextStage, ev.item, ev.time)
	}

	// Settle all same-timestamp transitions to a fixed point.
	d.settle(ev.time)
}

// settle iterates zero-time transitions until no more progress is made.
// This ensures the constraint is never left idle when admissible work exists.
// Causal ordering: unblock BAS → feed constraint → dispatch, so each
// iteration propagates one full dependency chain without unnecessary laps.
func (d *desEngine) settle(t float64) {
	maxIter := 2*len(d.stages) + 10 // derived from topology, not magic
	for iter := 0; iter < maxIter; iter++ {
		progressed := false
		// 1. Unblock pre-constraint BAS into buffer (frees servers).
		progressed = d.unblockIntoBuffer(t) || progressed
		// 2. Feed constraint from buffer (uses freed buffer items).
		progressed = d.feedConstraintFromBuffer(t) || progressed
		// 3. Dispatch queued items at all stages (starts service).
		for si := range d.stages {
			progressed = d.tryDispatch(si, t) || progressed
		}
		if !progressed {
			break
		}
		if iter == maxIter-1 {
			panic("sim: settle loop did not converge — likely zero-time cycle")
		}
	}
	// 4. Drain source backlog (outside settle loop — rope-limited, not cascading).
	d.drainSourceBacklog(t)
}

// admitToStage adds item to stage queue.
func (d *desEngine) admitToStage(stageIdx, itemIdx int, t float64) {
	s := &d.stages[stageIdx]
	it := &d.items[itemIdx]
	it.state = itemQueued
	it.stage = stageIdx
	s.queued = append(s.queued, itemIdx)
}

// tryDispatch starts service for queued items if servers are free.
// Returns true if any item was dispatched.
func (d *desEngine) tryDispatch(stageIdx int, t float64) bool {
	s := &d.stages[stageIdx]
	dispatched := false
	for s.freeServers() > 0 && len(s.queued) > 0 {
		itemIdx := s.queued[0]
		s.queued = s.queued[1:]
		it := &d.items[itemIdx]
		it.state = itemInService
		s.inServiceCount++
		svcTime := s.drawServiceTime()
		d.scheduleServiceComplete(stageIdx, itemIdx, t+svcTime)
		dispatched = true
	}
	return dispatched
}

// feedConstraintFromBuffer moves items from the buffer queue into the
// constraint stage when the constraint has free servers.
// Returns true if any item was moved.
func (d *desEngine) feedConstraintFromBuffer(t float64) bool {
	if d.constraintIdx == 0 {
		return false
	}
	fed := false
	cs := &d.stages[d.constraintIdx]
	for cs.freeServers() > 0 && len(d.bufferQueue) > 0 {
		itemIdx := d.bufferQueue[0]
		d.bufferQueue = d.bufferQueue[1:]
		d.bufferTime -= d.items[itemIdx].constraintCost
		if d.bufferTime < -1e-9 {
			panic("sim: bufferTime went significantly negative — accounting bug")
		}
		if d.bufferTime < 0 {
			d.bufferTime = 0
		}
		d.admitToStage(d.constraintIdx, itemIdx, t)
		fed = true
	}
	return fed
}

// unblockIntoBuffer moves blocked items from the pre-constraint stage into
// the buffer when buffer time allows.
// Returns true if any item was moved.
func (d *desEngine) unblockIntoBuffer(t float64) bool {
	if d.constraintIdx == 0 {
		return false
	}
	preIdx := d.constraintIdx - 1
	pre := &d.stages[preIdx]
	moved := false
	for len(pre.blockedAfterService) > 0 {
		itemIdx := pre.blockedAfterService[0]
		cost := d.items[itemIdx].constraintCost
		if d.bufferTime+cost > d.bufferMaxTime {
			break
		}
		pre.blockedAfterService = pre.blockedAfterService[1:]
		pre.inServiceCount-- // server freed

		it := &d.items[itemIdx]
		it.state = itemInBuffer
		it.stage = d.constraintIdx
		d.bufferQueue = append(d.bufferQueue, itemIdx)
		d.bufferTime += cost
		moved = true
	}
	return moved
}

// drainSourceBacklog admits items from backlog to stage 0, rate-limited by rope.
func (d *desEngine) drainSourceBacklog(t float64) {
	for d.intervalAdmissions < d.ropeRate && len(d.sourceBacklog) > 0 {
		itemIdx := d.sourceBacklog[0]
		d.sourceBacklog = d.sourceBacklog[1:]
		d.intervalAdmissions++
		d.admitToStage(0, itemIdx, t)
	}
	// Dispatch newly admitted items at stage 0.
	d.tryDispatch(0, t)
}

// checkSafety terminates the episode if system items exceed the safety cap.
func (d *desEngine) checkSafety() {
	if d.maxSystemItems <= 0 {
		return
	}
	systemItems := len(d.sourceBacklog) + len(d.bufferQueue)
	for i := range d.stages {
		systemItems += d.stages[i].wip()
	}
	if systemItems > d.maxSystemItems {
		d.safetyBreached = true
	}
}

// snapshot returns current state observation.
func (d *desEngine) snapshot() Observation {
	stages := make([]StageObs, len(d.stages))
	totalWIP := len(d.bufferQueue)
	for i, s := range d.stages {
		stages[i] = StageObs{
			Queued:              len(s.queued),
			InService:           s.inServiceCount - len(s.blockedAfterService),
			BlockedAfterService: len(s.blockedAfterService),
			Workers:             s.workers,
		}
		totalWIP += s.wip()
	}
	return Observation{
		Stages:        stages,
		SourceBacklog: len(d.sourceBacklog),
		TotalWIP:      totalWIP,
		SimTime:       d.simTime,
		BufferDepth:   len(d.bufferQueue),
		BufferTime:    d.bufferTime,
		RopeRate:      d.ropeRate,
	}
}

// resetInterval clears interval accumulators.
func (d *desEngine) resetInterval() {
	d.intervalCompletions = 0
	d.intervalWIPArea = 0
	d.intervalBacklogArea = 0
	d.lastWIPUpdateTime = d.simTime
	d.intervalAdmissions = 0
}

// conservation checks the conservation invariant.
func (d *desEngine) conservation() bool {
	var backlog, inSystem, complete int
	for _, it := range d.items {
		switch it.state {
		case itemSourceBacklog:
			if it.stage == -1 {
				backlog++
			}
		case itemQueued, itemInService, itemBlockedAfterService, itemInBuffer:
			inSystem++
		case itemComplete:
			complete++
		}
	}
	return len(d.items) == backlog+inSystem+complete
}

// bufferTimeConsistent checks that bufferTime matches ground truth.
func (d *desEngine) bufferTimeConsistent() bool {
	var sum float64
	for _, idx := range d.bufferQueue {
		sum += d.items[idx].constraintCost
	}
	diff := d.bufferTime - sum
	if diff < 0 {
		diff = -diff
	}
	// Absolute + relative tolerance.
	tol := 1e-9
	if sum > 1 {
		tol = sum * 1e-9
	}
	return diff < tol
}
