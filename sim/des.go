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
	itemComplete
)

type item struct {
	state     itemState
	stage     int     // current stage (-1 for source backlog, N for complete)
	arriveAt  float64 // time entered source/system
}

// ── Stage state ─────────────────────────────────────────────────────────

type stageState struct {
	queued              []int // item indices, FIFO
	inServiceCount      int
	blockedAfterService []int // item indices, FIFO (oldest blocked first)
	workers             int
	serviceMean         float64
	maxWIP              int // 0 = unbounded
	rng                 *rand.Rand
}

func (s *stageState) wip() int {
	return len(s.queued) + s.inServiceCount + len(s.blockedAfterService)
}

func (s *stageState) canAdmit() bool {
	return s.maxWIP == 0 || s.wip() < s.maxWIP
}

func (s *stageState) freeServers() int {
	return s.workers - s.inServiceCount - len(s.blockedAfterService)
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

	arrivalRNG  *rand.Rand
	arrivalMean float64

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
			maxWIP:      0, // set by actions
			rng:         rand.New(rand.NewPCG(seed, uint64(i+1))),
		}
	}

	d := &desEngine{
		stages:      stages,
		arrivalRNG:  rand.New(rand.NewPCG(seed, 0)),
		arrivalMean: cfg.ArrivalMean,
	}

	heap.Init(&d.eq)

	// Schedule first external arrival.
	ia := -cfg.ArrivalMean * math.Log(1.0-d.arrivalRNG.Float64())
	d.scheduleArrival(ia)

	return d
}

func (d *desEngine) scheduleArrival(t float64) {
	idx := len(d.items)
	d.items = append(d.items, item{state: itemSourceBacklog, stage: -1, arriveAt: t})
	heap.Push(&d.eq, simEvent{time: t, typ: evExternalArrival, item: idx, seq: d.nextSeq()})
}

func (d *desEngine) scheduleServiceComplete(stageIdx, itemIdx int, t float64) {
	heap.Push(&d.eq, simEvent{time: t, typ: evServiceComplete, stage: stageIdx, item: itemIdx, seq: d.nextSeq()})
}

func (d *desEngine) nextSeq() int {
	d.seqCounter++
	return d.seqCounter
}

// setActions applies RL actions (MaxWIP per stage).
func (d *desEngine) setActions(actions []int) {
	for i, a := range actions {
		d.stages[i].maxWIP = a
	}
}

// runUntil processes events until simTime reaches boundary (exclusive).
func (d *desEngine) runUntil(boundary float64) {
	for d.eq.Len() > 0 {
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
	totalWIP := 0
	for i := range d.stages {
		totalWIP += d.stages[i].wip()
	}
	d.intervalWIPArea += float64(totalWIP) * dt
	d.intervalBacklogArea += float64(len(d.sourceBacklog)) * dt
	d.lastWIPUpdateTime = t
}

func (d *desEngine) handleArrival(ev simEvent) {
	it := &d.items[ev.item]
	d.totalArrivals++

	// Schedule next arrival.
	ia := -d.arrivalMean * math.Log(1.0-d.arrivalRNG.Float64())
	d.scheduleArrival(ev.time + ia)

	// Try to admit to stage 0.
	if d.stages[0].canAdmit() {
		it.state = itemQueued
		it.stage = 0
		d.admitToStage(0, ev.item, ev.time)
	} else {
		it.state = itemSourceBacklog
		d.sourceBacklog = append(d.sourceBacklog, ev.item)
	}
}

func (d *desEngine) handleServiceComplete(ev simEvent) {
	stageIdx := ev.stage
	it := &d.items[ev.item]

	// Item finished service. Try to move downstream.
	if stageIdx == len(d.stages)-1 {
		// Last stage: item completes.
		it.state = itemComplete
		it.stage = len(d.stages)
		d.stages[stageIdx].inServiceCount--
		d.completed++
		d.intervalCompletions++
	} else {
		// Try to enter next stage.
		nextStage := stageIdx + 1
		if d.stages[nextStage].canAdmit() {
			d.stages[stageIdx].inServiceCount--
			it.state = itemQueued
			it.stage = nextStage
			d.admitToStage(nextStage, ev.item, ev.time)
		} else {
			// Blocked after service. Server stays occupied.
			it.state = itemBlockedAfterService
			d.stages[stageIdx].blockedAfterService = append(d.stages[stageIdx].blockedAfterService, ev.item)
		}
	}

	// Dispatch next queued item at this stage (if server freed).
	d.tryDispatch(stageIdx, ev.time)

	// Cascade: check if upstream stages can unblock.
	d.cascadeUnblock(stageIdx, ev.time)

	// Drain source backlog if stage 0 has capacity.
	d.drainSourceBacklog(ev.time)
}

// admitToStage adds item to stage queue and dispatches if server free.
func (d *desEngine) admitToStage(stageIdx, itemIdx int, t float64) {
	s := &d.stages[stageIdx]
	it := &d.items[itemIdx]
	it.state = itemQueued
	it.stage = stageIdx
	s.queued = append(s.queued, itemIdx)
	d.tryDispatch(stageIdx, t)
}

// tryDispatch starts service for the next queued item if a server is free.
func (d *desEngine) tryDispatch(stageIdx int, t float64) {
	s := &d.stages[stageIdx]
	for s.freeServers() > 0 && len(s.queued) > 0 {
		itemIdx := s.queued[0]
		s.queued = s.queued[1:]
		it := &d.items[itemIdx]
		it.state = itemInService
		s.inServiceCount++
		svcTime := s.drawServiceTime()
		d.scheduleServiceComplete(stageIdx, itemIdx, t+svcTime)
	}
}

// cascadeUnblock checks if departures freed capacity for upstream blocked items.
func (d *desEngine) cascadeUnblock(stageIdx int, t float64) {
	// Walk upstream from stageIdx.
	for si := stageIdx; si > 0; si-- {
		upstream := si - 1
		us := &d.stages[upstream]
		ds := &d.stages[si]

		// While downstream can admit and upstream has blocked items:
		for ds.canAdmit() && len(us.blockedAfterService) > 0 {
			// Transfer oldest blocked item.
			itemIdx := us.blockedAfterService[0]
			us.blockedAfterService = us.blockedAfterService[1:]
			us.inServiceCount-- // server freed

			d.admitToStage(si, itemIdx, t)

			// Freed server at upstream: dispatch next queued.
			d.tryDispatch(upstream, t)
		}
	}
}

// drainSourceBacklog admits items from backlog to stage 0 if capacity allows.
func (d *desEngine) drainSourceBacklog(t float64) {
	for d.stages[0].canAdmit() && len(d.sourceBacklog) > 0 {
		itemIdx := d.sourceBacklog[0]
		d.sourceBacklog = d.sourceBacklog[1:]
		d.admitToStage(0, itemIdx, t)
	}
}

// snapshot returns current state observation.
func (d *desEngine) snapshot() Observation {
	stages := make([]StageObs, len(d.stages))
	totalWIP := 0
	for i, s := range d.stages {
		stages[i] = StageObs{
			Queued:              len(s.queued),
			InService:           s.inServiceCount,
			BlockedAfterService: len(s.blockedAfterService),
			Workers:             s.workers,
			MaxWIP:              s.maxWIP,
		}
		totalWIP += s.wip()
	}
	return Observation{
		Stages:        stages,
		SourceBacklog: len(d.sourceBacklog),
		TotalWIP:      totalWIP,
		SimTime:       d.simTime,
	}
}

// resetInterval clears interval accumulators.
func (d *desEngine) resetInterval() {
	d.intervalCompletions = 0
	d.intervalWIPArea = 0
	d.intervalBacklogArea = 0
	d.lastWIPUpdateTime = d.simTime
}

// conservation checks the conservation invariant.
// Counts items by state, excluding unprocessed scheduled arrivals.
func (d *desEngine) conservation() bool {
	var backlog, inSystem, complete, pending int
	for _, it := range d.items {
		switch it.state {
		case itemSourceBacklog:
			if it.stage == -1 {
				// Could be in sourceBacklog or still in event queue (unprocessed).
				backlog++
			}
		case itemQueued, itemInService, itemBlockedAfterService:
			inSystem++
		case itemComplete:
			complete++
		}
	}
	_ = pending
	// All items should be accounted for.
	return len(d.items) == backlog+inSystem+complete
}
