// Package main demonstrates why queues form, using a single-station
// discrete-event simulation with a "lunch counter" metaphor.
//
// Six scenarios progress from lockstep (no queue possible) through
// D/D/1, M/D/1, D/M/1, M/M/1, to overloaded — showing that variability
// creates queues even when average capacity exceeds average demand.
//
// Run:
//
//	go run ./examples/queue-demo/
package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ── Simulation types ────────────────────────────────────────────────────

type eventType int

const (
	evArrival   eventType = iota
	evDepart              // departure = service complete
)

type simEvent struct {
	time    float64   // simulated minutes
	typ     eventType
	custIdx int       // index into customers slice
}

// Priority queue for DES events (earliest first, departure before arrival on tie).
type eventQueue []simEvent

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].time != q[j].time {
		return q[i].time < q[j].time
	}
	// Tie-break: departure before arrival.
	return q[i].typ > q[j].typ // evDepart(1) > evArrival(0)
}
func (q eventQueue) Swap(i, j int)       { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)         { *q = append(*q, x.(simEvent)) }
func (q *eventQueue) Pop() any           { old := *q; n := len(old); x := old[n-1]; *q = old[:n-1]; return x }

type customer struct {
	arrival      float64
	serviceStart float64
	completion   float64
}

type logEntry struct {
	time       float64
	typ        eventType
	custIdx    int  // customer index for this event
	queueDepth int  // queue length AFTER event (excluding in-service)
	serverBusy bool
}

type simResult struct {
	customers []customer
	log       []logEntry
	endTime   float64
}

// ── DES engine ──────────────────────────────────────────────────────────

type simConfig struct {
	arrivals []float64 // interarrival times (or nil for lockstep)
	services []float64 // service times
	lockstep bool      // if true, next arrival triggered by departure
	maxTime  float64   // 0 = no time limit (run until all done)
}

func runSim(cfg simConfig) simResult {
	var (
		customers []customer
		log       []logEntry
		eq        eventQueue
		queue     []int // indices of customers waiting in FIFO
		busy bool
	)

	heap.Init(&eq)

	record := func(t float64, typ eventType, custIdx int, qDepth int, serverBusy bool) {
		log = append(log, logEntry{time: t, typ: typ, custIdx: custIdx, queueDepth: qDepth, serverBusy: serverBusy})
	}

	// Schedule first arrival.
	if cfg.lockstep {
		// Lockstep: first arrival at t = service time of a "virtual" zero-th customer.
		// Effectively: first customer arrives at t = services[0] wait... no.
		// Actually for lockstep: first arrival at t=0 makes sense since there's no
		// prior customer. The NEXT arrival is triggered by the departure.
		customers = append(customers, customer{arrival: 0})
		heap.Push(&eq, simEvent{time: 0, typ: evArrival, custIdx: 0})
	} else if len(cfg.arrivals) > 0 {
		// First arrival at t = first interarrival time (not t=0).
		t := cfg.arrivals[0]
		customers = append(customers, customer{arrival: t})
		heap.Push(&eq, simEvent{time: t, typ: evArrival, custIdx: 0})
	}

	nextArrivalIdx := 1 // index into cfg.arrivals for next arrival

	scheduleNextArrival := func(afterTime float64) {
		if cfg.lockstep {
			// Lockstep: next arrival immediately after departure.
			idx := len(customers)
			if idx >= len(cfg.services) {
				return // no more customers
			}
			customers = append(customers, customer{arrival: afterTime})
			heap.Push(&eq, simEvent{time: afterTime, typ: evArrival, custIdx: idx})
		} else {
			if nextArrivalIdx >= len(cfg.arrivals) {
				return
			}
			t := customers[len(customers)-1].arrival + cfg.arrivals[nextArrivalIdx]
			if cfg.maxTime > 0 && t > cfg.maxTime {
				return // past time horizon
			}
			nextArrivalIdx++
			idx := len(customers)
			customers = append(customers, customer{arrival: t})
			heap.Push(&eq, simEvent{time: t, typ: evArrival, custIdx: idx})
		}
	}

	startService := func(custIdx int, t float64) {
		customers[custIdx].serviceStart = t
		svcIdx := custIdx
		if svcIdx >= len(cfg.services) {
			svcIdx = len(cfg.services) - 1 // reuse last service time
		}
		dur := cfg.services[svcIdx]
		heap.Push(&eq, simEvent{time: t + dur, typ: evDepart, custIdx: custIdx})
		busy = true
	}

	for eq.Len() > 0 {
		ev := heap.Pop(&eq).(simEvent)

		if cfg.maxTime > 0 && ev.time > cfg.maxTime {
			break
		}

		switch ev.typ {
		case evArrival:
			if !busy {
				startService(ev.custIdx, ev.time)
				record(ev.time, evArrival, ev.custIdx, len(queue), true)
			} else {
				queue = append(queue, ev.custIdx)
				record(ev.time, evArrival, ev.custIdx, len(queue), true)
			}
			if !cfg.lockstep {
				scheduleNextArrival(ev.time)
			}

		case evDepart:
			customers[ev.custIdx].completion = ev.time
			if len(queue) > 0 {
				next := queue[0]
				queue = queue[1:]
				startService(next, ev.time)
				record(ev.time, evDepart, ev.custIdx, len(queue), true)
			} else {
				busy = false
				record(ev.time, evDepart, ev.custIdx, 0, false)
			}
			if cfg.lockstep {
				scheduleNextArrival(ev.time)
			}
		}
	}

	endTime := 0.0
	if len(log) > 0 {
		endTime = log[len(log)-1].time
	}
	if cfg.maxTime > 0 {
		endTime = cfg.maxTime
	}

	return simResult{customers: customers, log: log, endTime: endTime}
}

// ── Metrics ─────────────────────────────────────────────────────────────

type metrics struct {
	served      int
	arrivedOnly int     // arrived but not completed (overloaded)
	throughput  float64 // cust/hr
	peakQ       int
	avgQ        float64 // L_q: event-time integrated
	avgWait     float64 // W_q: mean(serviceStart - arrival) over completed
	util        float64 // fraction of time busy
	lambda      float64 // observed arrival rate (arrivals/hr)
	littleLHS   float64 // L_q
	littleRHS   float64 // λ × W_q
}

func computeMetrics(r simResult) metrics {
	var m metrics

	// Count served and arrivals.
	for _, c := range r.customers {
		if c.completion > 0 {
			m.served++
		} else {
			m.arrivedOnly++
		}
	}

	if r.endTime <= 0 {
		return m
	}

	// Throughput.
	m.throughput = float64(m.served) / (r.endTime / 60.0) // per hour

	// Avg wait (W_q) — only over completed customers.
	var totalWait float64
	var waitCount int
	for _, c := range r.customers {
		if c.completion > 0 {
			w := c.serviceStart - c.arrival
			totalWait += w
			waitCount++
		}
	}
	if waitCount > 0 {
		m.avgWait = totalWait / float64(waitCount)
	}

	// Event-time integrated queue depth (L_q) and utilization.
	var queueArea float64
	var busyTime float64
	prevTime := 0.0
	prevDepth := 0
	prevBusy := false
	for _, e := range r.log {
		dt := e.time - prevTime
		queueArea += float64(prevDepth) * dt
		if prevBusy {
			busyTime += dt
		}
		if e.queueDepth > m.peakQ {
			m.peakQ = e.queueDepth
		}
		prevTime = e.time
		prevDepth = e.queueDepth
		prevBusy = e.serverBusy
	}
	// Final segment to endTime.
	dt := r.endTime - prevTime
	queueArea += float64(prevDepth) * dt
	if prevBusy {
		busyTime += dt
	}

	m.avgQ = queueArea / r.endTime
	m.util = busyTime / r.endTime

	// Observed arrival rate.
	totalArrivals := m.served + m.arrivedOnly
	m.lambda = float64(totalArrivals) / (r.endTime / 60.0)

	// Little's Law: L_q ≈ λ_per_min × W_q.
	lambdaPerMin := float64(totalArrivals) / r.endTime
	m.littleLHS = m.avgQ
	m.littleRHS = lambdaPerMin * m.avgWait

	return m
}

// ── Trace generation ────────────────────────────────────────────────────

func genExponentialTrace(n int, mean float64, seed uint64) []float64 {
	rng := rand.New(rand.NewPCG(seed, seed^0xdeadbeef))
	trace := make([]float64, n)
	for i := range trace {
		trace[i] = -mean * math.Log(1.0-rng.Float64())
	}
	return trace
}

func genFixedTrace(n int, value float64) []float64 {
	trace := make([]float64, n)
	for i := range trace {
		trace[i] = value
	}
	return trace
}

func scaleTrace(trace []float64, factor float64) []float64 {
	scaled := make([]float64, len(trace))
	for i, v := range trace {
		scaled[i] = v * factor
	}
	return scaled
}

// ── ANSI / terminal ─────────────────────────────────────────────────────

var (
	colorRed    = "\033[31m"
	colorYellow = "\033[33m"
	colorGreen  = "\033[32m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
	colorReset  = "\033[0m"
)

const (
	boxTL = "╭"
	boxTR = "╮"
	boxBL = "╰"
	boxBR = "╯"
	boxH  = "─"
	boxV  = "│"
	boxML = "├"
	boxMR = "┤"
)

type terminal struct {
	isTTY   bool
	noColor bool
}

func newTerminal() terminal {
	fi, err := os.Stdout.Stat()
	isTTY := err == nil && fi.Mode()&os.ModeCharDevice != 0
	_, noColor := os.LookupEnv("NO_COLOR")
	t := terminal{isTTY: isTTY, noColor: noColor}
	if noColor || !isTTY {
		disableColors()
	}
	return t
}

func disableColors() {
	colorRed = ""
	colorYellow = ""
	colorGreen = ""
	colorBold = ""
	colorDim = ""
	colorReset = ""
}

func (t terminal) enterAltScreen() {
	if t.isTTY {
		fmt.Print("\033[?1049h")
	}
}
func (t terminal) exitAltScreen() {
	if t.isTTY {
		fmt.Print("\033[?1049l")
	}
}
func (t terminal) hideCursor() {
	if t.isTTY {
		fmt.Print("\033[?25l")
	}
}
func (t terminal) showCursor() {
	if t.isTTY {
		fmt.Print("\033[?25h")
	}
}
func (t terminal) home() {
	if t.isTTY {
		fmt.Print("\033[H")
	}
}

// ── Scenario config ─────────────────────────────────────────────────────

const (
	serviceMean     = 3.0           // minutes
	interarrivalRho = 10.0 / 3.0   // 3 / 0.9 = 10/3 ≈ 3.33 min
	nearFullIA      = 60.0 / 19.0   // 3 / 0.95 ≈ 3.16 min
	nearFullScale   = 0.9 / 0.95    // scale ρ=0.9 trace → ρ=0.95
	overloadedIA    = 2.0           // 3 / 1.5 = 2 min
	overloadScale   = 0.6           // 0.9 / 1.5
	playbackSpeed   = 360.0         // sim minutes per wall minute
	renderInterval  = 250 * time.Millisecond // 4 FPS
	simPerFrame     = playbackSpeed / 60.0 * 0.25 // sim min per 250ms frame

	arrivalSeed = 42
	serviceSeed = 137
)

type scenario struct {
	name        string
	label       string // short display label
	desc        string // arrival/service one-liner
	analogy     string
	modelNote   string // queuing theory note
	watchFor    []string
	config      simConfig
	isOverload  bool
	displayCap  int // visual bar cap
	totalExpect int // expected total items (for "Served x/y")
}

func buildScenarios(arrivalTrace, serviceTrace []float64) []scenario {
	return []scenario{
		{
			name:    "Lockstep",
			label:   "Lockstep",
			desc:    "Gated arrivals, fixed 3 min service",
			analogy: "A sushi boat — the chef places a plate, it circles to you, you grab it, the empty spot returns.",
			modelNote: "Not a standard queuing model. Arrivals are synchronized to completions (gated/pull).",
			watchFor:  []string{"Queue is always 0. Throughput is perfectly constant."},
			config: simConfig{
				services: genFixedTrace(10, serviceMean),
				lockstep: true,
			},
			displayCap:  20,
			totalExpect: 10,
		},
		{
			name:    "Fixed Schedule (D/D/1)",
			label:   "D/D/1",
			desc:    fmt.Sprintf("Fixed arrival every %.1f min, fixed 3 min service, ρ=0.9", interarrivalRho),
			analogy: "A merry-go-round — kids arrive steadily, each ride is exactly 3 minutes, always an empty horse.",
			modelNote: "Queuing theory: D/D/1. Both sides deterministic.",
			watchFor:  []string{"Queue stays at 0. The buffer exists but is never used."},
			config: simConfig{
				arrivals: genFixedTrace(10, interarrivalRho),
				services: genFixedTrace(10, serviceMean),
			},
			displayCap:  20,
			totalExpect: 10,
		},
		{
			name:    "Random Arrivals (M/D/1)",
			label:   "M/D/1",
			desc:    fmt.Sprintf("Poisson arrivals (avg %.1f min), fixed 3 min service, ρ=0.9", interarrivalRho),
			analogy: "The bathroom at a house party — the line forms because three people all need it at 10:15.",
			modelNote: "Queuing theory: M/D/1. Variable arrivals, constant service.",
			watchFor:  []string{"Queue fluctuates even though average arrival < service rate.", "Arrival variability alone creates queues."},
			config: simConfig{
				arrivals: arrivalTrace[:50],
				services: genFixedTrace(50, serviceMean),
			},
			displayCap:  20,
			totalExpect: 50,
		},
		{
			name:    "Random Service (D/M/1)",
			label:   "D/M/1",
			desc:    fmt.Sprintf("Fixed arrival every %.1f min, exp service (avg 3 min), ρ=0.9", interarrivalRho),
			analogy: "A dentist with 30-min appointments — some need a cleaning, some a root canal.",
			modelNote: "Queuing theory: D/M/1. Constant arrivals, variable service.",
			watchFor:  []string{"Queue fluctuates from service variability.", "A slow customer blocks the line."},
			config: simConfig{
				arrivals: genFixedTrace(50, interarrivalRho),
				services: serviceTrace[:50],
			},
			displayCap:  20,
			totalExpect: 50,
		},
		{
			name:    "Random Everything (M/M/1)",
			label:   "M/M/1",
			desc:    fmt.Sprintf("Poisson arrivals (avg %.1f min), exp service (avg 3 min), ρ=0.9", interarrivalRho),
			analogy: "Thanksgiving bathroom — Uncle Jerry takes 15 min, Grandma takes 45 sec, nobody's on a schedule.",
			modelNote: "Queuing theory: M/M/1. Both sides random. This is how real systems behave.",
			watchFor:  []string{"Deeper queue swings than scenarios 3 or 4 alone.", "Both sources of variability compound."},
			config: simConfig{
				arrivals: arrivalTrace[:50],
				services: serviceTrace[:50],
			},
			displayCap:  20,
			totalExpect: 50,
		},
		{
			name:    "Near Full Load (M/M/1, ρ=0.95)",
			label:   "Near Full",
			desc:    fmt.Sprintf("Poisson arrivals (avg %.1f min), exp service (avg 3 min), ρ=0.95", nearFullIA),
			analogy: "A highway running at 95%% capacity — looks fine on paper, but one slow merge and traffic backs up for miles.",
			modelNote: "Same M/M/1 but ρ raised from 0.9 to 0.95. Only 5% less slack.",
			watchFor: []string{
				"Queue builds higher and recovers slower than at ρ=0.9.",
				"Averages say this should be fine. Watch what actually happens.",
			},
			config: simConfig{
				arrivals: scaleTrace(arrivalTrace[:80], nearFullScale),
				services: serviceTrace[:80],
			},
			displayCap:  30,
			totalExpect: 80,
		},
		{
			name:    "Overloaded (M/M/1, ρ=1.5)",
			label:   "Overloaded",
			desc:    fmt.Sprintf("Poisson arrivals (avg %.1f min), exp service (avg 3 min), ρ=1.5", overloadedIA),
			analogy: "The DMV at 8:01 AM — the doors just opened and there are already 40 people and one clerk.",
			modelNote: "Same M/M/1 but arrival rate exceeds service rate. Queue cannot stabilize.",
			watchFor:  []string{"Backlog trends upward and does not stabilize.", "No buffer is large enough when demand exceeds capacity."},
			config: simConfig{
				arrivals: scaleTrace(arrivalTrace, overloadScale),
				services: serviceTrace,
				maxTime:  120.0,
			},
			isOverload: true,
			displayCap: 60,
		},
	}
}

// ── Preamble ────────────────────────────────────────────────────────────

func printIntro() {
	fmt.Println()
	fmt.Println("================================================================")
	fmt.Println()
	fmt.Println("  THE LUNCH COUNTER")
	fmt.Println()
	fmt.Println("  You've waited in line. At the grocery store. In traffic.")
	fmt.Println("  At the bathroom at a party. This demo shows why queues")
	fmt.Println("  form -- and it's not because the cashier is slow.")
	fmt.Println()
	fmt.Println("  A single cashier serves customers one at a time.")
	fmt.Println("  Average service: 3 minutes. We'll vary how customers")
	fmt.Println("  arrive and how long service takes.")
	fmt.Println()
	fmt.Println("  All times in simulated minutes. Playback at 360x.")
	fmt.Println()
	fmt.Println("================================================================")
	fmt.Println()
}

func printPreamble(sc scenario, isTTY bool) {
	fmt.Printf("  %s%s%s\n", colorBold, sc.name, colorReset)
	fmt.Printf("  %s%s%s\n", colorDim, sc.analogy, colorReset)
	fmt.Println()
	fmt.Printf("  %s\n", sc.modelNote)
	fmt.Printf("  %s\n", sc.desc)
	fmt.Println()
	fmt.Println("  What to watch:")
	for _, w := range sc.watchFor {
		fmt.Printf("    - %s\n", w)
	}
	fmt.Println()
	if isTTY {
		fmt.Print("  [press enter to start]")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
	}
	fmt.Println()
}

// ── Playback renderer ───────────────────────────────────────────────────

const frameWidth = 60

func renderFrame(sc scenario, simTime float64, queueDepth int, serverBusy bool,
	served, total int, throughput, avgWait float64, isOverload bool, arrived int) string {

	var b strings.Builder

	b.WriteString(boxTL + strings.Repeat(boxH, frameWidth) + boxTR + "\n")

	// Header.
	var header string
	if isOverload {
		header = fmt.Sprintf(" %s%-34s%s Arrived:%d Done:%d",
			colorBold, sc.name, colorReset, arrived, served)
	} else {
		header = fmt.Sprintf(" %s%-34s%s  Served: %d/%d",
			colorBold, sc.name, colorReset, served, total)
	}
	b.WriteString(boxV + padRight(header, frameWidth) + boxV + "\n")

	descLine := fmt.Sprintf(" %s%s%s", colorDim, sc.desc, colorReset)
	b.WriteString(boxV + padRight(descLine, frameWidth) + boxV + "\n")

	b.WriteString(boxML + strings.Repeat(boxH, frameWidth) + boxMR + "\n")
	b.WriteString(boxV + strings.Repeat(" ", frameWidth) + boxV + "\n")

	// Queue bar.
	if sc.config.lockstep && queueDepth == 0 {
		qLine := fmt.Sprintf(" Waiting       %sNo queue (gated)%s", colorDim, colorReset)
		b.WriteString(boxV + padRight(qLine, frameWidth) + boxV + "\n")
	} else {
		barStr := renderQueueBar(queueDepth, sc.displayCap)
		depthStr := fmt.Sprintf("%d", queueDepth)
		if queueDepth > sc.displayCap {
			depthStr = fmt.Sprintf("%d+", queueDepth)
		}
		qLine := fmt.Sprintf(" Waiting       %s  %s", barStr, depthStr)
		b.WriteString(boxV + padRight(qLine, frameWidth) + boxV + "\n")
	}

	b.WriteString(boxV + strings.Repeat(" ", frameWidth) + boxV + "\n")

	// Cashier + sim time.
	status := colorDim + "[idle]" + colorReset
	if serverBusy {
		status = colorGreen + "[serving]" + colorReset
	}
	cashierLine := fmt.Sprintf(" Cashier %s    Sim time: %.0f min", status, simTime)
	b.WriteString(boxV + padRight(cashierLine, frameWidth) + boxV + "\n")

	// Throughput + avg wait.
	var statsLine string
	if served > 0 {
		if avgWait >= 0.05 {
			statsLine = fmt.Sprintf(" Throughput: %.0f cust/hr    Avg wait: %.1f min", throughput, avgWait)
		} else {
			statsLine = fmt.Sprintf(" Throughput: %.0f cust/hr    Avg wait: 0 min", throughput)
		}
	} else {
		statsLine = " Throughput: —    Avg wait: —"
	}
	b.WriteString(boxV + padRight(statsLine, frameWidth) + boxV + "\n")

	b.WriteString(boxBL + strings.Repeat(boxH, frameWidth) + boxBR + "\n")
	return b.String()
}

func renderQueueBar(depth, displayCap int) string {
	const barWidth = 30
	if displayCap <= 0 {
		displayCap = 1
	}
	filled := depth
	if filled > displayCap {
		filled = displayCap
	}
	filledChars := int(math.Round(float64(filled) / float64(displayCap) * float64(barWidth)))
	if filledChars > barWidth {
		filledChars = barWidth
	}
	emptyChars := barWidth - filledChars

	pct := float64(filled) / float64(displayCap)
	color := colorGreen
	switch {
	case pct > 0.75:
		color = colorRed
	case pct > 0.5:
		color = colorYellow
	}

	return color + strings.Repeat("▓", filledChars) + colorReset + strings.Repeat("░", emptyChars)
}

// playbackScenario animates a completed simulation result.
func playbackScenario(sc scenario, r simResult, m metrics, term terminal) {
	term.enterAltScreen()
	term.hideCursor()
	defer func() {
		time.Sleep(2 * time.Second)
		term.exitAltScreen()
		term.showCursor()
	}()

	ticker := time.NewTicker(renderInterval)
	defer ticker.Stop()

	// Walk event log.
	logIdx := 0
	curDepth := 0
	curBusy := false
	servedSoFar := 0
	arrivedSoFar := 0
	var totalWait float64

	for simTime := 0.0; simTime <= r.endTime; {
		<-ticker.C

		// Advance sim time by one frame.
		simTime += simPerFrame
		if simTime > r.endTime {
			simTime = r.endTime
		}

		// Process all events up to simTime.
		for logIdx < len(r.log) && r.log[logIdx].time <= simTime {
			e := r.log[logIdx]
			curDepth = e.queueDepth
			curBusy = e.serverBusy
			if e.typ == evDepart {
				c := r.customers[e.custIdx]
				if c.completion > 0 {
					servedSoFar++
					totalWait += c.serviceStart - c.arrival
				}
			}
			if e.typ == evArrival {
				arrivedSoFar++
			}
			logIdx++
		}

		// Compute live metrics.
		tput := 0.0
		avgW := 0.0
		if servedSoFar > 0 {
			tput = float64(servedSoFar) / (simTime / 60.0)
			avgW = totalWait / float64(servedSoFar)
		}

		term.home()
		fmt.Print(renderFrame(sc, simTime, curDepth, curBusy,
			servedSoFar, sc.totalExpect, tput, avgW,
			sc.isOverload, arrivedSoFar))
	}
}

// ── Comparison table ────────────────────────────────────────────────────

func printComparison(scenarios []scenario, results []simResult, allMetrics []metrics) {
	fmt.Println(divider("THE LUNCH COUNTER — RESULTS"))
	fmt.Println()

	const nameCol = 34
	fmt.Printf("  %s │ %6s │ %7s │ %6s │ %5s │ %8s │ %5s\n",
		padRight("Scenario", nameCol), "served", "cust/hr", "peak q", "avg q", "avg wait", "util")
	fmt.Println("  " + strings.Repeat("─", 90))

	for i, m := range allMetrics {
		sc := scenarios[i]
		waitStr := "—"
		if m.served > 0 && !sc.config.lockstep {
			waitStr = fmt.Sprintf("%.1fmin", m.avgWait)
		}
		if sc.config.lockstep {
			waitStr = "—"
		}

		qColor := colorGreen
		if m.peakQ > 10 {
			qColor = colorRed
		} else if m.peakQ > 5 {
			qColor = colorYellow
		}

		fmt.Printf("  %s │ %6d │ %7.1f │ %s%6d%s │ %5.1f │ %8s │ %4.0f%%\n",
			padRight(sc.name, nameCol), m.served, m.throughput,
			qColor, m.peakQ, colorReset,
			m.avgQ, waitStr, m.util*100)
	}

	// Queue depth sparklines.
	globalMaxQ := 0
	for _, m := range allMetrics {
		if m.peakQ > globalMaxQ {
			globalMaxQ = m.peakQ
		}
	}
	if globalMaxQ == 0 {
		globalMaxQ = 1
	}

	fmt.Println()
	fmt.Println("  " + colorBold + "Queue depth over time:" + colorReset)
	fmt.Println()
	for i, r := range results {
		spark := queueSparkline(r, globalMaxQ)
		fmt.Printf("  %s %s\n", padRight(scenarios[i].name+":", nameCol), spark)
	}

	// Little's Law verification (scenarios 3-5 only).
	fmt.Println()
	fmt.Println("  " + colorBold + "Little's Law (L_q ≈ λ × W_q):" + colorReset)
	fmt.Println()
	for i := 2; i <= 5 && i < len(allMetrics); i++ {
		m := allMetrics[i]
		if m.served > 0 {
			ratio := 0.0
			if m.littleRHS > 0 {
				ratio = m.littleLHS / m.littleRHS
			}
			fmt.Printf("  %s  L_q=%.2f  λW_q=%.2f  ratio=%.2f\n",
				padRight(scenarios[i].name, nameCol), m.littleLHS, m.littleRHS, ratio)
		}
	}
}

func queueSparkline(r simResult, maxQ int) string {
	if maxQ <= 0 {
		maxQ = 1
	}
	bars := []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}

	// Sample queue depth at regular intervals.
	const numSamples = 40
	step := r.endTime / float64(numSamples)
	if step <= 0 {
		return ""
	}

	var b strings.Builder
	logIdx := 0
	depth := 0
	for s := 0; s < numSamples; s++ {
		t := float64(s) * step
		for logIdx < len(r.log) && r.log[logIdx].time <= t {
			depth = r.log[logIdx].queueDepth
			logIdx++
		}

		pct := float64(depth) / float64(maxQ)
		if pct > 1 {
			pct = 1
		}
		idx := int(pct * float64(len(bars)-1))
		if idx >= len(bars) {
			idx = len(bars) - 1
		}
		color := colorGreen
		switch {
		case pct >= 0.66:
			color = colorRed
		case pct >= 0.33:
			color = colorYellow
		}
		b.WriteString(color)
		b.WriteRune(bars[idx])
		b.WriteString(colorReset)
	}
	return b.String()
}

// ── Closing narrative ───────────────────────────────────────────────────

func printClosing() {
	fmt.Println()
	fmt.Println("  " + colorBold + "What you just saw:" + colorReset)
	fmt.Println()
	fmt.Println("  Lockstep and fixed scheduling produce no queues -- but only")
	fmt.Println("  when everything is perfectly predictable (lockstep and D/D/1).")
	fmt.Println()
	fmt.Println("  Variable arrivals create queues (M/D/1). Variable service")
	fmt.Println("  creates queues (D/M/1). Both together make it worse")
	fmt.Println("  (M/M/1). Variability creates waiting, even below full")
	fmt.Println("  capacity.")
	fmt.Println()
	fmt.Println("  Now look at near-full load. Only 5% less slack")
	fmt.Println("  than M/M/1 at " + "\u03c1" + "=0.9, but avg wait nearly doubles. The queue")
	fmt.Println("  builds higher and recovers slower. It " + colorBold + "looks" + colorReset + " unstable --")
	fmt.Println("  but it's not. It's technically sustainable. Your gut says")
	fmt.Println("  \"this is broken\" while the averages say \"this is fine.\"")
	fmt.Println("  That gap is where real systems fail.")
	fmt.Println()
	fmt.Println("  Check the math: avg queue " + "\u2248" + " arrival rate " + "\u00d7" + " avg wait.")
	fmt.Println("  That's Little's Law (L = " + "\u03bb" + "W), and it holds for every")
	fmt.Println("  stable scenario in the table above.")
	fmt.Println()
	fmt.Println("  When demand actually exceeds capacity (overloaded), backlog")
	fmt.Println("  trends upward and does not stabilize while arrivals continue.")
	fmt.Println()
	fmt.Println("  " + colorBold + "What makes a queue stable or unstable?" + colorReset)
	fmt.Println("  1. " + "\u03c1" + " < 1 is necessary -- demand must be below capacity.")
	fmt.Println("  2. But " + "\u03c1" + " < 1 is not sufficient for comfort. Load and")
	fmt.Println("     variability are co-factors -- neither alone is the")
	fmt.Println("     problem. High load × high variability is what creates")
	fmt.Println("     the nonlinear blowup in waiting time.")
	fmt.Println("  3. The warning signs: spikes get deeper, recovery takes")
	fmt.Println("     longer, average wait grows nonlinearly with load.")
	fmt.Println()
	fmt.Println("  In real systems, both arrival and service always vary.")
	fmt.Println("  The question is never \"will there be queues?\" but \"how do")
	fmt.Println("  we manage them?\" The next demo explores what happens with")
	fmt.Println("  multiple stations in sequence -- and one slower than the rest.")
	fmt.Println()
	fmt.Println("  " + colorDim + "Terms:" + colorReset)
	fmt.Println("    Arrival rate (\u03bb)  how often customers show up")
	fmt.Println("    Service rate (\u03bc)  how fast the server works")
	fmt.Println("    Utilization (\u03c1)   \u03bb / \u03bc -- fraction of time server is busy")
	fmt.Println("    Queue depth (L_q) customers waiting (not counting in-service)")
	fmt.Println("    Wait time (W_q)   time in queue before service starts")
	fmt.Println("    Lead time (W)     total time from arrival to departure")
	fmt.Println("    Little's Law      L = \u03bbW  (any stable queue)")
	fmt.Println()
}

// ── Helpers ─────────────────────────────────────────────────────────────

func padRight(s string, width int) string {
	visible := stripANSI(s)
	pad := width - len([]rune(visible))
	if pad <= 0 {
		return s
	}
	return s + strings.Repeat(" ", pad)
}

func stripANSI(s string) string {
	var b strings.Builder
	inEsc := false
	for _, r := range s {
		if r == '\033' {
			inEsc = true
			continue
		}
		if inEsc {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inEsc = false
			}
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func divider(text string) string {
	const w = 86
	pad := w - len(text) - 4
	if pad < 0 {
		pad = 0
	}
	left := pad / 2
	right := pad - left
	return "══" + strings.Repeat("═", left) + " " + text + " " + strings.Repeat("═", right) + "══"
}

// ── Main ────────────────────────────────────────────────────────────────

func main() {
	term := newTerminal()

	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			term.exitAltScreen()
			term.showCursor()
		})
	}
	defer cleanup()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cleanup()
		os.Exit(1)
	}()

	// Generate matched traces.
	arrivalTrace := genExponentialTrace(200, interarrivalRho, arrivalSeed)
	serviceTrace := genExponentialTrace(200, serviceMean, serviceSeed)

	scenarios := buildScenarios(arrivalTrace, serviceTrace)

	// Run all simulations upfront (DES is instant).
	results := make([]simResult, len(scenarios))
	allMetrics := make([]metrics, len(scenarios))
	for i, sc := range scenarios {
		results[i] = runSim(sc.config)
		allMetrics[i] = computeMetrics(results[i])
	}

	printIntro()

	for i, sc := range scenarios {
		printPreamble(sc, term.isTTY)

		if term.isTTY {
			playbackScenario(sc, results[i], allMetrics[i], term)

			// One-line summary after alt-screen.
			m := allMetrics[i]
			fmt.Printf("  %-32s  served:%d  peak-q:%d  avg-wait:%.1fmin\n",
				sc.name, m.served, m.peakQ, m.avgWait)
		}
		// Non-TTY: preambles already printed, no animation.
	}

	fmt.Println()
	printComparison(scenarios, results, allMetrics)
	printClosing()
}
