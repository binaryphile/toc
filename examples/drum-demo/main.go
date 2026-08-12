// Package main demonstrates why identifying the correct constraint matters
// in a pipeline, using a restaurant kitchen metaphor.
//
// Four scenarios process 200 orders through Prep → Grill → Plate,
// where the Grill is 10× slower. The demo shows queue depths at each
// station and how WIP control at the constraint eliminates queue buildup
// without affecting throughput.
//
// Run:
//
//	go run ./examples/drum-demo/
package main

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/binaryphile/toc"
)

// ── Domain types ────────────────────────────────────────────────────────

// Order represents a customer order entering the kitchen.
type Order struct{ Name string }

// Prepped represents an order after prep station.
type Prepped struct{ Source string }

// Plated represents a finished dish ready to serve.
type Plated struct {
	Source string
}

// ── Constants ───────────────────────────────────────────────────────────

// Real kitchen times, displayed in the preamble. Simulation runs at 360× speed.
//
// Real:  Prep 18s, Grill 3 min, Plate 18s. Orders arrive about one per minute (Poisson).
// Sim:   50ms / 500ms / 50ms, mean inter-arrival 167ms.
// Grill serves 2/s. Arrivals at 6/s. ρ = 3.0.
// 50 orders arrive over ~8s. Queue builds visibly over ~15 ticks. Each scenario ~15s.
const (
	totalItems = 50
	simSpeed   = "360x"

	// Real durations (for display only).
	realPrepTime  = "18 sec"
	realGrillTime = "3 min"
	realPlateTime = "18 sec"
	realArrival   = "~1 min"

	// Simulated durations (real ÷ 360).
	prepTime  = 50 * time.Millisecond
	grillTime = 500 * time.Millisecond
	plateTime = 50 * time.Millisecond

	tickRate = 500 * time.Millisecond

	// Arrival rate: Poisson process, mean inter-arrival = 167ms simulated (1 min real).
	// Grill service rate = 2/s. Arrival rate = 6/s. ρ = 3.0.
	meanInterArrival = 167 * time.Millisecond
)

// ── ANSI ────────────────────────────────────────────────────────────────

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

// ── Terminal control ────────────────────────────────────────────────────

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

// ── Snapshot model ──────────────────────────────────────────────────────

type queueSnap struct {
	label        string
	depth        int64
	capacity     int  // real QueueCapacity
	isConstraint bool
}

type stationStatus struct {
	name     string
	inFlight int64
}

type snapshot struct {
	queues   [3]queueSnap
	stations [3]stationStatus
	done     int64
	total    int
}

func collectSnapshot(
	prep interface{ Stats() toc.Stats },
	grill interface{ Stats() toc.Stats },
	plate interface{ Stats() toc.Stats },
	completed int64,
) snapshot {
	ps := prep.Stats()
	gs := grill.Stats()
	ss := plate.Stats()

	prepInFlight := ps.Submitted - ps.Completed - ps.BufferedDepth
	if prepInFlight < 0 {
		prepInFlight = 0
	}
	grillInFlight := gs.Received - gs.Completed - gs.BufferedDepth
	if grillInFlight < 0 {
		grillInFlight = 0
	}
	plateInFlight := ss.Received - ss.Completed - ss.BufferedDepth
	if plateInFlight < 0 {
		plateInFlight = 0
	}

	return snapshot{
		queues: [3]queueSnap{
			{label: "Queued for Prep", depth: ps.BufferedDepth, capacity: ps.QueueCapacity},
			{label: "Queued for Grill", depth: gs.BufferedDepth, capacity: gs.QueueCapacity, isConstraint: true},
			{label: "Queued for Plate", depth: ss.BufferedDepth, capacity: ss.QueueCapacity},
		},
		stations: [3]stationStatus{
			{name: "Prep", inFlight: prepInFlight},
			{name: "Grill", inFlight: grillInFlight},
			{name: "Plate", inFlight: plateInFlight},
		},
		done:  completed,
		total: totalItems,
	}
}

// ── Frame renderer ──────────────────────────────────────────────────────

const frameWidth = 66

func renderFrame(scenarioLabel, constraintInfo string, snap snapshot, throughput float64) string {
	var b strings.Builder

	// Top border.
	b.WriteString(boxTL + strings.Repeat(boxH, frameWidth) + boxTR + "\n")

	// Header: scenario name + done.
	header := fmt.Sprintf(" %s%-36s%s  Done: %d/%d",
		colorBold, scenarioLabel, colorReset,
		snap.done, snap.total)
	b.WriteString(boxV + padRight(header, frameWidth) + boxV + "\n")

	// Constraint/WIP info line.
	info := fmt.Sprintf(" %s%s%s", colorDim, constraintInfo, colorReset)
	b.WriteString(boxV + padRight(info, frameWidth) + boxV + "\n")

	// Separator.
	b.WriteString(boxML + strings.Repeat(boxH, frameWidth) + boxMR + "\n")

	// Blank line.
	b.WriteString(boxV + strings.Repeat(" ", frameWidth) + boxV + "\n")

	// Queue bars.
	for i, q := range snap.queues {
		barLine := fmt.Sprintf(" %-19s %s", q.label, renderQueueBar(q))
		b.WriteString(boxV + padRight(barLine, frameWidth) + boxV + "\n")

		if q.isConstraint {
			// Constraint annotation line.
			annotation := fmt.Sprintf(" %s%s<- constraint%s",
				strings.Repeat(" ", 20), colorDim, colorReset)
			b.WriteString(boxV + padRight(annotation, frameWidth) + boxV + "\n")
		} else if i < 2 {
			// Spacer between non-constraint queues.
			b.WriteString(boxV + strings.Repeat(" ", frameWidth) + boxV + "\n")
		}
	}

	// Blank line.
	b.WriteString(boxV + strings.Repeat(" ", frameWidth) + boxV + "\n")

	// Station status line.
	statusLine := " "
	for i, st := range snap.stations {
		if i > 0 {
			statusLine += "   "
		}
		status := colorDim + "[idle]" + colorReset
		if st.inFlight > 0 {
			status = colorGreen + "[working]" + colorReset
		}
		statusLine += st.name + " " + status
	}
	b.WriteString(boxV + padRight(statusLine, frameWidth) + boxV + "\n")

	// Throughput line.
	tputLine := fmt.Sprintf(" Throughput: ~%.0f/s", throughput)
	b.WriteString(boxV + padRight(tputLine, frameWidth) + boxV + "\n")

	// Bottom border.
	b.WriteString(boxBL + strings.Repeat(boxH, frameWidth) + boxBR + "\n")

	return b.String()
}

func renderQueueBar(q queueSnap) string {
	const barWidth = 30

	filled := int(q.depth)
	if filled < 0 {
		filled = 0
	}

	cap := q.capacity
	if cap <= 0 {
		cap = 1
	}

	// Scale to bar width.
	filledChars := int(math.Round(float64(filled) / float64(cap) * float64(barWidth)))
	if filledChars > barWidth {
		filledChars = barWidth
	}
	emptyChars := barWidth - filledChars

	// Color based on fullness.
	pct := float64(filled) / float64(cap)
	color := colorGreen
	if q.isConstraint {
		if pct > 0.5 {
			color = colorRed
		}
	} else {
		switch {
		case pct > 0.75:
			color = colorRed
		case pct > 0.5:
			color = colorYellow
		}
	}

	return color + strings.Repeat("▓", filledChars) + colorReset +
		strings.Repeat("░", emptyChars) +
		fmt.Sprintf("  %d/%d", filled, cap)
}

// ── Preamble system ─────────────────────────────────────────────────────

func printKitchenIntro() {
	fmt.Println()
	fmt.Println("================================================================")
	fmt.Println()
	fmt.Println("  THE KITCHEN")
	fmt.Println()
	fmt.Println("  A restaurant kitchen has three stations:")
	fmt.Println()
	fmt.Printf("    Orders -> Prep (%s) -> Grill (%s) -> Plate (%s) -> Served\n",
		realPrepTime, realGrillTime, realPlateTime)
	fmt.Println()
	fmt.Printf("  Orders arrive randomly, about one every %s (Poisson).\n", realArrival)
	fmt.Println("  The grill is 10x slower than everything else -- it's the")
	fmt.Println("  bottleneck (the \"constraint\"). Station times and staffing are")
	fmt.Println("  fixed across all scenarios. Only the WIP policy changes.")
	fmt.Println()
	fmt.Printf("  Simulation runs at %s speed. You'll see three queue bars\n", simSpeed)
	fmt.Println("  showing tickets waiting at each station. Watch the queue")
	fmt.Println("  in front of the Grill.")
	fmt.Println()
	fmt.Println("================================================================")
	fmt.Println()
}

type scenarioPreamble struct {
	wipLimit    string
	whatHappens string
	lookFor     []string
}

var preambles = []scenarioPreamble{
	{
		wipLimit: "none",
		whatHappens: `Kitchen accepts orders as fast as they come. Prep finishes fast
    and dumps tickets on the grill counter. The grill can't keep up.`,
		lookFor: []string{
			`"Queued for Grill" grows steadily as orders arrive faster than the grill works.`,
			"Throughput is the same as later scenarios -- the flood doesn't help.",
		},
	},
	{
		wipLimit: "Prep (MaxWIP=8)",
		whatHappens: `We cap how many orders prep can work on. But prep is fast --
    it's not the problem. Tickets still pile up between prep and grill.`,
		lookFor: []string{
			"Prep queue stays small.",
			"Grill queue still fills up. Limiting the wrong station doesn't help.",
		},
	},
	{
		wipLimit: "Grill (MaxWIP=3)",
		whatHappens: `We cap WIP at the grill. Only 3 items can be admitted at once
    (buffered + being grilled). Backpressure propagates upstream --
    prep slows because it can't hand off.`,
		lookFor: []string{
			"Grill queue stays at 2-3. Same throughput.",
			"The flood is gone.",
		},
	},
	{
		wipLimit: "Grill (MaxWIP=3), plus Capacity=1 at Prep and Plate",
		whatHappens: `Same grill WIP cap, plus minimal buffer space at the other
    stations. Tightest possible flow.`,
		lookFor: []string{
			"All queues near zero. Same throughput.",
			"If throughput drops, this experiment is confounded.",
		},
	},
}

func printScenarioPreamble(sc scenario, idx int, isTTY bool) {
	p := preambles[idx]
	fmt.Printf("  %s%s%s\n", colorBold, sc.name, colorReset)
	fmt.Printf("  %s%s%s\n", colorDim, sc.desc, colorReset)
	fmt.Println()
	fmt.Printf("  Constraint:  Grill\n")
	fmt.Printf("  WIP limit:   %s\n", p.wipLimit)
	fmt.Println()
	fmt.Printf("  What happens:\n")
	fmt.Printf("    %s\n", p.whatHappens)
	fmt.Println()
	fmt.Printf("  What to look for:\n")
	for _, item := range p.lookFor {
		fmt.Printf("    - %s\n", item)
	}
	fmt.Println()

	if isTTY {
		fmt.Print("  [press enter to start]")
		reader := bufio.NewReader(os.Stdin)
		reader.ReadBytes('\n')
	}
	fmt.Println()
}

// ── Scenario config ─────────────────────────────────────────────────────

type stageOpts struct {
	prep  toc.Options[Order]
	grill toc.Options[Prepped]
	plate toc.Options[Plated]
}

type scenario struct {
	name           string
	desc           string
	constraintInfo string // "Constraint: Grill    WIP limit: ..."
	opts           stageOpts
}

type scenarioResult struct {
	name       string
	timeline   []snapshot
	elapsed    time.Duration
	throughput float64
	peakGrillQ int64
	avgGrillQ  float64
	ticketSec  float64 // integral of grill queue depth over time
}

// ── Main ────────────────────────────────────────────────────────────────

func main() {
	term := newTerminal()

	// Cleanup on panic/interrupt.
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

	defaultPlate := toc.Options[Plated]{Capacity: 10, Workers: 2}

	scenarios := []scenario{
		{
			name:           "Uncontrolled",
			desc:           "No WIP cap -- orders flood in freely",
			constraintInfo: "Constraint: Grill    WIP limit: none",
			opts: stageOpts{
				prep:  toc.Options[Order]{Capacity: 50, Workers: 4},
				grill: toc.Options[Prepped]{Capacity: 50, Workers: 1},
				plate: defaultPlate,
			},
		},
		{
			name:           "Limit on Prep (wrong place)",
			desc:           "MaxWIP=8 on prep -- fast station, not the bottleneck",
			constraintInfo: "Constraint: Grill    WIP limit: Prep (MaxWIP=8)",
			opts: stageOpts{
				prep:  toc.Options[Order]{Capacity: 10, Workers: 4, MaxWIP: 8},
				grill: toc.Options[Prepped]{Capacity: 30, Workers: 1},
				plate: defaultPlate,
			},
		},
		{
			name:           "Limit on Grill (the constraint)",
			desc:           "MaxWIP=3 on grill -- paces release to the bottleneck",
			constraintInfo: "Constraint: Grill    WIP limit: Grill (MaxWIP=3)",
			opts: stageOpts{
				prep:  toc.Options[Order]{Capacity: 10, Workers: 4},
				grill: toc.Options[Prepped]{Capacity: 4, Workers: 1, MaxWIP: 3},
				plate: defaultPlate,
			},
		},
		{
			name:           "Constraint + Tight Buffers",
			desc:           "MaxWIP=3 on grill, Capacity=1 everywhere else",
			constraintInfo: "Constraint: Grill    WIP limit: Grill (MaxWIP=3) + Cap=1",
			opts: stageOpts{
				prep:  toc.Options[Order]{Capacity: 1, Workers: 4},
				grill: toc.Options[Prepped]{Capacity: 2, Workers: 1, MaxWIP: 3},
				plate: toc.Options[Plated]{Capacity: 1, Workers: 2},
			},
		},
	}

	results := make([]scenarioResult, len(scenarios))

	printKitchenIntro()

	for i, sc := range scenarios {
		printScenarioPreamble(sc, i, term.isTTY)

		if term.isTTY {
			term.enterAltScreen()
			term.hideCursor()
			results[i] = runScenarioVisual(sc, term)
			time.Sleep(2 * time.Second)
			term.exitAltScreen()
			term.showCursor()
		} else {
			results[i] = runScenarioStatic(sc)
		}
	}

	fmt.Println()
	printComparison(results)
}

// ── Visual scenario (TTY) ───────────────────────────────────────────────

func runScenarioVisual(sc scenario, term terminal) scenarioResult {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prep := toc.Start[Order, Prepped](ctx, makePrepFn(), sc.opts.prep)
	grill := toc.Pipe[Prepped, Plated](ctx, prep.Out(), makeGrillFn(), sc.opts.grill)
	plate := toc.Pipe[Plated, Plated](ctx, grill.Out(), makePlateFn(), sc.opts.plate)

	go func() {
		for i := range totalItems {
			time.Sleep(poissonDelay(meanInterArrival))
			o := Order{Name: fmt.Sprintf("order-%d", i)}
			if err := prep.Submit(ctx, o); err != nil {
				break
			}
		}
		prep.CloseInput()
	}()

	var completed atomic.Int64
	var drainWg sync.WaitGroup
	drainWg.Add(1)
	go func() {
		defer drainWg.Done()
		for range plate.Out() {
			completed.Add(1)
		}
	}()

	ticker := time.NewTicker(tickRate)
	defer ticker.Stop()

	start := time.Now()
	var timeline []snapshot
	var peakGrillQ int64
	var grillQSum float64
	var prevGrillQ int64
	var ticketSec float64
	var ticks int64
	ewmaTput := 0.0
	prevDone := int64(0)

	scenarioLabel := sc.name

	for {
		<-ticker.C

		snap := collectSnapshot(prep, grill, plate, completed.Load())
		timeline = append(timeline, snap)
		ticks++

		grillQ := snap.queues[1].depth
		if grillQ > peakGrillQ {
			peakGrillQ = grillQ
		}
		grillQSum += float64(grillQ)
		ticketSec += float64(prevGrillQ+grillQ) / 2.0 * tickRate.Seconds()
		prevGrillQ = grillQ

		// EWMA throughput.
		doneDelta := snap.done - prevDone
		prevDone = snap.done
		instantTput := float64(doneDelta) / tickRate.Seconds()
		if ewmaTput == 0 {
			ewmaTput = instantTput
		} else {
			ewmaTput = 0.3*instantTput + 0.7*ewmaTput
		}

		term.home()
		fmt.Print(renderFrame(scenarioLabel, sc.constraintInfo, snap, ewmaTput))

		if snap.done >= int64(totalItems) {
			break
		}
	}

	drainWg.Wait()
	plate.Wait()
	elapsed := time.Since(start)
	throughput := float64(totalItems) / elapsed.Seconds()

	avgGrillQ := 0.0
	if ticks > 0 {
		avgGrillQ = grillQSum / float64(ticks)
	}

	return scenarioResult{
		name:       sc.name,
		timeline:   timeline,
		elapsed:    elapsed,
		throughput: throughput,
		peakGrillQ: peakGrillQ,
		avgGrillQ:  avgGrillQ,
		ticketSec:  ticketSec,
	}
}

// ── Static scenario (non-TTY) ───────────────────────────────────────────

func runScenarioStatic(sc scenario) scenarioResult {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	prep := toc.Start[Order, Prepped](ctx, makePrepFn(), sc.opts.prep)
	grill := toc.Pipe[Prepped, Plated](ctx, prep.Out(), makeGrillFn(), sc.opts.grill)
	plate := toc.Pipe[Plated, Plated](ctx, grill.Out(), makePlateFn(), sc.opts.plate)

	go func() {
		for i := range totalItems {
			time.Sleep(poissonDelay(meanInterArrival))
			o := Order{Name: fmt.Sprintf("order-%d", i)}
			if err := prep.Submit(ctx, o); err != nil {
				break
			}
		}
		prep.CloseInput()
	}()

	var completed atomic.Int64
	var drainWg sync.WaitGroup
	drainWg.Add(1)
	go func() {
		defer drainWg.Done()
		for range plate.Out() {
			completed.Add(1)
		}
	}()

	ticker := time.NewTicker(tickRate)
	defer ticker.Stop()

	start := time.Now()
	var timeline []snapshot
	var peakGrillQ int64
	var grillQSum float64
	var prevGrillQ int64
	var ticketSec float64
	var ticks int64

	for {
		<-ticker.C
		elapsed := time.Since(start)
		snap := collectSnapshot(prep, grill, plate, completed.Load())
		timeline = append(timeline, snap)
		ticks++

		grillQ := snap.queues[1].depth
		if grillQ > peakGrillQ {
			peakGrillQ = grillQ
		}
		grillQSum += float64(grillQ)
		ticketSec += float64(prevGrillQ+grillQ) / 2.0 * tickRate.Seconds()
		prevGrillQ = grillQ

		// Station status abbreviations.
		stationStr := ""
		for j, st := range snap.stations {
			if j > 0 {
				stationStr += " "
			}
			tag := "idle"
			if st.inFlight > 0 {
				tag = "work"
			}
			stationStr += fmt.Sprintf("%s:[%s]", strings.ToLower(st.name), tag)
		}

		fmt.Printf("    %s  done:%3d grill-q:%3d  %s\n",
			fmtDur(elapsed), snap.done, grillQ, stationStr)

		if snap.done >= int64(totalItems) {
			break
		}
	}

	drainWg.Wait()
	plate.Wait()
	elapsed := time.Since(start)
	throughput := float64(totalItems) / elapsed.Seconds()

	avgGrillQ := 0.0
	if ticks > 0 {
		avgGrillQ = grillQSum / float64(ticks)
	}

	fmt.Printf("    Throughput: %.0f/s  Peak grill q: %d  Avg grill q: %.0f\n",
		throughput, peakGrillQ, avgGrillQ)

	return scenarioResult{
		name:       sc.name,
		timeline:   timeline,
		elapsed:    elapsed,
		throughput: throughput,
		peakGrillQ: peakGrillQ,
		avgGrillQ:  avgGrillQ,
		ticketSec:  ticketSec,
	}
}

// ── Stage functions ─────────────────────────────────────────────────────

func makePrepFn() func(context.Context, Order) (Prepped, error) {
	return func(_ context.Context, o Order) (Prepped, error) {
		time.Sleep(prepTime)
		return Prepped{Source: o.Name}, nil
	}
}

func makeGrillFn() func(context.Context, Prepped) (Plated, error) {
	return func(_ context.Context, p Prepped) (Plated, error) {
		time.Sleep(grillTime)
		return Plated{Source: p.Source}, nil
	}
}

func makePlateFn() func(context.Context, Plated) (Plated, error) {
	return func(_ context.Context, p Plated) (Plated, error) {
		time.Sleep(plateTime)
		return p, nil
	}
}

// ── Comparison ──────────────────────────────────────────────────────────

func printComparison(results []scenarioResult) {
	fmt.Println(divider("THE KITCHEN — RESULTS"))
	fmt.Println()

	fmt.Printf("  %-36s %s %s %s %s %s\n",
		"Scenario", "│", " t/s", "│ peak grill q", "│ avg grill q", "│ ticket-sec │  time")
	fmt.Println("  " + strings.Repeat("─", 96))

	for _, r := range results {
		grillColor := colorGreen
		switch {
		case r.peakGrillQ > 100:
			grillColor = colorRed
		case r.peakGrillQ > 30:
			grillColor = colorYellow
		}

		fmt.Printf("  %-36s │ %4.0f │ %s%12d%s │ %10.0f │ %10.0f │ %s\n",
			r.name, r.throughput,
			grillColor, r.peakGrillQ, colorReset,
			r.avgGrillQ,
			r.ticketSec,
			fmtDur(r.elapsed))
	}

	// Grill queue sparkline — use global max across all scenarios for honest comparison.
	globalMaxQ := int64(0)
	for _, r := range results {
		for _, s := range r.timeline {
			if s.queues[1].depth > globalMaxQ {
				globalMaxQ = s.queues[1].depth
			}
		}
	}
	if globalMaxQ == 0 {
		globalMaxQ = 1
	}

	fmt.Println()
	fmt.Println("  " + colorBold + "Grill queue over time:" + colorReset)
	fmt.Println()
	for _, r := range results {
		spark := queueSparkline(r.timeline, globalMaxQ)
		fmt.Printf("  %-36s %s\n", r.name+":", spark)
	}

	// Closing narrative.
	fmt.Println()
	fmt.Println("  " + colorBold + "The grill sets the pace for the whole kitchen." + colorReset)
	fmt.Println("  Scenarios 1-2: no limit or wrong limit -- tickets pile up at the grill.")
	fmt.Println("  Scenario 3: WIP limit at the grill -- same speed, tiny queues.")
	fmt.Println("  Scenario 4: tight buffers everywhere -- leanest operation, same speed.")
	fmt.Println()
	fmt.Println("  Throughput is unchanged because the grill was always the bottleneck.")
	fmt.Println("  WIP control didn't slow the kitchen down -- it stopped wasting counter space.")

	if len(results) >= 4 && results[3].ticketSec > 0 {
		ratio := results[0].ticketSec / results[3].ticketSec
		fmt.Printf("\n  Scenario 1 vs 4: %.0fx total ticket-seconds at the grill -- same work done.\n", ratio)
	}

	fmt.Printf(`
  %sHow this maps to toc:%s

  The demo's "drum" is a constrained stage in a normal toc pipeline.

  Build the runtime stages that do the actual work:

    prep := toc.Start[Order, Prepped](ctx, prepFn, toc.Options[Order]{
        Capacity: 10, Workers: 4,
    })
    grill := toc.Pipe[Prepped, Plated](ctx, prep.Out(), grillFn,
        toc.Options[Prepped]{Capacity: 4, Workers: 1, MaxWIP: 3})
    plate := toc.Pipe[Plated, Plated](ctx, grill.Out(), plateFn,
        toc.Options[Plated]{Capacity: 10, Workers: 2})

  Then, separately, declare the topology for analysis and control:

    pipeline := toc.NewPipeline()
    pipeline.AddStage("prep",  prep.Stats)
    pipeline.AddStage("grill", grill.Stats)
    pipeline.AddStage("plate", plate.Stats)
    pipeline.AddEdge("prep", "grill")
    pipeline.AddEdge("grill", "plate")
    pipeline.Freeze()

  Pass that Pipeline to tools like Analyzer or RopeController.
  Stage execution is independent of the Pipeline object.

  Capacity is the buffer. Workers is the staffing. MaxWIP is
  the WIP cap -- what the demo calls the limit on the grill.

  Register every stage, not just the one you expect to be the
  bottleneck -- the constraint can move. From per-stage stats,
  the Analyzer can infer which stage is currently acting as the
  constraint. Separately, you choose whether and where to apply
  controls such as MaxWIP.

  The arrival process is ordinary application code; toc starts
  at the pipeline stage boundaries.
`, colorBold, colorReset)
}

func queueSparkline(timeline []snapshot, maxQ int64) string {
	if maxQ <= 0 {
		maxQ = 1
	}
	bars := []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}
	var b strings.Builder
	for _, s := range timeline {
		pct := float64(s.queues[1].depth) / float64(maxQ)
		if pct < 0 {
			pct = 0
		}
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

// ── Arrival distribution ────────────────────────────────────────────────

// poissonDelay returns an exponentially distributed delay with the given mean.
// This models Poisson process inter-arrival times.
func poissonDelay(mean time.Duration) time.Duration {
	// Exponential distribution: -mean * ln(U), U ~ Uniform(0,1)
	return time.Duration(float64(mean) * (-math.Log(1.0 - rand.Float64())))
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

func fmtDur(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}

