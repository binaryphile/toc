// Package main demonstrates why identifying the correct drum (constraint)
// matters in a pipeline, using a visual pipeline diagram.
//
// Four scenarios process the same work through parse → transform → store,
// where transform is 10× slower. The demo shows typed items (File → Chunk →
// Embedding) flowing through stages with visible buffers.
//
// Run:
//
//	go run ./examples/drum-demo/
package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"codeberg.org/binaryphile/toc"
)

// ── Domain types ────────────────────────────────────────────────────────

// File represents a raw input item.
type File struct {
	Name string
	Size int
}

// Chunk represents a parsed intermediate item.
type Chunk struct {
	Source string
	Index  int
}

// Embedding represents a transformed output item.
type Embedding struct {
	Source string
	Vec    [4]float64
}

// ── Constants ───────────────────────────────────────────────────────────

const (
	totalItems = 200
	parseTime  = 2 * time.Millisecond  // fast
	xformTime  = 20 * time.Millisecond // bottleneck — 10× slower
	storeTime  = 2 * time.Millisecond  // fast
	tickRate   = 200 * time.Millisecond

	// Simulated per-item memory cost.
	itemWeightKB = 64
)

// ── ANSI / symbols ──────────────────────────────────────────────────────

var (
	colorRed    = "\033[31m"
	colorYellow = "\033[33m"
	colorGreen  = "\033[32m"
	colorBlue   = "\033[34m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
	colorReset  = "\033[0m"
)

const (
	symFile      = "○"
	symChunk     = "●"
	symEmbedding = "◆"
	symEmpty     = "·"
	symDone      = "✓"

	arrowIdle   = "───▶"
	arrowActive = "═══▶"

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
	colorBlue = ""
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

func (t terminal) clearScreen() {
	if t.isTTY {
		fmt.Print("\033[2J\033[H")
	}
}

// ── Snapshot model ──────────────────────────────────────────────────────

type stageSnap struct {
	name        string
	symbol      string
	color       string
	buffered    int64
	capacity    int
	inFlight    int64
	transferred int64 // delta since last tick
	workers     int
	serviceTime time.Duration
}

type snapshot struct {
	elapsed     time.Duration
	stages      [3]stageSnap
	pipelineWIP int64
	memoryKB    int64
	done        int64
	total       int
}

type prevStats struct {
	parseCompleted int64
	xformCompleted int64
	storeCompleted int64
}

func collectSnapshot(
	elapsed time.Duration,
	parse interface{ Stats() toc.Stats },
	xform interface{ Stats() toc.Stats },
	store interface{ Stats() toc.Stats },
	submitted, completed int64,
	prev prevStats,
	parseOpts, xformOpts, storeOpts int, // capacities
	parseWorkers, xformWorkers, storeWorkers int,
) (snapshot, prevStats) {
	ps := parse.Stats()
	xs := xform.Stats()
	ss := store.Stats()

	pipelineWIP := submitted - completed
	if pipelineWIP < 0 {
		pipelineWIP = 0
	}

	// Start stage: Submitted - Completed - BufferedDepth
	parseInFlight := ps.Submitted - ps.Completed - ps.BufferedDepth
	if parseInFlight < 0 {
		parseInFlight = 0
	}
	// Pipe stage: Received - Completed - BufferedDepth
	xformInFlight := xs.Received - xs.Completed - xs.BufferedDepth
	if xformInFlight < 0 {
		xformInFlight = 0
	}
	storeInFlight := ss.Received - ss.Completed - ss.BufferedDepth
	if storeInFlight < 0 {
		storeInFlight = 0
	}

	snap := snapshot{
		elapsed:     elapsed,
		pipelineWIP: pipelineWIP,
		memoryKB:    pipelineWIP * itemWeightKB,
		done:        completed,
		total:       totalItems,
		stages: [3]stageSnap{
			{
				name: "parse", symbol: symFile, color: colorGreen,
				buffered: ps.BufferedDepth, capacity: parseOpts,
				inFlight: parseInFlight, transferred: ps.Completed - prev.parseCompleted,
				workers: parseWorkers, serviceTime: parseTime,
			},
			{
				name: "xform", symbol: symChunk, color: colorYellow,
				buffered: xs.BufferedDepth, capacity: xformOpts,
				inFlight: xformInFlight, transferred: xs.Completed - prev.xformCompleted,
				workers: xformWorkers, serviceTime: xformTime,
			},
			{
				name: "store", symbol: symEmbedding, color: colorBlue,
				buffered: ss.BufferedDepth, capacity: storeOpts,
				inFlight: storeInFlight, transferred: ss.Completed - prev.storeCompleted,
				workers: storeWorkers, serviceTime: storeTime,
			},
		},
	}

	newPrev := prevStats{
		parseCompleted: ps.Completed,
		xformCompleted: xs.Completed,
		storeCompleted: ss.Completed,
	}

	return snap, newPrev
}

// ── Frame renderer (pure) ───────────────────────────────────────────────

const frameWidth = 64

func renderFrame(scenarioName, scenarioDesc string, snap snapshot) string {
	var b strings.Builder

	// Top border
	b.WriteString(boxTL + strings.Repeat(boxH, frameWidth) + boxTR + "\n")

	// Header
	header := fmt.Sprintf(" %s%-30s%s  WIP: %-3d  Done: %d/%d",
		colorBold, scenarioName, colorReset,
		snap.pipelineWIP, snap.done, snap.total)
	b.WriteString(boxV + padRight(header, frameWidth) + boxV + "\n")

	desc := fmt.Sprintf(" %s%s%s", colorDim, scenarioDesc, colorReset)
	b.WriteString(boxV + padRight(desc, frameWidth) + boxV + "\n")

	// Separator
	b.WriteString(boxML + strings.Repeat(boxH, frameWidth) + boxMR + "\n")

	// Blank line
	b.WriteString(boxV + strings.Repeat(" ", frameWidth) + boxV + "\n")

	// Stage names with arrows
	stageLine := " "
	for i, st := range snap.stages {
		arrow := arrowIdle
		if st.transferred > 0 {
			arrow = colorBold + arrowActive + colorReset
		}
		if i > 0 {
			stageLine += "  " + arrow + "  "
		}
		stageLine += st.color + st.symbol + colorReset + " " + st.name
	}
	// Final arrow to done
	lastArrow := arrowIdle
	if snap.stages[2].transferred > 0 {
		lastArrow = colorBold + arrowActive + colorReset
	}
	stageLine += "  " + lastArrow + "  " + symDone
	b.WriteString(boxV + padRight(stageLine, frameWidth) + boxV + "\n")

	// Buffer line
	bufLine := " "
	for i, st := range snap.stages {
		if i > 0 {
			bufLine += "        " // arrow spacing
		}
		bufLine += "buf:" + renderBuffer(st)
	}
	b.WriteString(boxV + padRight(bufLine, frameWidth) + boxV + "\n")

	// In-flight line
	inLine := " "
	for i, st := range snap.stages {
		if i > 0 {
			inLine += "        "
		}
		inLine += fmt.Sprintf("in:%-2d %s", st.inFlight, fmtDur(st.serviceTime))
	}
	b.WriteString(boxV + padRight(inLine, frameWidth) + boxV + "\n")

	// Blank line
	b.WriteString(boxV + strings.Repeat(" ", frameWidth) + boxV + "\n")

	// Stats bars
	memMB := snap.memoryKB / 1024
	tputPerSec := float64(snap.stages[2].transferred) / tickRate.Seconds()
	statsLine := fmt.Sprintf(" throughput %s %3.0f/s   memory %s %dMB",
		bar(tputPerSec, 60, 12), tputPerSec,
		memBar(memMB, 12), memMB)
	b.WriteString(boxV + padRight(statsLine, frameWidth) + boxV + "\n")

	// Legend
	legend := fmt.Sprintf(" %s%s%s File  %s%s%s Chunk  %s%s%s Embedding",
		colorGreen, symFile, colorReset,
		colorYellow, symChunk, colorReset,
		colorBlue, symEmbedding, colorReset)
	b.WriteString(boxV + padRight(legend, frameWidth) + boxV + "\n")

	// Bottom border
	b.WriteString(boxBL + strings.Repeat(boxH, frameWidth) + boxBR + "\n")

	return b.String()
}

func renderBuffer(st stageSnap) string {
	if st.capacity <= 16 {
		// Dot display
		filled := int(st.buffered)
		if filled < 0 {
			filled = 0
		}
		if filled > st.capacity {
			filled = st.capacity
		}
		empty := st.capacity - filled
		return st.color + strings.Repeat(st.symbol, filled) + colorReset +
			strings.Repeat(symEmpty, empty)
	}
	// Bar display for large capacity
	return barWithCount(st.buffered, int64(st.capacity), 8, st.color)
}

func barWithCount(current, max int64, width int, color string) string {
	if max <= 0 {
		max = 1
	}
	pct := float64(current) / float64(max)
	if pct < 0 {
		pct = 0
	}
	if pct > 1 {
		pct = 1
	}
	filled := int(math.Round(pct * float64(width)))
	if filled > width {
		filled = width
	}
	empty := width - filled
	return color + strings.Repeat("█", filled) + colorReset +
		strings.Repeat("░", empty) +
		fmt.Sprintf(" %d", current)
}

func bar(value, max float64, width int) string {
	if max <= 0 {
		max = 1
	}
	pct := value / max
	if pct < 0 {
		pct = 0
	}
	if pct > 1 {
		pct = 1
	}
	filled := int(math.Round(pct * float64(width)))
	if filled > width {
		filled = width
	}
	empty := width - filled

	color := colorGreen
	if pct >= 0.5 {
		color = colorYellow
	}
	return color + strings.Repeat("█", filled) + strings.Repeat("░", empty) + colorReset
}

func memBar(memMB int64, width int) string {
	maxMB := int64(totalItems * itemWeightKB / 1024)
	if maxMB <= 0 {
		maxMB = 1
	}
	pct := float64(memMB) / float64(maxMB)
	if pct < 0 {
		pct = 0
	}
	if pct > 1 {
		pct = 1
	}
	filled := int(math.Round(pct * float64(width)))
	if filled > width {
		filled = width
	}
	empty := width - filled

	color := colorGreen
	switch {
	case pct >= 0.5:
		color = colorRed
	case pct >= 0.15:
		color = colorYellow
	}
	return color + strings.Repeat("█", filled) + strings.Repeat("░", empty) + colorReset
}

// ── Scenario config ─────────────────────────────────────────────────────

type stageOpts struct {
	parse toc.Options[File]
	xform toc.Options[Chunk]
	store toc.Options[Embedding]
}

type scenario struct {
	name string
	desc string
	opts stageOpts
}

type scenarioResult struct {
	name       string
	timeline   []snapshot
	elapsed    time.Duration
	throughput float64
	peakWIP    int64
	peakMemKB  int64
	peakXformQ int64
	wipSeconds float64
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

	defaultStore := toc.Options[Embedding]{Capacity: 10, Workers: 2}

	scenarios := []scenario{
		{
			name: "No drum",
			desc: "No WIP limits. Parse floods freely.",
			opts: stageOpts{
				parse: toc.Options[File]{Capacity: 200, Workers: 4},
				xform: toc.Options[Chunk]{Capacity: 200, Workers: 1},
				store: defaultStore,
			},
		},
		{
			name: "Limit on wrong stage",
			desc: "MaxWIP=8 on parse. Helps some, but transform still floods.",
			opts: stageOpts{
				parse: toc.Options[File]{Capacity: 10, Workers: 4, MaxWIP: 8},
				xform: toc.Options[Chunk]{Capacity: 100, Workers: 1},
				store: defaultStore,
			},
		},
		{
			name: "Correct drum (transform)",
			desc: "MaxWIP=3 on transform. Only what the drum can eat.",
			opts: stageOpts{
				parse: toc.Options[File]{Capacity: 10, Workers: 4},
				xform: toc.Options[Chunk]{Capacity: 4, Workers: 1, MaxWIP: 3},
				store: defaultStore,
			},
		},
		{
			name: "Drum + minimal buffers",
			desc: "MaxWIP=3 on transform, Capacity:1 everywhere else.",
			opts: stageOpts{
				parse: toc.Options[File]{Capacity: 1, Workers: 4},
				xform: toc.Options[Chunk]{Capacity: 2, Workers: 1, MaxWIP: 3},
				store: toc.Options[Embedding]{Capacity: 1, Workers: 2},
			},
		},
	}

	results := make([]scenarioResult, len(scenarios))

	for i, sc := range scenarios {
		if term.isTTY {
			term.enterAltScreen()
			term.hideCursor()
			results[i] = runScenarioVisual(sc, term)
			// Show summary for 2 seconds.
			time.Sleep(2 * time.Second)
			term.exitAltScreen()
			term.showCursor()
			// Print one-line result to main screen.
			fmt.Printf("  %s%-30s%s  t/s: %5.0f  peak WIP: %3d  mem: %dMB\n",
				colorBold, sc.name, colorReset,
				results[i].throughput, results[i].peakWIP, results[i].peakMemKB/1024)
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

	parse := toc.Start[File, Chunk](ctx, makeParseFn(), sc.opts.parse)
	xform := toc.Pipe[Chunk, Embedding](ctx, parse.Out(), makeXformFn(), sc.opts.xform)
	store := toc.Pipe[Embedding, Embedding](ctx, xform.Out(), makeStoreFn(), sc.opts.store)

	var submitted atomic.Int64
	go func() {
		for i := range totalItems {
			f := File{Name: fmt.Sprintf("file-%d.txt", i), Size: 1024 + i}
			if err := parse.Submit(ctx, f); err != nil {
				break
			}
			submitted.Add(1)
		}
		parse.CloseInput()
	}()

	var completed atomic.Int64
	var drainWg sync.WaitGroup
	drainWg.Add(1)
	go func() {
		defer drainWg.Done()
		for range store.Out() {
			completed.Add(1)
		}
	}()

	ticker := time.NewTicker(tickRate)
	defer ticker.Stop()

	start := time.Now()
	var prev prevStats
	var timeline []snapshot
	var peakWIP, peakXformQ int64
	var wipSeconds float64
	prevWIP := int64(0)

	for {
		<-ticker.C

		elapsed := time.Since(start)
		snap, newPrev := collectSnapshot(
			elapsed, parse, xform, store,
			submitted.Load(), completed.Load(), prev,
			sc.opts.parse.Capacity, sc.opts.xform.Capacity, sc.opts.store.Capacity,
			workerCount(sc.opts.parse.Workers), workerCount(sc.opts.xform.Workers), workerCount(sc.opts.store.Workers),
		)
		prev = newPrev
		timeline = append(timeline, snap)

		wipSeconds += float64(prevWIP+snap.pipelineWIP) / 2.0 * tickRate.Seconds()
		prevWIP = snap.pipelineWIP

		if snap.pipelineWIP > peakWIP {
			peakWIP = snap.pipelineWIP
		}
		if snap.stages[1].buffered > peakXformQ {
			peakXformQ = snap.stages[1].buffered
		}

		term.home()
		fmt.Print(renderFrame(sc.name, sc.desc, snap))

		if snap.done >= int64(totalItems) {
			break
		}
	}

	drainWg.Wait()
	store.Wait()
	elapsed := time.Since(start)
	throughput := float64(totalItems) / elapsed.Seconds()

	return scenarioResult{
		name:       sc.name,
		timeline:   timeline,
		elapsed:    elapsed,
		throughput: throughput,
		peakWIP:    peakWIP,
		peakMemKB:  peakWIP * itemWeightKB,
		peakXformQ: peakXformQ,
		wipSeconds: wipSeconds,
	}
}

// ── Static scenario (non-TTY) ───────────────────────────────────────────

func runScenarioStatic(sc scenario) scenarioResult {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	parse := toc.Start[File, Chunk](ctx, makeParseFn(), sc.opts.parse)
	xform := toc.Pipe[Chunk, Embedding](ctx, parse.Out(), makeXformFn(), sc.opts.xform)
	store := toc.Pipe[Embedding, Embedding](ctx, xform.Out(), makeStoreFn(), sc.opts.store)

	var submitted atomic.Int64
	go func() {
		for i := range totalItems {
			f := File{Name: fmt.Sprintf("file-%d.txt", i), Size: 1024 + i}
			if err := parse.Submit(ctx, f); err != nil {
				break
			}
			submitted.Add(1)
		}
		parse.CloseInput()
	}()

	var completed atomic.Int64
	var drainWg sync.WaitGroup
	drainWg.Add(1)
	go func() {
		defer drainWg.Done()
		for range store.Out() {
			completed.Add(1)
		}
	}()

	fmt.Printf("\n--- %s ---\n", sc.name)
	fmt.Printf("    %s\n", sc.desc)

	ticker := time.NewTicker(tickRate)
	defer ticker.Stop()

	start := time.Now()
	var prev prevStats
	var timeline []snapshot
	var peakWIP, peakXformQ int64
	var wipSeconds float64
	prevWIP := int64(0)

	for {
		<-ticker.C
		elapsed := time.Since(start)
		snap, newPrev := collectSnapshot(
			elapsed, parse, xform, store,
			submitted.Load(), completed.Load(), prev,
			sc.opts.parse.Capacity, sc.opts.xform.Capacity, sc.opts.store.Capacity,
			workerCount(sc.opts.parse.Workers), workerCount(sc.opts.xform.Workers), workerCount(sc.opts.store.Workers),
		)
		prev = newPrev
		timeline = append(timeline, snap)

		wipSeconds += float64(prevWIP+snap.pipelineWIP) / 2.0 * tickRate.Seconds()
		prevWIP = snap.pipelineWIP
		if snap.pipelineWIP > peakWIP {
			peakWIP = snap.pipelineWIP
		}
		if snap.stages[1].buffered > peakXformQ {
			peakXformQ = snap.stages[1].buffered
		}

		fmt.Printf("    %s  fed:%3d done:%3d WIP:%3d xformQ:%3d\n",
			fmtDur(elapsed), submitted.Load(), completed.Load(),
			snap.pipelineWIP, snap.stages[1].buffered)

		if snap.done >= int64(totalItems) {
			break
		}
	}

	drainWg.Wait()
	store.Wait()
	elapsed := time.Since(start)
	throughput := float64(totalItems) / elapsed.Seconds()

	fmt.Printf("    Throughput: %.0f/s  Peak WIP: %d  Memory: %dMB\n",
		throughput, peakWIP, peakWIP*itemWeightKB/1024)

	return scenarioResult{
		name:       sc.name,
		timeline:   timeline,
		elapsed:    elapsed,
		throughput: throughput,
		peakWIP:    peakWIP,
		peakMemKB:  peakWIP * itemWeightKB,
		peakXformQ: peakXformQ,
		wipSeconds: wipSeconds,
	}
}

// ── Stage functions ─────────────────────────────────────────────────────

func makeParseFn() func(context.Context, File) (Chunk, error) {
	return func(_ context.Context, f File) (Chunk, error) {
		time.Sleep(parseTime)
		return Chunk{Source: f.Name, Index: 0}, nil
	}
}

func makeXformFn() func(context.Context, Chunk) (Embedding, error) {
	return func(_ context.Context, c Chunk) (Embedding, error) {
		time.Sleep(xformTime)
		return Embedding{Source: c.Source, Vec: [4]float64{0.1, 0.2, 0.3, 0.4}}, nil
	}
}

func makeStoreFn() func(context.Context, Embedding) (Embedding, error) {
	return func(_ context.Context, e Embedding) (Embedding, error) {
		time.Sleep(storeTime)
		return e, nil
	}
}

func workerCount(w int) int {
	if w <= 0 {
		return 1
	}
	return w
}

// ── Comparison ──────────────────────────────────────────────────────────

func printComparison(results []scenarioResult) {
	fmt.Println(divider("RESOURCE COMPARISON"))
	fmt.Println()

	fmt.Printf("  %-30s │ %5s │ %8s │ %8s │ %8s │ %12s\n",
		"Scenario", "t/s", "peak WIP", "peak mem", "xform q", "WIP·sec")
	fmt.Println("  " + strings.Repeat("─", 84))

	for _, r := range results {
		memStr := fmt.Sprintf("%dMB", r.peakMemKB/1024)
		wipSec := fmt.Sprintf("%.0f", r.wipSeconds)

		wipColor := colorGreen
		switch {
		case r.peakWIP > 100:
			wipColor = colorRed
		case r.peakWIP > 30:
			wipColor = colorYellow
		}

		fmt.Printf("  %-30s │ %5.0f │ %s%8d%s │ %8s │ %8d │ %12s\n",
			r.name, r.throughput,
			wipColor, r.peakWIP, colorReset,
			memStr, r.peakXformQ, wipSec)
	}

	fmt.Println()
	fmt.Println("  " + colorBold + "Pipeline WIP over time:" + colorReset)
	fmt.Println()
	for _, r := range results {
		spark := wipSparkline(r.timeline, int64(totalItems))
		fmt.Printf("  %-18s %s\n", r.name+":", spark)
	}

	fmt.Println()
	fmt.Println("  " + colorBold + "Drum (xform) queue over time:" + colorReset)
	fmt.Println()
	for _, r := range results {
		maxQ := int64(0)
		for _, s := range r.timeline {
			if s.stages[1].buffered > maxQ {
				maxQ = s.stages[1].buffered
			}
		}
		if maxQ == 0 {
			maxQ = 1
		}
		spark := queueSparkline(r.timeline, maxQ)
		fmt.Printf("  %-18s %s\n", r.name+":", spark)
	}

	fmt.Println()
	fmt.Println(colorBold + "  The rope must reference the drum." + colorReset)
	fmt.Println("  Scenarios 1-2: limiting the wrong stage or nothing — items flood the constraint.")
	fmt.Println("  Scenario 3: rope at the constraint — steady flow, minimal WIP.")
	fmt.Println("  Scenario 4: rope + minimal non-drum buffers — even lower WIP cost")
	fmt.Println("  under this workload. Same throughput, less resource waste.")

	if len(results) >= 4 && results[3].peakMemKB > 0 {
		memRatio := float64(results[0].peakMemKB) / float64(results[3].peakMemKB)
		costRatio := results[0].wipSeconds / results[3].wipSeconds
		fmt.Printf("\n  Scenario 1 vs 4: %.0f× peak memory, %.0f× total resource cost — same work done.\n",
			memRatio, costRatio)
	}
}

func wipSparkline(timeline []snapshot, maxPossible int64) string {
	if maxPossible <= 0 {
		maxPossible = 1
	}
	bars := []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}
	var b strings.Builder
	for _, s := range timeline {
		pct := float64(s.pipelineWIP) / float64(maxPossible)
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
		case pct >= 0.5:
			color = colorRed
		case pct >= 0.15:
			color = colorYellow
		}
		b.WriteString(color)
		b.WriteRune(bars[idx])
		b.WriteString(colorReset)
	}
	return b.String()
}

func queueSparkline(timeline []snapshot, maxQ int64) string {
	if maxQ <= 0 {
		maxQ = 1
	}
	bars := []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}
	var b strings.Builder
	for _, s := range timeline {
		pct := float64(s.stages[1].buffered) / float64(maxQ)
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

// ── Helpers ─────────────────────────────────────────────────────────────

func padRight(s string, width int) string {
	// Strip ANSI for length calculation.
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
