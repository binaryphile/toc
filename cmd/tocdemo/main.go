// Command tocdemo demonstrates Goldratt's five focusing steps using
// a toc pipeline with configurable variability.
//
// Three scenarios show why identifying the constraint is necessary but
// not sufficient — you must also protect it from starvation with a
// protective buffer (the EXPLOIT step).
//
// Run:
//
//	go run ./cmd/tocdemo/
//	go run ./cmd/tocdemo/ --summary    # headless comparison only
//	go run ./cmd/tocdemo/ --cv 0.5     # lower variability
//	go run ./cmd/tocdemo/ --seed 123   # different RNG seed
package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/binaryphile/toc"
)

// ── Domain types ────────────────────────────────────────────────────────

// File represents a raw input item.
type File struct {
	ID   int
	Name string
}

// Chunk represents a parsed intermediate item.
type Chunk struct {
	ID     int
	Source string
}

// Embedding represents a transformed output item.
type Embedding struct {
	ID     int
	Source string
	Vec    [4]float64
}

// ── Constants ───────────────────────────────────────────────────────────

const (
	defaultItems = 300
	parseTime    = 25 * time.Millisecond  // close to constraint rate
	xformTime    = 30 * time.Millisecond // constraint — only slightly slower
	storeTime    = 5 * time.Millisecond
	tickRate     = 200 * time.Millisecond
	itemWeightKB = 64
)

// ── Scenario config ─────────────────────────────────────────────────────

type scenario struct {
	name string
	desc string
	cv   float64
	opts stageOpts
}

type stageOpts struct {
	parse toc.Options[File]
	xform toc.Options[Chunk]
	store toc.Options[Embedding]
}

type scenarioResult struct {
	name       string
	throughput float64
	peakWIP    int64
	peakMemKB  int64
	avgStarvedPct float64
	elapsed    time.Duration
}

// ── Main ────────────────────────────────────────────────────────────────

func main() {
	var (
		seed    = flag.Int64("seed", 42, "RNG seed for service time generation")
		items   = flag.Int("items", defaultItems, "number of items to process")
		cv      = flag.Float64("cv", 0.75, "coefficient of variation for variable scenarios")
		summary = flag.Bool("summary", false, "headless comparison only, no live animation")
	)
	flag.Parse()

	term := newTerminal()
	cleanup := setupCleanup(term)
	defer cleanup()

	// Buffer sizing via BufferCapacity.
	throughput := 1.0 / xformTime.Seconds()
	protectionTime := 3 * xformTime
	bufferItems := toc.BufferCapacity(throughput, protectionTime)

	scenarios := []scenario{
		{
			name: "deterministic-drum",
			desc: "CV=0, MaxWIP=3. Rope works perfectly without variability.",
			cv:   0,
			opts: stageOpts{
				parse: toc.Options[File]{Capacity: 10, Workers: 4},
				xform: toc.Options[Chunk]{Capacity: 4, Workers: 1, MaxWIP: 3},
				store: toc.Options[Embedding]{Capacity: 10, Workers: 2},
			},
		},
		{
			name: "variable-no-buffer",
			desc: fmt.Sprintf("CV=%.2f, MaxWIP=2, Capacity:1, 1 parse worker. Constraint starves.", *cv),
			cv:   *cv,
			opts: stageOpts{
				// 1 parse worker at 3ms + variability ≈ constraint's throughput.
				// Variability in parse creates arrival gaps → constraint starves.
				parse: toc.Options[File]{Capacity: 1, Workers: 1},
				xform: toc.Options[Chunk]{Capacity: 1, Workers: 1, MaxWIP: 2},
				store: toc.Options[Embedding]{Capacity: 1, Workers: 1},
			},
		},
		{
			name: "variable-buffer",
			desc: fmt.Sprintf("CV=%.2f, buffer=%d (BufferCapacity), 1 parse worker. Protected.", *cv, bufferItems),
			cv:   *cv,
			opts: stageOpts{
				parse: toc.Options[File]{Capacity: 1, Workers: 1},
				xform: toc.Options[Chunk]{Capacity: bufferItems, Workers: 1, MaxWIP: bufferItems + 1},
				store: toc.Options[Embedding]{Capacity: 1, Workers: 1},
			},
		},
	}

	results := make([]scenarioResult, len(scenarios))

	fmt.Printf("tocdemo: %d items, seed=%d\n", *items, *seed)
	fmt.Printf("Buffer sized by BufferCapacity(%.0f/s, %s) = %d items\n\n",
		throughput, protectionTime, bufferItems)

	for i, sc := range scenarios {
		if *summary {
			results[i] = runScenario(sc, *seed, *items, term, true)
		} else if term.isTTY {
			term.enterAltScreen()
			term.hideCursor()
			results[i] = runScenario(sc, *seed, *items, term, false)
			time.Sleep(2 * time.Second)
			term.exitAltScreen()
			term.showCursor()
		} else {
			results[i] = runScenario(sc, *seed, *items, term, true)
		}
		fmt.Println(renderSummaryLine(
			results[i].name, results[i].throughput,
			results[i].peakWIP, results[i].peakMemKB, results[i].avgStarvedPct))
	}

	fmt.Println()
	printComparison(results)
}

// ── Scenario execution ──────────────────────────────────────────────────

func runScenario(sc scenario, seed int64, totalItems int, term terminal, staticMode bool) scenarioResult {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Stage functions with precomputed service times.
	parseFn := func(_ context.Context, f File) (Chunk, error) {
		time.Sleep(itemDuration(seed, f.ID, "parse", parseTime, sc.cv))
		return Chunk{ID: f.ID, Source: f.Name}, nil
	}
	xformFn := func(_ context.Context, c Chunk) (Embedding, error) {
		time.Sleep(itemDuration(seed, c.ID, "xform", xformTime, sc.cv))
		return Embedding{ID: c.ID, Source: c.Source, Vec: [4]float64{0.1, 0.2, 0.3, 0.4}}, nil
	}
	storeFn := func(_ context.Context, e Embedding) (Embedding, error) {
		time.Sleep(itemDuration(seed, e.ID, "store", storeTime, sc.cv))
		return e, nil
	}

	parse := toc.Start[File, Chunk](ctx, parseFn, sc.opts.parse)
	xform := toc.Pipe[Chunk, Embedding](ctx, parse.Out(), xformFn, sc.opts.xform)
	store := toc.Pipe[Embedding, Embedding](ctx, xform.Out(), storeFn, sc.opts.store)

	// Submit items.
	var submitted atomic.Int64
	go func() {
		for i := range totalItems {
			f := File{ID: i, Name: fmt.Sprintf("file-%d.txt", i)}
			if err := parse.Submit(ctx, f); err != nil {
				break
			}
			submitted.Add(1)
		}
		parse.CloseInput()
	}()

	// Drain results.
	var completed atomic.Int64
	var drainWg sync.WaitGroup
	drainWg.Add(1)
	go func() {
		defer drainWg.Done()
		for range store.Out() {
			completed.Add(1)
		}
	}()

	// Sample loop.
	ticker := time.NewTicker(tickRate)
	defer ticker.Stop()

	start := time.Now()
	var prevXformStarved time.Duration
	var prevXformCompleted int64
	var prevStoreCompleted int64
	var prevParseCompleted int64
	prevSampleTime := start

	var peakWIP int64
	var starvedSum float64
	var starvedSamples int

	xformWorkers := sc.opts.xform.Workers
	if xformWorkers <= 0 {
		xformWorkers = 1
	}

	for {
		<-ticker.C

		now := time.Now()
		elapsed := now.Sub(start)
		sampleElapsed := now.Sub(prevSampleTime)
		prevSampleTime = now

		ps := parse.Stats()
		xs := xform.Stats()
		ss := store.Stats()

		sub := submitted.Load()
		done := completed.Load()
		pipelineWIP := sub - done
		if pipelineWIP < 0 {
			pipelineWIP = 0
		}

		if pipelineWIP > peakWIP {
			peakWIP = pipelineWIP
		}

		// In-flight approximations.
		parseInFlight := ps.Submitted - ps.Completed - ps.BufferedDepth
		if parseInFlight < 0 {
			parseInFlight = 0
		}
		xformInFlight := xs.Received - xs.Completed - xs.BufferedDepth
		if xformInFlight < 0 {
			xformInFlight = 0
		}
		storeInFlight := ss.Received - ss.Completed - ss.BufferedDepth
		if storeInFlight < 0 {
			storeInFlight = 0
		}

		// Starvation: StarvedTime delta / (elapsed × workers).
		starvedDelta := xs.StarvedTime - prevXformStarved
		prevXformStarved = xs.StarvedTime
		starvedPct := 0.0
		if sampleElapsed > 0 && sub > 0 && done < int64(totalItems) {
			starvedPct = starvedDelta.Seconds() / (sampleElapsed.Seconds() * float64(xformWorkers)) * 100
			if starvedPct < 0 {
				starvedPct = 0
			}
			if starvedPct > 100 {
				starvedPct = 100
			}
			starvedSum += starvedPct
			starvedSamples++
		}

		// Transfer deltas.
		parseTransferred := ps.Completed - prevParseCompleted
		xformTransferred := xs.Completed - prevXformCompleted
		storeTransferred := ss.Completed - prevStoreCompleted
		prevParseCompleted = ps.Completed
		prevXformCompleted = xs.Completed
		prevStoreCompleted = ss.Completed

		f := frameData{
			scenarioName: sc.name,
			scenarioDesc: sc.desc,
			pipelineWIP:  pipelineWIP,
			done:         done,
			total:        totalItems,
			memoryKB:     pipelineWIP * itemWeightKB,
			starvedPct:      starvedPct,
			elapsed:      elapsed,
			stages: [3]stageSnap{
				{
					name: "parse", symbol: symFile, color: colorGreen,
					buffered: ps.BufferedDepth, capacity: sc.opts.parse.Capacity,
					inFlight: parseInFlight, transferred: parseTransferred,
					serviceTime: parseTime,
				},
				{
					name: "xform", symbol: symChunk, color: colorYellow,
					buffered: xs.BufferedDepth, capacity: sc.opts.xform.Capacity,
					inFlight: xformInFlight, transferred: xformTransferred,
					serviceTime: xformTime,
				},
				{
					name: "store", symbol: symEmbedding, color: colorBlue,
					buffered: ss.BufferedDepth, capacity: sc.opts.store.Capacity,
					inFlight: storeInFlight, transferred: storeTransferred,
					serviceTime: storeTime,
				},
			},
		}

		if staticMode {
			fmt.Printf("  %s  WIP:%-3d done:%-3d starved:%.0f%%\n",
				fmtDur(elapsed), pipelineWIP, done, starvedPct)
		} else {
			term.home()
			fmt.Print(renderFrame(f))
		}

		if done >= int64(totalItems) {
			break
		}
	}

	drainWg.Wait()
	store.Wait()
	totalElapsed := time.Since(start)
	throughput := float64(totalItems) / totalElapsed.Seconds()

	avgStarved := 0.0
	if starvedSamples > 0 {
		avgStarved = starvedSum / float64(starvedSamples)
	}

	return scenarioResult{
		name:       sc.name,
		throughput: throughput,
		peakWIP:    peakWIP,
		peakMemKB:  peakWIP * itemWeightKB,
		avgStarvedPct: avgStarved,
		elapsed:    totalElapsed,
	}
}

// ── Comparison ──────────────────────────────────────────────────────────

func printComparison(results []scenarioResult) {
	fmt.Println(divider("COMPARISON"))
	fmt.Println()

	fmt.Printf("  %-28s │ %5s │ %8s │ %8s │ %12s │ %s\n",
		"Scenario", "t/s", "peak WIP", "peak mem", "starved", "elapsed")
	fmt.Println("  " + strings.Repeat("─", 82))

	for _, r := range results {
		wipColor := colorGreen
		switch {
		case r.peakWIP > 100:
			wipColor = colorRed
		case r.peakWIP > 30:
			wipColor = colorYellow
		}

		starvedColor := colorGreen
		if r.avgStarvedPct > 10 {
			starvedColor = colorRed
		} else if r.avgStarvedPct > 5 {
			starvedColor = colorYellow
		}

		fmt.Printf("  %-28s │ %5.0f │ %s%8d%s │ %7dMB │ %s%11.0f%%%s │ %s\n",
			r.name, r.throughput,
			wipColor, r.peakWIP, colorReset,
			r.peakMemKB/1024,
			starvedColor, r.avgStarvedPct, colorReset,
			r.elapsed.Round(time.Millisecond))
	}

	fmt.Println()

	if len(results) >= 3 {
		s1 := results[0] // deterministic
		s2 := results[1] // variable no buffer
		s3 := results[2] // variable with buffer

		fmt.Println(colorBold + "  Lessons:" + colorReset)
		fmt.Println()

		fmt.Println("  1. IDENTIFY: deterministic-drum shows the baseline.")
		fmt.Printf("     Throughput: %.0f/s, WIP: %d, starved: %.0f%%\n", s1.throughput, s1.peakWIP, s1.avgStarvedPct)
		fmt.Println()

		if s2.avgStarvedPct > s1.avgStarvedPct+2 {
			fmt.Println("  2. EXPLOIT needed: variable-no-buffer shows starvation.")
			fmt.Printf("     Constraint starved %.0f%% — it has nothing to process.\n", s2.avgStarvedPct)
			if s2.throughput < s1.throughput*0.95 {
				tputDrop := (1.0 - s2.throughput/s1.throughput) * 100
				fmt.Printf("     Throughput dropped %.0f%% (%.0f/s → %.0f/s).\n", tputDrop, s1.throughput, s2.throughput)
			}
			fmt.Println()
		}

		if s3.avgStarvedPct < s2.avgStarvedPct {
			fmt.Println("  3. EXPLOIT delivered: variable-buffer protects the constraint.")
			fmt.Printf("     Starvation dropped from %.0f%% → %.0f%%.\n", s2.avgStarvedPct, s3.avgStarvedPct)
			if s3.throughput > s2.throughput*1.02 {
				fmt.Printf("     Throughput recovered: %.0f/s → %.0f/s.\n", s2.throughput, s3.throughput)
			}
			fmt.Println()
		}

		fmt.Printf("  Buffer sized by toc.BufferCapacity: %d items — just enough to absorb variance.\n",
			toc.BufferCapacity(1.0/xformTime.Seconds(), 3*xformTime))
		fmt.Println("  These results are illustrative, not workload-calibrated.")
	}
}

func divider(text string) string {
	const w = 84
	pad := w - len(text) - 4
	if pad < 0 {
		pad = 0
	}
	left := pad / 2
	right := pad - left
	return "══" + strings.Repeat("═", left) + " " + text + " " + strings.Repeat("═", right) + "══"
}

// Compile-time check that BufferCapacity is accessible.
var _ = toc.BufferCapacity
