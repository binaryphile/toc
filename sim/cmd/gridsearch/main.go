// Command gridsearch evaluates all static WIP vectors on a multi-stage
// DES pipeline. Reports which vectors produce the best steady-state
// reward and whether the optimal policy shows DBR structure.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"time"

	"codeberg.org/binaryphile/toc/sim"
)

func main() {
	warmup := flag.Int("warmup", 10, "warmup intervals (discarded)")
	seeds := flag.Int("seeds", 5, "CRN seeds per vector")
	maxTime := flag.Float64("max-time", 5000, "sim-time per episode")
	alpha := flag.Float64("alpha", 0.01, "WIP penalty coefficient")
	csvFile := flag.String("csv", "", "CSV output file (all results)")
	maxWIP := flag.Int("max-wip", 20, "max WIP per stage")
	interval := flag.Float64("interval", 100.0, "sim-time per step")
	flag.Parse()

	cfg := sim.EnvConfig{
		Stages: []sim.StageConfig{
			{Workers: 1, ServiceMean: 1.0},
			{Workers: 1, ServiceMean: 10.0}, // bottleneck
			{Workers: 1, ServiceMean: 1.0},
		},
		ArrivalMean:  11.1, // ρ ≈ 0.9 at bottleneck
		IntervalTime: *interval,
		MaxTime:      *maxTime,
		RewardAlpha:  *alpha,
	}

	seedList := make([]uint64, *seeds)
	for i := range seedList {
		seedList[i] = uint64(42 + i*97)
	}

	type result struct {
		w0, w1, w2 int
		mean       float64
		stddev     float64
		perSeed    []float64
	}

	total := *maxWIP * *maxWIP * *maxWIP
	results := make([]result, 0, total)

	start := time.Now()
	done := 0

	for w0 := 1; w0 <= *maxWIP; w0++ {
		for w1 := 1; w1 <= *maxWIP; w1++ {
			for w2 := 1; w2 <= *maxWIP; w2++ {
				actions := []int{w0, w1, w2}
				perSeed := make([]float64, len(seedList))

				for si, seed := range seedList {
					perSeed[si] = evaluateStatic(cfg, actions, seed, *warmup)
				}

				mean, std := meanStddev(perSeed)
				results = append(results, result{w0, w1, w2, mean, std, perSeed})

				done++
				if done%1000 == 0 {
					elapsed := time.Since(start)
					pct := float64(done) / float64(total) * 100
					eta := time.Duration(float64(elapsed) / float64(done) * float64(total-done))
					fmt.Fprintf(os.Stderr, "  %d/%d (%.0f%%) elapsed=%s eta=%s\n", done, total, pct, elapsed.Round(time.Second), eta.Round(time.Second))
				}
			}
		}
	}

	// Sort by mean reward descending.
	sort.Slice(results, func(i, j int) bool { return results[i].mean > results[j].mean })

	// Print top 10.
	fmt.Println("\n=== TOP 10 ===")
	for i := 0; i < 10 && i < len(results); i++ {
		r := results[i]
		fmt.Printf("  WIP=[%2d,%2d,%2d]  reward=%.4f ± %.4f\n", r.w0, r.w1, r.w2, r.mean, r.stddev)
	}

	// Print bottom 5.
	fmt.Println("\n=== BOTTOM 5 ===")
	for i := len(results) - 5; i < len(results); i++ {
		r := results[i]
		fmt.Printf("  WIP=[%2d,%2d,%2d]  reward=%.4f ± %.4f\n", r.w0, r.w1, r.w2, r.mean, r.stddev)
	}

	// Top equivalence set (within 1 stderr of best).
	best := results[0]
	bestStderr := best.stddev / math.Sqrt(float64(len(seedList)))
	threshold := best.mean - bestStderr
	equivCount := 0
	for _, r := range results {
		if r.mean >= threshold {
			equivCount++
		}
	}
	fmt.Printf("\n=== EQUIVALENCE SET (within 1 stderr of best) ===\n")
	fmt.Printf("  Best: WIP=[%d,%d,%d] reward=%.4f\n", best.w0, best.w1, best.w2, best.mean)
	fmt.Printf("  Threshold: %.4f (%d vectors)\n", threshold, equivCount)

	// Marginal sensitivity: best reward for each value of w1 (fix w0, w2 at best).
	fmt.Println("\n=== MARGINAL SENSITIVITY: w1 (bottleneck WIP) ===")
	fmt.Println("  (fixing w0, w2 at best values)")
	for w1 := 1; w1 <= *maxWIP; w1++ {
		for _, r := range results {
			if r.w0 == best.w0 && r.w2 == best.w2 && r.w1 == w1 {
				marker := ""
				if w1 == best.w1 {
					marker = " <-- best"
				}
				fmt.Printf("  w1=%2d  reward=%.4f%s\n", w1, r.mean, marker)
				break
			}
		}
	}

	// DBR structure check.
	fmt.Println("\n=== DBR STRUCTURE ===")
	fmt.Printf("  Best bottleneck WIP (w1): %d\n", best.w1)
	fmt.Printf("  Best upstream WIP (w0):   %d\n", best.w0)
	fmt.Printf("  Best downstream WIP (w2): %d\n", best.w2)
	if best.w1 < best.w0 && best.w1 < best.w2 {
		fmt.Println("  → Bottleneck has tightest WIP (DBR-like)")
	} else if best.w1 > best.w0 || best.w1 > best.w2 {
		fmt.Println("  → Bottleneck does NOT have tightest WIP")
	} else {
		fmt.Println("  → Mixed / inconclusive")
	}

	// CSV output.
	if *csvFile != "" {
		f, err := os.Create(*csvFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "csv: %v\n", err)
			os.Exit(1)
		}
		w := csv.NewWriter(f)
		header := []string{"w0", "w1", "w2", "mean_reward", "stddev"}
		for i := range seedList {
			header = append(header, fmt.Sprintf("seed_%d", i))
		}
		w.Write(header)
		for _, r := range results {
			row := []string{
				strconv.Itoa(r.w0), strconv.Itoa(r.w1), strconv.Itoa(r.w2),
				strconv.FormatFloat(r.mean, 'f', 6, 64),
				strconv.FormatFloat(r.stddev, 'f', 6, 64),
			}
			for _, v := range r.perSeed {
				row = append(row, strconv.FormatFloat(v, 'f', 6, 64))
			}
			w.Write(row)
		}
		w.Flush()
		f.Close()
		fmt.Fprintf(os.Stderr, "\nCSV written to %s (%d rows)\n", *csvFile, len(results))
	}

	fmt.Fprintf(os.Stderr, "\nTotal time: %s\n", time.Since(start).Round(time.Second))
}

func evaluateStatic(cfg sim.EnvConfig, actions []int, seed uint64, warmup int) float64 {
	env := sim.NewEnv(cfg)
	env.Reset(seed)

	// Warmup: step but discard rewards.
	for i := 0; i < warmup; i++ {
		_, _, done, _ := env.Step(actions)
		if done {
			return 0 // episode ended during warmup (shouldn't happen with MaxTime)
		}
	}

	// Measurement: accumulate rewards.
	totalReward := 0.0
	steps := 0
	for {
		_, reward, done, _ := env.Step(actions)
		totalReward += reward
		steps++
		if done {
			break
		}
	}
	if steps == 0 {
		return 0
	}
	return totalReward / float64(steps) // average reward per step
}

func meanStddev(vals []float64) (float64, float64) {
	n := float64(len(vals))
	if n == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	mean := sum / n

	sumSq := 0.0
	for _, v := range vals {
		d := v - mean
		sumSq += d * d
	}
	if n <= 1 {
		return mean, 0
	}
	return mean, math.Sqrt(sumSq / (n - 1))
}
