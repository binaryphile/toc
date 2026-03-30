// Command gridsearch evaluates all static DBR action pairs (rope × buffer time)
// on a multi-stage DES pipeline. Reports which pairs produce the best
// steady-state reward.
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
	maxRope := flag.Int("max-rope", 20, "max rope rate")
	bufSteps := flag.Int("buf-steps", 20, "number of buffer time steps to evaluate")
	constraint := flag.Int("constraint", 1, "constraint stage index")
	interval := flag.Float64("interval", 100.0, "sim-time per step")
	flag.Parse()

	cfg := sim.EnvConfig{
		Stages: []sim.StageConfig{
			{Workers: 1, ServiceMean: 1.0},
			{Workers: 1, ServiceMean: 10.0}, // bottleneck
			{Workers: 1, ServiceMean: 1.0},
		},
		ArrivalMean:     11.1, // ρ ≈ 0.9 at bottleneck
		IntervalTime:    *interval,
		MaxTime:         *maxTime,
		RewardAlpha:     *alpha,
		ConstraintStage: *constraint,
		MaxRopeRate:     *maxRope,
	}

	// Buffer time steps: [1×serviceMean, 2×serviceMean, ..., N×serviceMean].
	// Each step = one item's worth of constraint time.
	constraintMean := cfg.Stages[cfg.ConstraintStage].ServiceMean
	maxBufTime := float64(*bufSteps) * constraintMean
	cfg.MaxBufferTime = maxBufTime

	seedList := make([]uint64, *seeds)
	for i := range seedList {
		seedList[i] = uint64(42 + i*97)
	}

	type result struct {
		rope    int
		bufTime float64
		bufN    int // buffer in units of constraint service times
		mean    float64
		stddev  float64
		perSeed []float64
	}

	total := *maxRope * *bufSteps
	results := make([]result, 0, total)

	start := time.Now()
	done := 0

	for rope := 1; rope <= *maxRope; rope++ {
		for bn := 1; bn <= *bufSteps; bn++ {
			bufTime := float64(bn) * constraintMean
			action := sim.DBRAction{RopeRate: rope, BufferTime: bufTime}
			perSeed := make([]float64, len(seedList))

			for si, seed := range seedList {
				perSeed[si] = evaluateStatic(cfg, action, seed, *warmup)
			}

			mean, std := meanStddev(perSeed)
			results = append(results, result{rope, bufTime, bn, mean, std, perSeed})

			done++
			if done%100 == 0 {
				elapsed := time.Since(start)
				pct := float64(done) / float64(total) * 100
				eta := time.Duration(float64(elapsed) / float64(done) * float64(total-done))
				fmt.Fprintf(os.Stderr, "  %d/%d (%.0f%%) elapsed=%s eta=%s\n", done, total, pct, elapsed.Round(time.Second), eta.Round(time.Second))
			}
		}
	}

	// Sort by mean reward descending.
	sort.Slice(results, func(i, j int) bool { return results[i].mean > results[j].mean })

	// Print top 10.
	fmt.Println("\n=== TOP 10 ===")
	for i := 0; i < 10 && i < len(results); i++ {
		r := results[i]
		fmt.Printf("  rope=%2d buf=%2d×svc(%.0f)  reward=%.4f ± %.4f\n",
			r.rope, r.bufN, r.bufTime, r.mean, r.stddev)
	}

	// Print bottom 5.
	fmt.Println("\n=== BOTTOM 5 ===")
	for i := len(results) - 5; i < len(results); i++ {
		r := results[i]
		fmt.Printf("  rope=%2d buf=%2d×svc(%.0f)  reward=%.4f ± %.4f\n",
			r.rope, r.bufN, r.bufTime, r.mean, r.stddev)
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
	fmt.Printf("  Best: rope=%d buf=%d×svc(%.0f) reward=%.4f\n",
		best.rope, best.bufN, best.bufTime, best.mean)
	fmt.Printf("  Threshold: %.4f (%d vectors)\n", threshold, equivCount)

	// Marginal sensitivity: rope (fixing buffer at best).
	fmt.Println("\n=== MARGINAL SENSITIVITY: rope (fixing buffer at best) ===")
	for rope := 1; rope <= *maxRope; rope++ {
		for _, r := range results {
			if r.bufN == best.bufN && r.rope == rope {
				marker := ""
				if rope == best.rope {
					marker = " <-- best"
				}
				fmt.Printf("  rope=%2d  reward=%.4f%s\n", rope, r.mean, marker)
				break
			}
		}
	}

	// Marginal sensitivity: buffer time (fixing rope at best).
	fmt.Println("\n=== MARGINAL SENSITIVITY: buffer time (fixing rope at best) ===")
	for bn := 1; bn <= *bufSteps; bn++ {
		for _, r := range results {
			if r.rope == best.rope && r.bufN == bn {
				marker := ""
				if bn == best.bufN {
					marker = " <-- best"
				}
				fmt.Printf("  buf=%2d×svc(%3.0f)  reward=%.4f%s\n", bn, r.bufTime, r.mean, marker)
				break
			}
		}
	}

	// DBR interpretation.
	fmt.Println("\n=== DBR INTERPRETATION ===")
	fmt.Printf("  Best rope rate:    %d items/interval\n", best.rope)
	fmt.Printf("  Best buffer time:  %.0f (=%d × serviceMean=%.0f)\n",
		best.bufTime, best.bufN, constraintMean)
	theoreticalDrum := cfg.IntervalTime / constraintMean
	fmt.Printf("  Theoretical drum:  %.1f items/interval (interval/service_mean)\n", theoreticalDrum)
	if float64(best.rope) >= theoreticalDrum*0.8 && float64(best.rope) <= theoreticalDrum*1.2 {
		fmt.Println("  → Rope ≈ drum rate (DBR-aligned)")
	} else if float64(best.rope) > theoreticalDrum*1.2 {
		fmt.Println("  → Rope > drum rate (over-releasing)")
	} else {
		fmt.Println("  → Rope < drum rate (under-releasing)")
	}

	// CSV output.
	if *csvFile != "" {
		f, err := os.Create(*csvFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "csv: %v\n", err)
			os.Exit(1)
		}
		w := csv.NewWriter(f)
		header := []string{"rope", "buffer_time", "buffer_n", "mean_reward", "stddev"}
		for i := range seedList {
			header = append(header, fmt.Sprintf("seed_%d", i))
		}
		w.Write(header)
		for _, r := range results {
			row := []string{
				strconv.Itoa(r.rope),
				strconv.FormatFloat(r.bufTime, 'f', 1, 64),
				strconv.Itoa(r.bufN),
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

func evaluateStatic(cfg sim.EnvConfig, action sim.DBRAction, seed uint64, warmup int) float64 {
	env := sim.NewEnv(cfg)
	env.Reset(seed)

	for i := 0; i < warmup; i++ {
		_, _, done, _ := env.Step(action)
		if done {
			return 0
		}
	}

	totalReward := 0.0
	steps := 0
	for {
		_, reward, done, _ := env.Step(action)
		totalReward += reward
		steps++
		if done {
			break
		}
	}
	if steps == 0 {
		return 0
	}
	return totalReward / float64(steps)
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
