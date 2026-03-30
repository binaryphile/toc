// Command gridsearch evaluates all static DBR action pairs (rope × buffer time)
// on a multi-stage DES pipeline. Reports decomposed metrics: throughput,
// average WIP, average flow time, and composite reward.
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

type metrics struct {
	throughput float64 // completions per sim-time
	avgWIP     float64
	avgBacklog float64
	reward     float64
}

func main() {
	warmup := flag.Int("warmup", 10, "warmup intervals (discarded)")
	seeds := flag.Int("seeds", 5, "CRN seeds per vector")
	maxTime := flag.Float64("max-time", 5000, "sim-time per episode")
	alpha := flag.Float64("alpha", 0.01, "WIP penalty coefficient")
	csvFile := flag.String("csv", "", "CSV output file (all results)")
	maxRope := flag.Int("max-rope", 20, "max rope rate")
	bufSteps := flag.Int("buf-steps", 20, "number of buffer time steps (1x to Nx serviceMean)")
	bufSubSteps := flag.Bool("buf-sub", false, "include sub-1x buffer steps (0.01x, 0.1x, 0.5x)")
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

	constraintMean := cfg.Stages[cfg.ConstraintStage].ServiceMean

	// Build buffer steps: optionally include sub-1x, then 1x through Nx.
	type bufStep struct {
		label string
		time  float64
	}
	var bufStepList []bufStep
	if *bufSubSteps {
		bufStepList = append(bufStepList,
			bufStep{"0.01×svc", 0.01 * constraintMean},
			bufStep{"0.1×svc", 0.1 * constraintMean},
			bufStep{"0.5×svc", 0.5 * constraintMean},
		)
	}
	for bn := 1; bn <= *bufSteps; bn++ {
		bufStepList = append(bufStepList, bufStep{
			label: fmt.Sprintf("%d×svc", bn),
			time:  float64(bn) * constraintMean,
		})
	}

	maxBufTime := bufStepList[len(bufStepList)-1].time
	cfg.MaxBufferTime = maxBufTime

	seedList := make([]uint64, *seeds)
	for i := range seedList {
		seedList[i] = uint64(42 + i*97)
	}

	type result struct {
		rope     int
		bufLabel string
		bufTime  float64
		m        metrics // mean across seeds
		mStddev  metrics // stddev across seeds
	}

	total := *maxRope * len(bufStepList)
	results := make([]result, 0, total)

	start := time.Now()
	done := 0

	for rope := 1; rope <= *maxRope; rope++ {
		for _, bs := range bufStepList {
			action := sim.DBRAction{RopeRate: rope, BufferTime: bs.time}
			perSeed := make([]metrics, len(seedList))

			for si, seed := range seedList {
				perSeed[si] = evaluateStatic(cfg, action, seed, *warmup)
			}

			mean, std := metricsStats(perSeed)
			results = append(results, result{rope, bs.label, bs.time, mean, std})

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
	sort.Slice(results, func(i, j int) bool { return results[i].m.reward > results[j].m.reward })

	// Print top 10 with decomposed metrics.
	fmt.Println("\n=== TOP 10 (by reward) ===")
	fmt.Println("  rope  buffer       throughput  avg_wip  reward")
	for i := 0; i < 10 && i < len(results); i++ {
		r := results[i]
		fmt.Printf("  %4d  %-10s   %9.4f  %7.2f  %7.4f\n",
			r.rope, r.bufLabel, r.m.throughput, r.m.avgWIP, r.m.reward)
	}

	// Print bottom 5.
	fmt.Println("\n=== BOTTOM 5 ===")
	for i := len(results) - 5; i < len(results); i++ {
		if i < 0 {
			continue
		}
		r := results[i]
		fmt.Printf("  %4d  %-10s   %9.4f  %7.2f  %7.4f\n",
			r.rope, r.bufLabel, r.m.throughput, r.m.avgWIP, r.m.reward)
	}

	// Find best by reward for marginal analysis.
	best := results[0]

	// Marginal sensitivity: rope (fixing buffer at best).
	fmt.Println("\n=== MARGINAL: rope (buffer fixed at best) ===")
	fmt.Println("  rope  throughput  avg_wip  reward")
	for rope := 1; rope <= *maxRope; rope++ {
		for _, r := range results {
			if r.bufLabel == best.bufLabel && r.rope == rope {
				marker := ""
				if rope == best.rope {
					marker = " <--"
				}
				fmt.Printf("  %4d  %9.4f  %7.2f  %7.4f%s\n",
					rope, r.m.throughput, r.m.avgWIP, r.m.reward, marker)
				break
			}
		}
	}

	// Marginal sensitivity: buffer (fixing rope at best).
	fmt.Println("\n=== MARGINAL: buffer (rope fixed at best) ===")
	fmt.Println("  buffer       throughput  avg_wip  reward")
	for _, bs := range bufStepList {
		for _, r := range results {
			if r.rope == best.rope && r.bufLabel == bs.label {
				marker := ""
				if r.bufLabel == best.bufLabel {
					marker = " <--"
				}
				fmt.Printf("  %-10s   %9.4f  %7.2f  %7.4f%s\n",
					r.bufLabel, r.m.throughput, r.m.avgWIP, r.m.reward, marker)
				break
			}
		}
	}

	// Interpretation.
	fmt.Println("\n=== INTERPRETATION ===")
	fmt.Printf("  Best rope:   %d items/interval\n", best.rope)
	fmt.Printf("  Best buffer: %s (%.1f sim-time)\n", best.bufLabel, best.bufTime)
	theoreticalDrum := cfg.IntervalTime / constraintMean
	fmt.Printf("  Theoretical: %.1f items/interval (interval/service_mean)\n", theoreticalDrum)
	fmt.Printf("  Interval:    %.0f sim-time\n", cfg.IntervalTime)

	// CSV output.
	if *csvFile != "" {
		f, err := os.Create(*csvFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "csv: %v\n", err)
			os.Exit(1)
		}
		w := csv.NewWriter(f)
		header := []string{"rope", "buffer_label", "buffer_time",
			"mean_throughput", "mean_avg_wip", "mean_reward",
			"std_throughput", "std_avg_wip", "std_reward"}
		w.Write(header)
		for _, r := range results {
			row := []string{
				strconv.Itoa(r.rope),
				r.bufLabel,
				strconv.FormatFloat(r.bufTime, 'f', 2, 64),
				strconv.FormatFloat(r.m.throughput, 'f', 6, 64),
				strconv.FormatFloat(r.m.avgWIP, 'f', 6, 64),
				strconv.FormatFloat(r.m.reward, 'f', 6, 64),
				strconv.FormatFloat(r.mStddev.throughput, 'f', 6, 64),
				strconv.FormatFloat(r.mStddev.avgWIP, 'f', 6, 64),
				strconv.FormatFloat(r.mStddev.reward, 'f', 6, 64),
			}
			w.Write(row)
		}
		w.Flush()
		f.Close()
		fmt.Fprintf(os.Stderr, "\nCSV written to %s (%d rows)\n", *csvFile, len(results))
	}

	fmt.Fprintf(os.Stderr, "\nTotal time: %s\n", time.Since(start).Round(time.Second))
}

func evaluateStatic(cfg sim.EnvConfig, action sim.DBRAction, seed uint64, warmup int) metrics {
	env := sim.NewEnv(cfg)
	env.Reset(seed)

	for i := 0; i < warmup; i++ {
		_, _, done, _ := env.Step(action)
		if done {
			return metrics{}
		}
	}

	var totalReward, totalTP, totalWIP, totalBacklog float64
	steps := 0
	for {
		_, reward, done, info := env.Step(action)
		totalReward += reward
		totalTP += float64(info.Completions) / info.IntervalTime
		totalWIP += info.AvgTotalWIP
		totalBacklog += info.AvgSourceBacklog
		steps++
		if done {
			break
		}
	}
	if steps == 0 {
		return metrics{}
	}
	n := float64(steps)
	return metrics{
		throughput: totalTP / n,
		avgWIP:     totalWIP / n,
		avgBacklog: totalBacklog / n,
		reward:     totalReward / n,
	}
}

func metricsStats(vals []metrics) (mean, stddev metrics) {
	n := float64(len(vals))
	if n == 0 {
		return
	}
	for _, v := range vals {
		mean.throughput += v.throughput
		mean.avgWIP += v.avgWIP
		mean.avgBacklog += v.avgBacklog
		mean.reward += v.reward
	}
	mean.throughput /= n
	mean.avgWIP /= n
	mean.avgBacklog /= n
	mean.reward /= n

	if n <= 1 {
		return
	}
	for _, v := range vals {
		stddev.throughput += (v.throughput - mean.throughput) * (v.throughput - mean.throughput)
		stddev.avgWIP += (v.avgWIP - mean.avgWIP) * (v.avgWIP - mean.avgWIP)
		stddev.avgBacklog += (v.avgBacklog - mean.avgBacklog) * (v.avgBacklog - mean.avgBacklog)
		stddev.reward += (v.reward - mean.reward) * (v.reward - mean.reward)
	}
	stddev.throughput = math.Sqrt(stddev.throughput / (n - 1))
	stddev.avgWIP = math.Sqrt(stddev.avgWIP / (n - 1))
	stddev.avgBacklog = math.Sqrt(stddev.avgBacklog / (n - 1))
	stddev.reward = math.Sqrt(stddev.reward / (n - 1))
	return
}
