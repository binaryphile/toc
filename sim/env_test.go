package sim_test

import (
	"math"
	"testing"

	"codeberg.org/binaryphile/toc/sim"
)

func baseConfig() sim.EnvConfig {
	return sim.EnvConfig{
		Stages: []sim.StageConfig{
			{Workers: 1, ServiceMean: 3.0},
			{Workers: 1, ServiceMean: 3.0},
			{Workers: 1, ServiceMean: 3.0},
		},
		ArrivalMean:  3.33,
		IntervalTime: 30.0,
		MaxItems:     100,
		RewardAlpha:  0.01,
	}
}

func unboundedActions(n int) []int {
	return make([]int, n) // all zeros = unbounded
}

func TestSingleStageBasic(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages:       []sim.StageConfig{{Workers: 1, ServiceMean: 3.0}},
		ArrivalMean:  3.33, // ρ ≈ 0.9
		IntervalTime: 100.0,
		MaxTime:      1000.0,
		RewardAlpha:  0.01,
	}
	env := sim.NewEnv(cfg)
	obs := env.Reset(42)

	if obs.TotalWIP != 0 {
		t.Errorf("initial TotalWIP = %d, want 0", obs.TotalWIP)
	}

	totalCompletions := 0
	for i := 0; i < 10; i++ {
		obs, _, done, info := env.Step([]int{0})
		totalCompletions += info.Completions
		if done {
			break
		}
		_ = obs
	}

	if totalCompletions == 0 {
		t.Error("no completions after 10 steps")
	}
	// ρ ≈ 0.9, ~30 completions per 100 sim-time → ~300 total
	if totalCompletions < 100 {
		t.Errorf("completions = %d, expected > 100 for ρ≈0.9 over 1000 sim-time", totalCompletions)
	}
}

func TestMultiStageBottleneck(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages: []sim.StageConfig{
			{Workers: 1, ServiceMean: 1.0},  // fast
			{Workers: 1, ServiceMean: 10.0}, // bottleneck
			{Workers: 1, ServiceMean: 1.0},  // fast
		},
		ArrivalMean:  2.0, // faster than bottleneck
		IntervalTime: 100.0,
		MaxTime:      1000.0,
		RewardAlpha:  0.01,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	totalCompletions := 0
	for i := 0; i < 10; i++ {
		_, _, done, info := env.Step(unboundedActions(3))
		totalCompletions += info.Completions
		if done {
			break
		}
	}

	// Bottleneck throughput ≈ 1/10 = 0.1/sim-time. Over 1000: ~100.
	if totalCompletions < 50 {
		t.Errorf("completions = %d, bottleneck should allow ~100", totalCompletions)
	}
	if totalCompletions > 200 {
		t.Errorf("completions = %d, should be bounded by bottleneck", totalCompletions)
	}
}

func TestWIPCapEnforced(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages: []sim.StageConfig{
			{Workers: 1, ServiceMean: 1.0},
			{Workers: 1, ServiceMean: 5.0},
		},
		ArrivalMean:  1.0,
		IntervalTime: 50.0,
		MaxTime:      500.0,
		RewardAlpha:  0,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	for i := 0; i < 10; i++ {
		obs, _, done, _ := env.Step([]int{3, 3}) // MaxWIP=3 on both
		for j, s := range obs.Stages {
			// WIP can exceed cap due to blocked-after-service items
			// (they entered before downstream was full). Check that
			// queued + in_service (the admission-controlled portion)
			// does not exceed cap.
			admitted := s.Queued + s.InService
			if admitted > 3 {
				t.Errorf("step %d: stage %d admitted = %d, exceeds MaxWIP=3", i, j, admitted)
			}
		}
		if done {
			break
		}
	}
}

func TestBlockingAfterService(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages: []sim.StageConfig{
			{Workers: 1, ServiceMean: 1.0},
			{Workers: 1, ServiceMean: 10.0}, // slow
		},
		ArrivalMean:  1.0,
		IntervalTime: 50.0,
		MaxTime:      200.0,
		RewardAlpha:  0,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	sawBlocked := false
	for i := 0; i < 4; i++ {
		obs, _, done, _ := env.Step([]int{0, 1}) // stage 1 MaxWIP=1
		if obs.Stages[0].BlockedAfterService > 0 {
			sawBlocked = true
		}
		if done {
			break
		}
	}

	if !sawBlocked {
		t.Error("expected stage 0 to have blocked-after-service items with MaxWIP=1 on stage 1")
	}
}

func TestSourceBacklog(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages: []sim.StageConfig{
			{Workers: 1, ServiceMean: 5.0},
		},
		ArrivalMean:  1.0, // arrives fast, serves slow
		IntervalTime: 50.0,
		MaxTime:      200.0,
		RewardAlpha:  0,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	sawBacklog := false
	for i := 0; i < 4; i++ {
		obs, _, done, _ := env.Step([]int{1}) // MaxWIP=1
		if obs.SourceBacklog > 0 {
			sawBacklog = true
		}
		if done {
			break
		}
	}

	if !sawBacklog {
		t.Error("expected source backlog with MaxWIP=1 and fast arrivals")
	}
}

func TestConservationInvariant(t *testing.T) {
	cfg := baseConfig()
	env := sim.NewEnv(cfg)
	env.Reset(42)

	for i := 0; i < 20; i++ {
		_, _, done, _ := env.Step([]int{5, 2, 5}) // varied WIP caps
		if !env.CheckConservation() {
			t.Fatalf("conservation violated at step %d", i)
		}
		if done {
			break
		}
	}
}

func TestReproducibility(t *testing.T) {
	cfg := baseConfig()

	// Run 1.
	env1 := sim.NewEnv(cfg)
	env1.Reset(42)
	var obs1 []sim.Observation
	for i := 0; i < 10; i++ {
		o, _, done, _ := env1.Step([]int{3, 3, 3})
		obs1 = append(obs1, o)
		if done {
			break
		}
	}

	// Run 2: same seed, same actions.
	env2 := sim.NewEnv(cfg)
	env2.Reset(42)
	for i := 0; i < len(obs1); i++ {
		o, _, done, _ := env2.Step([]int{3, 3, 3})
		if o.TotalWIP != obs1[i].TotalWIP || o.SourceBacklog != obs1[i].SourceBacklog {
			t.Errorf("step %d: diverged (wip %d vs %d, backlog %d vs %d)",
				i, o.TotalWIP, obs1[i].TotalWIP, o.SourceBacklog, obs1[i].SourceBacklog)
		}
		if done {
			break
		}
	}
}

func TestResetNewSeed(t *testing.T) {
	cfg := baseConfig()
	env := sim.NewEnv(cfg)

	env.Reset(42)
	o1, _, _, _ := env.Step(unboundedActions(3))

	env.Reset(999)
	o2, _, _, _ := env.Step(unboundedActions(3))

	// Different seeds should produce different states (with high probability).
	if o1.TotalWIP == o2.TotalWIP && o1.SourceBacklog == o2.SourceBacklog {
		t.Log("warning: different seeds produced identical first step (unlikely but possible)")
	}
}

func TestEpisodeTermination(t *testing.T) {
	t.Run("max_items", func(t *testing.T) {
		cfg := sim.EnvConfig{
			Stages:       []sim.StageConfig{{Workers: 1, ServiceMean: 1.0}},
			ArrivalMean:  1.0,
			IntervalTime: 50.0,
			MaxItems:     10,
			RewardAlpha:  0,
		}
		env := sim.NewEnv(cfg)
		env.Reset(42)

		done := false
		for i := 0; i < 100 && !done; i++ {
			_, _, done, _ = env.Step([]int{0})
		}
		if !done {
			t.Error("episode never terminated with MaxItems=10")
		}
	})

	t.Run("max_time", func(t *testing.T) {
		cfg := sim.EnvConfig{
			Stages:       []sim.StageConfig{{Workers: 1, ServiceMean: 1.0}},
			ArrivalMean:  1.0,
			IntervalTime: 50.0,
			MaxTime:      100.0,
			RewardAlpha:  0,
		}
		env := sim.NewEnv(cfg)
		env.Reset(42)

		_, _, done1, _ := env.Step([]int{0})
		_, _, done2, _ := env.Step([]int{0})
		if !done1 && !done2 {
			t.Error("episode should terminate within 2 steps (100 sim-time / 50 interval)")
		}
	})

	t.Run("step_after_done_panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on Step after done")
			}
		}()

		cfg := sim.EnvConfig{
			Stages:       []sim.StageConfig{{Workers: 1, ServiceMean: 1.0}},
			ArrivalMean:  1.0,
			IntervalTime: 50.0,
			MaxTime:      10.0,
			RewardAlpha:  0,
		}
		env := sim.NewEnv(cfg)
		env.Reset(42)
		env.Step([]int{0}) // should be done
		env.Step([]int{0}) // should panic
	})
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  sim.EnvConfig
	}{
		{"no stages", sim.EnvConfig{ArrivalMean: 1, IntervalTime: 1, MaxItems: 1}},
		{"zero workers", sim.EnvConfig{Stages: []sim.StageConfig{{Workers: 0, ServiceMean: 1}}, ArrivalMean: 1, IntervalTime: 1, MaxItems: 1}},
		{"negative service", sim.EnvConfig{Stages: []sim.StageConfig{{Workers: 1, ServiceMean: -1}}, ArrivalMean: 1, IntervalTime: 1, MaxItems: 1}},
		{"zero arrival", sim.EnvConfig{Stages: []sim.StageConfig{{Workers: 1, ServiceMean: 1}}, ArrivalMean: 0, IntervalTime: 1, MaxItems: 1}},
		{"zero interval", sim.EnvConfig{Stages: []sim.StageConfig{{Workers: 1, ServiceMean: 1}}, ArrivalMean: 1, IntervalTime: 0, MaxItems: 1}},
		{"no termination", sim.EnvConfig{Stages: []sim.StageConfig{{Workers: 1, ServiceMean: 1}}, ArrivalMean: 1, IntervalTime: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic for invalid config")
				}
			}()
			sim.NewEnv(tt.cfg)
		})
	}
}

func TestRewardIncludesWIPPenalty(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages:       []sim.StageConfig{{Workers: 1, ServiceMean: 1.0}},
		ArrivalMean:  0.5, // ρ = 2.0, overloaded → WIP grows
		IntervalTime: 50.0,
		MaxTime:      200.0,
		RewardAlpha:  1.0, // high penalty
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	_, r1, _, _ := env.Step([]int{0}) // unbounded
	_, _, _, _ = env.Step([]int{0})
	_, r3, _, _ := env.Step([]int{0})

	// With high WIP and high alpha, reward should decrease as WIP grows.
	if r3 >= r1 && !math.IsNaN(r3) {
		t.Logf("r1=%f r3=%f — expected reward to decrease as WIP grows", r1, r3)
	}
}
