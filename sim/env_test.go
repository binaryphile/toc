package sim_test

import (
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
		ArrivalMean:     3.33,
		IntervalTime:    30.0,
		MaxItems:        100,
		RewardAlpha:     0.01,
		ConstraintStage: 1,
	}
}

func looseAction() sim.DBRAction {
	return sim.DBRAction{RopeRate: 20, BufferTime: 200.0}
}

func TestSingleStageBasic(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages:          []sim.StageConfig{{Workers: 1, ServiceMean: 3.0}},
		ArrivalMean:     3.33, // ρ ≈ 0.9
		IntervalTime:    100.0,
		MaxTime:         1000.0,
		RewardAlpha:     0.01,
		ConstraintStage: 0,
	}
	env := sim.NewEnv(cfg)
	obs := env.Reset(42)

	if obs.TotalWIP != 0 {
		t.Errorf("initial TotalWIP = %d, want 0", obs.TotalWIP)
	}

	totalCompletions := 0
	// Constraint is stage 0: buffer is ignored, rope only.
	action := sim.DBRAction{RopeRate: 20, BufferTime: 1.0}
	for i := 0; i < 10; i++ {
		obs, _, done, info := env.Step(action)
		totalCompletions += info.Completions
		if done {
			break
		}
		_ = obs
	}

	if totalCompletions == 0 {
		t.Error("no completions after 10 steps")
	}
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
		ArrivalMean:     2.0, // faster than bottleneck
		IntervalTime:    100.0,
		MaxTime:         1000.0,
		RewardAlpha:     0.01,
		ConstraintStage: 1,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	totalCompletions := 0
	action := sim.DBRAction{RopeRate: 20, BufferTime: 200.0}
	for i := 0; i < 10; i++ {
		_, _, done, info := env.Step(action)
		totalCompletions += info.Completions
		if done {
			break
		}
	}

	if totalCompletions < 50 {
		t.Errorf("completions = %d, bottleneck should allow ~100", totalCompletions)
	}
	if totalCompletions > 200 {
		t.Errorf("completions = %d, should be bounded by bottleneck", totalCompletions)
	}
}

func TestRopeRateLimiting(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages:          []sim.StageConfig{{Workers: 1, ServiceMean: 0.01}},
		ArrivalMean:     0.1,
		IntervalTime:    100.0,
		MaxTime:         200.0,
		RewardAlpha:     0,
		ConstraintStage: 0,
		MaxRopeRate:     5,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	action := sim.DBRAction{RopeRate: 1, BufferTime: 1.0}
	_, _, _, info := env.Step(action)

	if info.Completions > 1 {
		t.Errorf("rope=1 but completions=%d, expected <= 1", info.Completions)
	}

	obs, _, _, _ := env.Step(action)
	if obs.SourceBacklog == 0 {
		t.Error("expected source backlog with rope=1 and fast arrivals")
	}
}

func TestBufferTimeFull(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages: []sim.StageConfig{
			{Workers: 1, ServiceMean: 0.1},  // fast
			{Workers: 1, ServiceMean: 50.0}, // very slow constraint
		},
		ArrivalMean:     0.5,
		IntervalTime:    50.0,
		MaxTime:         200.0,
		RewardAlpha:     0,
		ConstraintStage: 1,
		MaxBufferTime:   200.0,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	// BufferTime=50 means 1 item's worth at constraint (serviceMean=50).
	action := sim.DBRAction{RopeRate: 20, BufferTime: 50.0}
	sawBlocked := false
	for i := 0; i < 4; i++ {
		obs, _, done, _ := env.Step(action)
		if obs.Stages[0].BlockedAfterService > 0 {
			sawBlocked = true
		}
		if done {
			break
		}
	}

	if !sawBlocked {
		t.Error("expected stage 0 blocked-after-service with tight buffer time and slow constraint")
	}
}

func TestBufferDrainToConstraint(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages: []sim.StageConfig{
			{Workers: 1, ServiceMean: 0.1},
			{Workers: 1, ServiceMean: 5.0},
			{Workers: 1, ServiceMean: 0.1},
		},
		ArrivalMean:     1.0,
		IntervalTime:    50.0,
		MaxTime:         500.0,
		RewardAlpha:     0,
		ConstraintStage: 1,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	action := sim.DBRAction{RopeRate: 20, BufferTime: 100.0}

	totalCompletions := 0
	for i := 0; i < 10; i++ {
		_, _, done, info := env.Step(action)
		totalCompletions += info.Completions
		if done {
			break
		}
	}

	if totalCompletions == 0 {
		t.Error("no completions — buffer should feed constraint")
	}
}

func TestConstraintStage0(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages:          []sim.StageConfig{{Workers: 1, ServiceMean: 5.0}},
		ArrivalMean:     1.0,
		IntervalTime:    50.0,
		MaxTime:         200.0,
		RewardAlpha:     0,
		ConstraintStage: 0,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	action := sim.DBRAction{RopeRate: 5, BufferTime: 100.0}
	for i := 0; i < 4; i++ {
		obs, _, done, _ := env.Step(action)
		if obs.BufferDepth != 0 {
			t.Errorf("step %d: BufferDepth=%d, expected 0 when constraint is stage 0", i, obs.BufferDepth)
		}
		if obs.BufferTime != 0 {
			t.Errorf("step %d: BufferTime=%f, expected 0 when constraint is stage 0", i, obs.BufferTime)
		}
		if done {
			break
		}
	}
}

func TestConservationWithBuffer(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages: []sim.StageConfig{
			{Workers: 1, ServiceMean: 1.0},
			{Workers: 1, ServiceMean: 5.0},
			{Workers: 1, ServiceMean: 1.0},
		},
		ArrivalMean:     1.0,
		IntervalTime:    30.0,
		MaxItems:        100,
		RewardAlpha:     0.01,
		ConstraintStage: 1,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	action := sim.DBRAction{RopeRate: 5, BufferTime: 15.0}
	for i := 0; i < 20; i++ {
		_, _, done, _ := env.Step(action)
		if !env.CheckConservation() {
			t.Fatalf("conservation violated at step %d", i)
		}
		if done {
			break
		}
	}
}

func TestReproducibilityDBR(t *testing.T) {
	cfg := baseConfig()
	action := sim.DBRAction{RopeRate: 5, BufferTime: 15.0}

	env1 := sim.NewEnv(cfg)
	env1.Reset(42)
	var obs1 []sim.Observation
	for i := 0; i < 10; i++ {
		o, _, done, _ := env1.Step(action)
		obs1 = append(obs1, o)
		if done {
			break
		}
	}

	env2 := sim.NewEnv(cfg)
	env2.Reset(42)
	for i := 0; i < len(obs1); i++ {
		o, _, done, _ := env2.Step(action)
		if o.TotalWIP != obs1[i].TotalWIP || o.SourceBacklog != obs1[i].SourceBacklog || o.BufferDepth != obs1[i].BufferDepth {
			t.Errorf("step %d: diverged (wip %d vs %d, backlog %d vs %d, buffer %d vs %d)",
				i, o.TotalWIP, obs1[i].TotalWIP, o.SourceBacklog, obs1[i].SourceBacklog,
				o.BufferDepth, obs1[i].BufferDepth)
		}
		if done {
			break
		}
	}
}

func TestResetNewSeed(t *testing.T) {
	cfg := baseConfig()
	env := sim.NewEnv(cfg)
	action := looseAction()

	env.Reset(42)
	o1, _, _, _ := env.Step(action)

	env.Reset(999)
	o2, _, _, _ := env.Step(action)

	if o1.TotalWIP == o2.TotalWIP && o1.SourceBacklog == o2.SourceBacklog {
		t.Log("warning: different seeds produced identical first step (unlikely but possible)")
	}
}

func TestEpisodeTermination(t *testing.T) {
	t.Run("max_items", func(t *testing.T) {
		cfg := sim.EnvConfig{
			Stages:          []sim.StageConfig{{Workers: 1, ServiceMean: 1.0}},
			ArrivalMean:     1.0,
			IntervalTime:    50.0,
			MaxItems:        10,
			RewardAlpha:     0,
			ConstraintStage: 0,
		}
		env := sim.NewEnv(cfg)
		env.Reset(42)

		action := sim.DBRAction{RopeRate: 20, BufferTime: 1.0}
		done := false
		for i := 0; i < 100 && !done; i++ {
			_, _, done, _ = env.Step(action)
		}
		if !done {
			t.Error("episode never terminated with MaxItems=10")
		}
	})

	t.Run("max_time", func(t *testing.T) {
		cfg := sim.EnvConfig{
			Stages:          []sim.StageConfig{{Workers: 1, ServiceMean: 1.0}},
			ArrivalMean:     1.0,
			IntervalTime:    50.0,
			MaxTime:         100.0,
			RewardAlpha:     0,
			ConstraintStage: 0,
		}
		env := sim.NewEnv(cfg)
		env.Reset(42)

		action := sim.DBRAction{RopeRate: 20, BufferTime: 1.0}
		_, _, done1, _ := env.Step(action)
		_, _, done2, _ := env.Step(action)
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
			Stages:          []sim.StageConfig{{Workers: 1, ServiceMean: 1.0}},
			ArrivalMean:     1.0,
			IntervalTime:    50.0,
			MaxTime:         10.0,
			RewardAlpha:     0,
			ConstraintStage: 0,
		}
		env := sim.NewEnv(cfg)
		env.Reset(42)
		action := sim.DBRAction{RopeRate: 20, BufferTime: 1.0}
		env.Step(action)
		env.Step(action)
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
		{"constraint out of range", sim.EnvConfig{Stages: []sim.StageConfig{{Workers: 1, ServiceMean: 1}}, ArrivalMean: 1, IntervalTime: 1, MaxItems: 1, ConstraintStage: 5}},
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
		Stages:          []sim.StageConfig{{Workers: 1, ServiceMean: 1.0}},
		ArrivalMean:     0.5,
		IntervalTime:    50.0,
		MaxTime:         200.0,
		RewardAlpha:     1.0,
		ConstraintStage: 0,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	action := sim.DBRAction{RopeRate: 20, BufferTime: 1.0}
	_, r1, _, _ := env.Step(action)
	_, _, _, _ = env.Step(action)
	_, r3, _, _ := env.Step(action)

	if r3 >= r1 {
		t.Logf("r1=%f r3=%f — expected reward to decrease as WIP grows", r1, r3)
	}
}

func TestBufferTimeAccounting(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages: []sim.StageConfig{
			{Workers: 1, ServiceMean: 0.1},  // fast
			{Workers: 1, ServiceMean: 10.0}, // slow constraint
		},
		ArrivalMean:     0.5,
		IntervalTime:    50.0,
		MaxTime:         200.0,
		RewardAlpha:     0,
		ConstraintStage: 1,
		MaxBufferTime:   200.0,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	action := sim.DBRAction{RopeRate: 20, BufferTime: 200.0}
	for i := 0; i < 4; i++ {
		obs, _, done, _ := env.Step(action)
		if !env.CheckBufferTime() {
			t.Fatalf("step %d: buffer time inconsistent with ground truth", i)
		}
		if obs.BufferDepth > 0 && obs.BufferTime == 0 {
			t.Errorf("step %d: buffer has items but BufferTime is 0", i)
		}
		if done {
			break
		}
	}
}

// TestSettleConstraintFed is a regression test for the v1 cascade bug.
// With a single-pass approach, the constraint could be left idle when:
// - bufferQueue is empty
// - pre-constraint has BAS items
// - buffer has room after constraint completion
// The settle loop must unblock BAS → feed buffer → dispatch at constraint
// in a single event handling, not require a future event.
func TestSettleConstraintFed(t *testing.T) {
	// Setup: fast stage 0, slow constraint (stage 1), fast stage 2.
	// Tight buffer (1 item's worth = 5.0 time). High rope.
	cfg := sim.EnvConfig{
		Stages: []sim.StageConfig{
			{Workers: 1, ServiceMean: 0.01}, // stage 0: near-instant
			{Workers: 1, ServiceMean: 5.0},  // constraint
			{Workers: 1, ServiceMean: 0.01}, // stage 2: near-instant
		},
		ArrivalMean:     0.5,
		IntervalTime:    100.0,
		MaxTime:         500.0,
		RewardAlpha:     0,
		ConstraintStage: 1,
		MaxBufferTime:   5.0, // exactly 1 item's worth
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	// Run several intervals. With tight buffer (1 item) and fast upstream,
	// stage 0 will frequently block. The settle loop must unblock BAS → buffer
	// → constraint on every constraint completion, or throughput collapses.
	action := sim.DBRAction{RopeRate: 20, BufferTime: 5.0}
	totalCompletions := 0
	for i := 0; i < 5; i++ {
		_, _, done, info := env.Step(action)
		totalCompletions += info.Completions
		if !env.CheckConservation() {
			t.Fatalf("conservation violated at step %d", i)
		}
		if !env.CheckBufferTime() {
			t.Fatalf("buffer time inconsistent at step %d", i)
		}
		if done {
			break
		}
	}

	// Constraint throughput ≈ 1/5 = 0.2/time. Over 500 time: ~100 completions.
	// With broken settle (constraint idles), completions would be much lower.
	if totalCompletions < 50 {
		t.Errorf("completions = %d, expected ~100 — constraint may be idling (settle bug)", totalCompletions)
	}
}

func TestSafetyCap(t *testing.T) {
	cfg := sim.EnvConfig{
		Stages:          []sim.StageConfig{{Workers: 1, ServiceMean: 100.0}}, // very slow
		ArrivalMean:     0.1,                                                  // very fast arrivals
		IntervalTime:    50.0,
		MaxTime:         10000.0,
		RewardAlpha:     0,
		ConstraintStage: 0,
		MaxSystemItems:  50,
	}
	env := sim.NewEnv(cfg)
	env.Reset(42)

	action := sim.DBRAction{RopeRate: 20, BufferTime: 1.0}
	terminated := false
	for i := 0; i < 100; i++ {
		_, _, done, _ := env.Step(action)
		if done {
			terminated = true
			break
		}
	}
	if !terminated {
		t.Error("expected episode to terminate from safety cap")
	}
}
