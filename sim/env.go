package sim

import "fmt"

// StageConfig configures one stage in the pipeline.
type StageConfig struct {
	Workers     int     // parallel servers; >= 1
	ServiceMean float64 // exponential mean service time; > 0
}

// EnvConfig configures the RL environment.
type EnvConfig struct {
	Stages       []StageConfig
	ArrivalMean  float64 // > 0
	IntervalTime float64 // sim time per Step; > 0
	MaxItems     int     // episode ends after N completions; 0 = use MaxTime
	MaxTime      float64 // episode ends after this sim time; 0 = use MaxItems
	RewardAlpha  float64 // WIP penalty coefficient; default 0.01
}

// StageObs is the per-stage observation (current state only).
type StageObs struct {
	Queued              int
	InService           int
	BlockedAfterService int
	Workers             int
	MaxWIP              int
}

// Observation is the RL observation (current state only).
type Observation struct {
	Stages        []StageObs
	SourceBacklog int
	TotalWIP      int
	SimTime       float64
}

// StepInfo contains interval diagnostics (not for policy input).
type StepInfo struct {
	Completions      int
	IntervalTime     float64
	AvgTotalWIP      float64
	AvgSourceBacklog float64
}

// Env is the RL environment wrapping a multi-stage DES.
type Env struct {
	config EnvConfig
	des    *desEngine
	done   bool
}

// NewEnv creates an environment. Panics on invalid config.
func NewEnv(cfg EnvConfig) *Env {
	validateConfig(cfg)
	if cfg.RewardAlpha == 0 {
		cfg.RewardAlpha = 0.01
	}
	return &Env{config: cfg}
}

// Reset starts a new episode with the given seed. Returns initial observation.
func (e *Env) Reset(seed uint64) Observation {
	e.des = newDES(e.config, seed)
	e.done = false
	return e.des.snapshot()
}

// Step applies actions, runs one interval, returns observation, reward, done, info.
// actions[i] = MaxWIP for stage i (0 = unbounded).
// Panics if called after done or before Reset.
func (e *Env) Step(actions []int) (Observation, float64, bool, StepInfo) {
	if e.des == nil {
		panic("sim.Env.Step: call Reset first")
	}
	if e.done {
		panic("sim.Env.Step: episode is done")
	}
	if len(actions) != len(e.config.Stages) {
		panic(fmt.Sprintf("sim.Env.Step: actions length %d != stages %d", len(actions), len(e.config.Stages)))
	}

	// Apply actions.
	e.des.setActions(actions)

	// Run DES for one interval.
	e.des.resetInterval()
	boundary := e.des.simTime + e.config.IntervalTime
	e.des.runUntil(boundary)

	// Compute reward.
	intervalTime := e.config.IntervalTime
	avgWIP := e.des.intervalWIPArea / intervalTime
	avgBacklog := e.des.intervalBacklogArea / intervalTime
	reward := float64(e.des.intervalCompletions)/intervalTime - e.config.RewardAlpha*avgWIP

	// Check done.
	if e.config.MaxItems > 0 && e.des.completed >= e.config.MaxItems {
		e.done = true
	}
	if e.config.MaxTime > 0 && e.des.simTime >= e.config.MaxTime {
		e.done = true
	}

	info := StepInfo{
		Completions:      e.des.intervalCompletions,
		IntervalTime:     intervalTime,
		AvgTotalWIP:      avgWIP,
		AvgSourceBacklog: avgBacklog,
	}

	return e.des.snapshot(), reward, e.done, info
}

// CheckConservation verifies arrivals = backlog + WIP + completed.
func (e *Env) CheckConservation() bool {
	if e.des == nil {
		return true
	}
	return e.des.conservation()
}

func validateConfig(cfg EnvConfig) {
	if len(cfg.Stages) == 0 {
		panic("sim.EnvConfig: at least one stage required")
	}
	for i, sc := range cfg.Stages {
		if sc.Workers < 1 {
			panic(fmt.Sprintf("sim.EnvConfig: stage %d workers must be >= 1", i))
		}
		if sc.ServiceMean <= 0 {
			panic(fmt.Sprintf("sim.EnvConfig: stage %d service mean must be > 0", i))
		}
	}
	if cfg.ArrivalMean <= 0 {
		panic("sim.EnvConfig: arrival mean must be > 0")
	}
	if cfg.IntervalTime <= 0 {
		panic("sim.EnvConfig: interval time must be > 0")
	}
	if cfg.MaxItems <= 0 && cfg.MaxTime <= 0 {
		panic("sim.EnvConfig: at least one of MaxItems or MaxTime required")
	}
}
