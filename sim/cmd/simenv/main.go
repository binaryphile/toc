// Command simenv wraps sim.Env as a JSON-over-stdio subprocess for
// Python RL training. Protocol v2: line-delimited JSON, synchronous
// request/response. DBR action space: [rope_rate, buffer_time_step].
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"

	"codeberg.org/binaryphile/toc/sim"
)

const (
	protocolVersion = 2
	maxRopeCap      = 20
	maxBufStepsCap  = 20
)

type protocolState int

const (
	stateInit protocolState = iota
	stateConfigured
	stateActive
	stateDone
)

// ── Wire types ──────────────────────────────────────────────────────────

type request struct {
	ID      *int        `json:"id,omitempty"`
	Cmd     string      `json:"cmd"`
	Config  *wireConfig `json:"config,omitempty"`
	Seed    *uint64     `json:"seed,omitempty"`
	Actions []int       `json:"actions,omitempty"`
}

type wireConfig struct {
	Stages          []wireStage `json:"stages"`
	ArrivalMean     float64     `json:"arrival_mean"`
	IntervalTime    float64     `json:"interval_time"`
	MaxItems        int         `json:"max_items"`
	MaxTime         float64     `json:"max_time"`
	RewardAlpha     float64     `json:"reward_alpha"`
	ConstraintStage int         `json:"constraint_stage"`
	MaxRopeRate     int         `json:"max_rope_rate"`
	MaxBufSteps     int         `json:"max_buf_steps"`
}

type wireStage struct {
	Workers     int     `json:"workers"`
	ServiceMean float64 `json:"service_mean"`
}

type response struct {
	ID              *int       `json:"id,omitempty"`
	OK              bool       `json:"ok"`
	ProtocolVersion int        `json:"protocol_version,omitempty"`
	ObsDim          int        `json:"obs_dim,omitempty"`
	ActionDims      []int      `json:"action_dims,omitempty"`
	Obs             []float64  `json:"obs,omitempty"`
	Reward          *float64   `json:"reward,omitempty"`
	Terminated      *bool      `json:"terminated,omitempty"`
	Truncated       *bool      `json:"truncated,omitempty"`
	Info            *wireInfo  `json:"info,omitempty"`
	Error           *wireError `json:"error,omitempty"`
}

type wireInfo struct {
	Completions  int     `json:"completions"`
	IntervalTime float64 `json:"interval_time"`
	AvgWIP       float64 `json:"avg_wip"`
	AvgBacklog   float64 `json:"avg_backlog"`
}

type wireError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ── Main ────────────────────────────────────────────────────────────────

func main() {
	log.SetOutput(os.Stderr)
	log.SetPrefix("[simenv] ")

	reader := bufio.NewReader(os.Stdin)
	enc := json.NewEncoder(os.Stdout)

	var env *sim.Env
	var state protocolState
	var maxRope, maxBufSteps int
	var constraintMean float64 // for mapping buffer step → time

	send := func(r response) {
		enc.Encode(r)
	}

	errResp := func(id *int, code, msg string) response {
		return response{ID: id, OK: false, Error: &wireError{Code: code, Message: msg}}
	}

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("stdin read error: %v", err)
			}
			return
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			send(errResp(nil, "parse", err.Error()))
			continue
		}

		switch req.Cmd {
		case "config":
			if req.Config == nil {
				send(errResp(req.ID, "invalid_config", "config field required"))
				continue
			}
			cfg, bufSteps, cMean, err := parseConfig(req.Config)
			if err != nil {
				send(errResp(req.ID, "invalid_config", err.Error()))
				continue
			}
			env = sim.NewEnv(cfg)
			maxRope = cfg.MaxRopeRate
			maxBufSteps = bufSteps
			constraintMean = cMean
			state = stateConfigured

			numStages := len(cfg.Stages)
			send(response{
				ID:              req.ID,
				OK:              true,
				ProtocolVersion: protocolVersion,
				ObsDim:          numStages*4 + 6,
				ActionDims:      []int{maxRope, maxBufSteps},
			})

		case "reset":
			if state == stateInit {
				send(errResp(req.ID, "invalid_state", "must config before reset"))
				continue
			}
			if req.Seed == nil {
				send(errResp(req.ID, "missing_seed", "seed required"))
				continue
			}
			obs := env.Reset(*req.Seed)
			state = stateActive
			send(response{ID: req.ID, OK: true, Obs: flattenObs(obs)})

		case "step":
			if state != stateActive {
				send(errResp(req.ID, "invalid_state", "must reset before step (or episode is done)"))
				continue
			}
			if len(req.Actions) != 2 {
				send(errResp(req.ID, "invalid_action",
					fmt.Sprintf("expected 2 actions [rope_rate, buffer_step], got %d", len(req.Actions))))
				continue
			}
			ropeIdx, bufIdx := req.Actions[0], req.Actions[1]
			if ropeIdx < 0 || ropeIdx >= maxRope {
				send(errResp(req.ID, "invalid_action",
					fmt.Sprintf("rope_rate index %d out of range [0,%d)", ropeIdx, maxRope)))
				continue
			}
			if bufIdx < 0 || bufIdx >= maxBufSteps {
				send(errResp(req.ID, "invalid_action",
					fmt.Sprintf("buffer_step index %d out of range [0,%d)", bufIdx, maxBufSteps)))
				continue
			}
			action := sim.DBRAction{
				RopeRate:   ropeIdx + 1,                            // action 0 → rope rate 1
				BufferTime: float64(bufIdx+1) * constraintMean, // action 0 → 1×serviceMean
			}

			obs, reward, done, info := env.Step(action)

			terminated := done && info.Completions > 0
			truncated := done && !terminated

			state = stateActive
			if done {
				state = stateDone
			}

			t := terminated
			tr := truncated
			send(response{
				ID:         req.ID,
				OK:         true,
				Obs:        flattenObs(obs),
				Reward:     &reward,
				Terminated: &t,
				Truncated:  &tr,
				Info: &wireInfo{
					Completions:  info.Completions,
					IntervalTime: info.IntervalTime,
					AvgWIP:       info.AvgTotalWIP,
					AvgBacklog:   info.AvgSourceBacklog,
				},
			})

		default:
			send(errResp(req.ID, "unknown_cmd", req.Cmd))
		}
	}
}

func parseConfig(wc *wireConfig) (sim.EnvConfig, int, float64, error) {
	if len(wc.Stages) == 0 {
		return sim.EnvConfig{}, 0, 0, fmt.Errorf("at least one stage required")
	}
	stages := make([]sim.StageConfig, len(wc.Stages))
	for i, ws := range wc.Stages {
		if ws.Workers < 1 {
			return sim.EnvConfig{}, 0, 0, fmt.Errorf("stage %d: workers must be >= 1", i)
		}
		if ws.ServiceMean <= 0 || math.IsNaN(ws.ServiceMean) || math.IsInf(ws.ServiceMean, 0) {
			return sim.EnvConfig{}, 0, 0, fmt.Errorf("stage %d: service_mean must be positive and finite", i)
		}
		stages[i] = sim.StageConfig{Workers: ws.Workers, ServiceMean: ws.ServiceMean}
	}
	if wc.ArrivalMean <= 0 || math.IsNaN(wc.ArrivalMean) || math.IsInf(wc.ArrivalMean, 0) {
		return sim.EnvConfig{}, 0, 0, fmt.Errorf("arrival_mean must be positive and finite")
	}
	if wc.IntervalTime <= 0 || math.IsNaN(wc.IntervalTime) || math.IsInf(wc.IntervalTime, 0) {
		return sim.EnvConfig{}, 0, 0, fmt.Errorf("interval_time must be positive and finite")
	}
	if wc.MaxItems <= 0 && wc.MaxTime <= 0 {
		return sim.EnvConfig{}, 0, 0, fmt.Errorf("at least one of max_items or max_time required")
	}
	if math.IsNaN(wc.RewardAlpha) || math.IsInf(wc.RewardAlpha, 0) {
		return sim.EnvConfig{}, 0, 0, fmt.Errorf("reward_alpha must be finite")
	}
	if wc.ConstraintStage < 0 || wc.ConstraintStage >= len(wc.Stages) {
		return sim.EnvConfig{}, 0, 0, fmt.Errorf("constraint_stage %d out of range [0,%d)", wc.ConstraintStage, len(wc.Stages))
	}

	maxRope := wc.MaxRopeRate
	if maxRope <= 0 {
		maxRope = maxRopeCap
	}
	bufSteps := wc.MaxBufSteps
	if bufSteps <= 0 {
		bufSteps = maxBufStepsCap
	}

	constraintMean := stages[wc.ConstraintStage].ServiceMean
	maxBufTime := float64(bufSteps) * constraintMean

	return sim.EnvConfig{
		Stages:          stages,
		ArrivalMean:     wc.ArrivalMean,
		IntervalTime:    wc.IntervalTime,
		MaxItems:        wc.MaxItems,
		MaxTime:         wc.MaxTime,
		RewardAlpha:     wc.RewardAlpha,
		ConstraintStage: wc.ConstraintStage,
		MaxRopeRate:     maxRope,
		MaxBufferTime:   maxBufTime,
	}, bufSteps, constraintMean, nil
}

func flattenObs(obs sim.Observation) []float64 {
	n := len(obs.Stages)*4 + 6
	out := make([]float64, 0, n)
	for _, s := range obs.Stages {
		out = append(out,
			float64(s.Queued),
			float64(s.InService),
			float64(s.BlockedAfterService),
			float64(s.Workers),
		)
	}
	out = append(out,
		float64(obs.SourceBacklog),
		float64(obs.TotalWIP),
		obs.SimTime,
		float64(obs.BufferDepth),
		obs.BufferTime,
		float64(obs.RopeRate),
	)
	return out
}
