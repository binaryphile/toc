// Command simenv wraps sim.Env as a JSON-over-stdio subprocess for
// Python RL training. Protocol v1: line-delimited JSON, synchronous
// request/response. See plan for full protocol spec.
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
	protocolVersion = 1
	maxWIPCap       = 20
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
	ID      *int            `json:"id,omitempty"`
	Cmd     string          `json:"cmd"`
	Config  *wireConfig     `json:"config,omitempty"`
	Seed    *uint64         `json:"seed,omitempty"`
	Actions []int           `json:"actions,omitempty"`
}

type wireConfig struct {
	Stages       []wireStage `json:"stages"`
	ArrivalMean  float64     `json:"arrival_mean"`
	IntervalTime float64     `json:"interval_time"`
	MaxItems     int         `json:"max_items"`
	MaxTime      float64     `json:"max_time"`
	RewardAlpha  float64     `json:"reward_alpha"`
}

type wireStage struct {
	Workers     int     `json:"workers"`
	ServiceMean float64 `json:"service_mean"`
}

type response struct {
	ID              *int          `json:"id,omitempty"`
	OK              bool          `json:"ok"`
	ProtocolVersion int           `json:"protocol_version,omitempty"`
	ObsDim          int           `json:"obs_dim,omitempty"`
	ActionDims      []int         `json:"action_dims,omitempty"`
	Obs             []float64     `json:"obs,omitempty"`
	Reward          *float64      `json:"reward,omitempty"`
	Terminated      *bool         `json:"terminated,omitempty"`
	Truncated       *bool         `json:"truncated,omitempty"`
	Info            *wireInfo     `json:"info,omitempty"`
	Error           *wireError    `json:"error,omitempty"`
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

// ── Main ───────────────────────────────────────────���────────────────────

func main() {
	log.SetOutput(os.Stderr)
	log.SetPrefix("[simenv] ")

	reader := bufio.NewReader(os.Stdin)
	enc := json.NewEncoder(os.Stdout)

	var env *sim.Env
	var state protocolState
	var numStages int

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
			cfg, err := parseConfig(req.Config)
			if err != nil {
				send(errResp(req.ID, "invalid_config", err.Error()))
				continue
			}
			env = sim.NewEnv(cfg)
			numStages = len(cfg.Stages)
			state = stateConfigured

			dims := make([]int, numStages)
			for i := range dims {
				dims[i] = maxWIPCap
			}
			send(response{
				ID:              req.ID,
				OK:              true,
				ProtocolVersion: protocolVersion,
				ObsDim:          numStages*5 + 3,
				ActionDims:      dims,
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
			if len(req.Actions) != numStages {
				send(errResp(req.ID, "invalid_action",
					fmt.Sprintf("expected %d actions, got %d", numStages, len(req.Actions))))
				continue
			}
			// Validate and map actions.
			wipActions := make([]int, numStages)
			valid := true
			for i, a := range req.Actions {
				if a < 0 || a >= maxWIPCap {
					send(errResp(req.ID, "invalid_action",
						fmt.Sprintf("action[%d]=%d out of range [0,%d)", i, a, maxWIPCap)))
					valid = false
					break
				}
				wipActions[i] = a + 1 // action j → MaxWIP j+1
			}
			if !valid {
				continue
			}

			obs, reward, done, info := env.Step(wipActions)

			terminated := done && info.Completions > 0 // heuristic: completions at done = max_items
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

func parseConfig(wc *wireConfig) (sim.EnvConfig, error) {
	if len(wc.Stages) == 0 {
		return sim.EnvConfig{}, fmt.Errorf("at least one stage required")
	}
	stages := make([]sim.StageConfig, len(wc.Stages))
	for i, ws := range wc.Stages {
		if ws.Workers < 1 {
			return sim.EnvConfig{}, fmt.Errorf("stage %d: workers must be >= 1", i)
		}
		if ws.ServiceMean <= 0 || math.IsNaN(ws.ServiceMean) || math.IsInf(ws.ServiceMean, 0) {
			return sim.EnvConfig{}, fmt.Errorf("stage %d: service_mean must be positive and finite", i)
		}
		stages[i] = sim.StageConfig{Workers: ws.Workers, ServiceMean: ws.ServiceMean}
	}
	if wc.ArrivalMean <= 0 || math.IsNaN(wc.ArrivalMean) || math.IsInf(wc.ArrivalMean, 0) {
		return sim.EnvConfig{}, fmt.Errorf("arrival_mean must be positive and finite")
	}
	if wc.IntervalTime <= 0 || math.IsNaN(wc.IntervalTime) || math.IsInf(wc.IntervalTime, 0) {
		return sim.EnvConfig{}, fmt.Errorf("interval_time must be positive and finite")
	}
	if wc.MaxItems <= 0 && wc.MaxTime <= 0 {
		return sim.EnvConfig{}, fmt.Errorf("at least one of max_items or max_time required")
	}
	if math.IsNaN(wc.RewardAlpha) || math.IsInf(wc.RewardAlpha, 0) {
		return sim.EnvConfig{}, fmt.Errorf("reward_alpha must be finite")
	}
	return sim.EnvConfig{
		Stages:       stages,
		ArrivalMean:  wc.ArrivalMean,
		IntervalTime: wc.IntervalTime,
		MaxItems:     wc.MaxItems,
		MaxTime:      wc.MaxTime,
		RewardAlpha:  wc.RewardAlpha,
	}, nil
}

func flattenObs(obs sim.Observation) []float64 {
	n := len(obs.Stages)*5 + 3
	out := make([]float64, 0, n)
	for _, s := range obs.Stages {
		out = append(out,
			float64(s.Queued),
			float64(s.InService),
			float64(s.BlockedAfterService),
			float64(s.Workers),
			float64(s.MaxWIP),
		)
	}
	out = append(out,
		float64(obs.SourceBacklog),
		float64(obs.TotalWIP),
		obs.SimTime,
	)
	return out
}
