package adapter

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/binaryphile/toc"
	"github.com/binaryphile/toc/core"
)

// Run executes the poll loop. It polls sources at cfg.PollInterval
// (with +/-10% jitter), computes deltas, and publishes observation
// batches. Returns nil on context cancellation (clean shutdown).
// Closes all sources on exit.
func Run(ctx context.Context, cfg Config, sources []NamedSource, pub toc.ObservationPublisher, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}

	// Ensure all sources are closed on exit.
	defer func() {
		for _, src := range sources {
			if err := src.Close(); err != nil {
				logger.Error("source close failed", "error", err)
			}
		}
	}()

	dt := NewDeltaTracker(cfg.StalenessWindows, logger)

	// Initial poll to establish baseline.
	doPollCycle(ctx, cfg, sources, dt, pub, logger)

	interval := cfg.PollInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			doPollCycle(ctx, cfg, sources, dt, pub, logger)
			ticker.Reset(jitterDuration(interval))
		}
	}
}

// doPollCycle runs one poll-compute-publish cycle.
func doPollCycle(
	ctx context.Context,
	cfg Config,
	sources []NamedSource,
	dt *DeltaTracker,
	pub toc.ObservationPublisher,
	logger *slog.Logger,
) {
	// Global deadline for this cycle.
	pollCtx, cancel := context.WithDeadline(ctx, time.Now().Add(cfg.PollInterval))
	defer cancel()

	// Poll all sources, merge results.
	merged := make(map[string]StageMetrics)
	for _, src := range sources {
		metrics, err := src.Poll(pollCtx)
		if err != nil {
			logger.Error("source poll failed",
				"source_type", src.Type,
				"source_url", src.URL,
				"error", err,
			)
			continue
		}
		for name, m := range metrics {
			merged[name] = m
		}
	}

	if len(merged) == 0 {
		return
	}

	now := time.Now()
	obs := dt.Step(now, merged)
	if len(obs) == 0 {
		return
	}

	// Sort observations by stage name for deterministic output.
	sort.Slice(obs, func(i, j int) bool {
		return obs[i].Stage < obs[j].Stage
	})

	batch := toc.ObservationBatch{
		PipelineID:         cfg.PipelineID,
		TimestampUnixNano:  now.UnixNano(),
		WindowDurationNano: cfg.PollInterval.Nanoseconds(),
		Observations:       obs,
	}

	if err := pub.PublishObservations(pollCtx, batch); err != nil {
		logger.Error("publish failed",
			"pipeline", cfg.PipelineID,
			"stages", stageNames(obs),
			"error", err,
		)
	}
}

func stageNames(obs []core.StageObservation) []string {
	names := make([]string, len(obs))
	for i, o := range obs {
		names[i] = o.Stage
	}
	return names
}

// jitterDuration returns d +/- 10% uniformly distributed.
func jitterDuration(d time.Duration) time.Duration {
	jitter := float64(d) * 0.1
	offset := (rand.Float64()*2 - 1) * jitter
	return d + time.Duration(offset)
}
