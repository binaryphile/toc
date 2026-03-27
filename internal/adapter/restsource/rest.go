// Package restsource implements [adapter.Source] for HTTP/JSON endpoints.
package restsource

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"

	"codeberg.org/binaryphile/toc/internal/adapter"
	"github.com/tidwall/gjson"
)

// maxResponseBytes limits response body reads to 10 MB.
const maxResponseBytes = 10 << 20

// stageSpec holds extraction config for one stage.
type stageSpec struct {
	name     string
	selector string
	fields   map[string]adapter.FieldConfig
}

// RESTSource polls a single HTTP endpoint and extracts stage metrics
// via gjson selectors.
type RESTSource struct {
	client *http.Client
	url    string
	stages []stageSpec
	logger *slog.Logger
}

// NewRESTSource creates a RESTSource from a SourceConfig.
func NewRESTSource(cfg adapter.SourceConfig, logger *slog.Logger) (*RESTSource, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("restsource: url is required")
	}
	if len(cfg.Stages) == 0 {
		return nil, fmt.Errorf("restsource: at least one stage is required")
	}
	if logger == nil {
		logger = slog.Default()
	}

	specs := make([]stageSpec, len(cfg.Stages))
	for i, s := range cfg.Stages {
		specs[i] = stageSpec{
			name:     s.Name,
			selector: s.Selector,
			fields:   s.Fields,
		}
	}

	return &RESTSource{
		client: &http.Client{Timeout: cfg.Timeout},
		url:    cfg.URL,
		stages: specs,
		logger: logger,
	}, nil
}

// Poll fetches the endpoint and extracts stage metrics.
func (r *RESTSource) Poll(ctx context.Context) (map[string]adapter.StageMetrics, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return nil, fmt.Errorf("restsource: create request: %w", err)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("restsource: %s: %w", r.url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("restsource: %s: HTTP %d", r.url, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("restsource: %s: read body: %w", r.url, err)
	}

	root := gjson.ParseBytes(body)
	result := make(map[string]adapter.StageMetrics, len(r.stages))

	for _, spec := range r.stages {
		m, err := r.extractStage(root, spec)
		if err != nil {
			r.logger.Error("stage extraction failed",
				"source", r.url,
				"stage", spec.name,
				"error", err,
			)
			continue // skip stage, don't fail source
		}
		result[spec.name] = m
	}

	return result, nil
}

// Close releases resources. RESTSource uses a standard http.Client
// which doesn't require explicit cleanup.
func (r *RESTSource) Close() error {
	return nil
}

// extractStage extracts metrics for one stage from the JSON response.
func (r *RESTSource) extractStage(root gjson.Result, spec stageSpec) (adapter.StageMetrics, error) {
	target := root
	if spec.selector != "" {
		target = root.Get(spec.selector)
		if !target.Exists() {
			return adapter.StageMetrics{}, fmt.Errorf("selector %q matched nothing", spec.selector)
		}
	}

	var m adapter.StageMetrics
	for name, fc := range spec.fields {
		val := target.Get(fc.Path)
		if !val.Exists() {
			if fc.Required {
				return adapter.StageMetrics{}, fmt.Errorf("required field %q (path %q) not found", name, fc.Path)
			}
			continue // optional field missing — don't set mask bit
		}

		v := val.Float()
		if math.IsNaN(v) || math.IsInf(v, 0) {
			r.logger.Warn("non-finite numeric value, skipping",
				"source", r.url, "stage", spec.name, "field", name, "value", val.Raw)
			continue
		}

		v *= fc.Multiplier // already defaulted to 1.0 during config normalization
		iv := int64(math.Round(v))

		switch name {
		case "completions":
			m.Completions = iv
			m.Mask |= adapter.HasCompletions
		case "failures":
			m.Failures = iv
			m.Mask |= adapter.HasFailures
		case "arrivals":
			m.Arrivals = iv
			m.Mask |= adapter.HasArrivals
		case "queue_depth":
			m.QueueDepth = iv
			m.Mask |= adapter.HasQueueDepth
		case "workers":
			m.Workers = iv
			m.Mask |= adapter.HasWorkers
		case "busy_ns":
			m.BusyNs = iv
			m.Mask |= adapter.HasBusyNs
		case "idle_ns":
			m.IdleNs = iv
			m.Mask |= adapter.HasIdleNs
		case "blocked_ns":
			m.BlockedNs = iv
			m.Mask |= adapter.HasBlockedNs
		}
	}

	return m, nil
}

// Compile-time interface check.
var _ adapter.Source = (*RESTSource)(nil)
