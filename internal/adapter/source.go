package adapter

import "context"

// Source polls an external system and returns metrics for one or more
// pipeline stages. Implementations are system-scoped: a single Poll
// call may fetch data from one endpoint and extract metrics for
// multiple stages.
//
// Poll must be safe for sequential calls from the poll loop.
// Concurrent calls are not required.
type Source interface {
	// Poll fetches current metrics from the external system.
	// Returns a map of stage name → StageMetrics with cumulative
	// counters and point-in-time gauges. Missing stages (extraction
	// failure, filtered out) should be omitted from the map, not
	// returned with empty masks.
	Poll(ctx context.Context) (map[string]StageMetrics, error)

	// Close releases resources (HTTP clients, DB connections, etc).
	Close() error
}

// NamedSource pairs a Source with identity for logging.
type NamedSource struct {
	Source
	Type string
	URL  string
}
