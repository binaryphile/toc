// Package adapter provides types and logic for polling external systems
// and converting their metrics into toc ObservationBatches.
package adapter

// MetricsMask distinguishes "not observed" from "observed zero."
// Aligned with core.ObservationMask convention.
type MetricsMask uint16

const (
	HasCompletions MetricsMask = 1 << iota
	HasFailures
	HasArrivals
	HasQueueDepth
	HasWorkers
	HasBusyNs
	HasIdleNs
	HasBlockedNs
)

// StageMetrics is the common intermediate produced by all Source
// implementations. Counters are cumulative (monotonically increasing
// across polls); gauges are point-in-time snapshots. Only fields with
// corresponding Mask bits set are valid — zero without a mask bit
// means "not observed," not "observed zero."
type StageMetrics struct {
	Mask        MetricsMask
	Completions int64 // cumulative
	Failures    int64 // cumulative
	Arrivals    int64 // cumulative
	QueueDepth  int64 // gauge
	Workers     int64 // gauge
	BusyNs      int64 // cumulative nanoseconds
	IdleNs      int64 // cumulative nanoseconds
	BlockedNs   int64 // cumulative nanoseconds
}
