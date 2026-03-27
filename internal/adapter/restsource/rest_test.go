package restsource

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"codeberg.org/binaryphile/toc/internal/adapter"
)

var discardLogger = slog.New(slog.DiscardHandler)

func newTestSource(t *testing.T, url string, stages []adapter.StageExtraction) *RESTSource {
	t.Helper()
	// Apply same defaults as LoadConfig normalization.
	for i := range stages {
		for k, fc := range stages[i].Fields {
			if fc.Multiplier == 0 {
				fc.Multiplier = 1.0
				stages[i].Fields[k] = fc
			}
		}
	}
	src, err := NewRESTSource(adapter.SourceConfig{
		Type:    "rest",
		URL:     url,
		Timeout: 5 * time.Second,
		Stages:  stages,
	}, discardLogger)
	if err != nil {
		t.Fatalf("NewRESTSource: %v", err)
	}
	return src
}

func TestPoll_SingleObject(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"completed": 150, "failed": 3, "queue": 10, "workers": 4}`))
	}))
	defer srv.Close()

	src := newTestSource(t, srv.URL, []adapter.StageExtraction{
		{
			Name: "stage-a",
			Fields: map[string]adapter.FieldConfig{
				"completions": {Path: "completed"},
				"failures":    {Path: "failed"},
				"queue_depth": {Path: "queue"},
				"workers":     {Path: "workers"},
			},
		},
	})

	metrics, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	m, ok := metrics["stage-a"]
	if !ok {
		t.Fatal("stage-a not in results")
	}
	if m.Completions != 150 {
		t.Errorf("completions = %d, want 150", m.Completions)
	}
	if m.Failures != 3 {
		t.Errorf("failures = %d, want 3", m.Failures)
	}
	if m.QueueDepth != 10 {
		t.Errorf("queue_depth = %d, want 10", m.QueueDepth)
	}
	if m.Workers != 4 {
		t.Errorf("workers = %d, want 4", m.Workers)
	}
	if m.Mask&adapter.HasCompletions == 0 {
		t.Error("mask missing HasCompletions")
	}
	if m.Mask&adapter.HasFailures == 0 {
		t.Error("mask missing HasFailures")
	}
	if m.Mask&adapter.HasQueueDepth == 0 {
		t.Error("mask missing HasQueueDepth")
	}
	if m.Mask&adapter.HasWorkers == 0 {
		t.Error("mask missing HasWorkers")
	}
}

func TestPoll_ArrayWithSelector(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
			"stages": [
				{"name": "ingest", "completed": 100, "queue": 5},
				{"name": "transform", "completed": 200, "queue": 12}
			]
		}`))
	}))
	defer srv.Close()

	src := newTestSource(t, srv.URL, []adapter.StageExtraction{
		{
			Name:     "ingest",
			Selector: `stages.#(name=="ingest")`,
			Fields: map[string]adapter.FieldConfig{
				"completions": {Path: "completed"},
				"queue_depth": {Path: "queue"},
			},
		},
		{
			Name:     "transform",
			Selector: `stages.#(name=="transform")`,
			Fields: map[string]adapter.FieldConfig{
				"completions": {Path: "completed"},
				"queue_depth": {Path: "queue"},
			},
		},
	})

	metrics, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if m := metrics["ingest"]; m.Completions != 100 || m.QueueDepth != 5 {
		t.Errorf("ingest = %+v", m)
	}
	if m := metrics["transform"]; m.Completions != 200 || m.QueueDepth != 12 {
		t.Errorf("transform = %+v", m)
	}
}

func TestPoll_MissingOptionalField(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"completed": 50}`))
	}))
	defer srv.Close()

	src := newTestSource(t, srv.URL, []adapter.StageExtraction{
		{
			Name: "s",
			Fields: map[string]adapter.FieldConfig{
				"completions": {Path: "completed"},
				"failures":    {Path: "failed"}, // missing, optional
			},
		},
	})

	metrics, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	m := metrics["s"]
	if m.Mask&adapter.HasCompletions == 0 {
		t.Error("should have HasCompletions")
	}
	if m.Mask&adapter.HasFailures != 0 {
		t.Error("should NOT have HasFailures (optional, missing)")
	}
}

func TestPoll_MissingRequiredField(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"queue": 5}`))
	}))
	defer srv.Close()

	src := newTestSource(t, srv.URL, []adapter.StageExtraction{
		{
			Name: "s",
			Fields: map[string]adapter.FieldConfig{
				"completions": {Path: "completed", Required: true},
				"queue_depth": {Path: "queue"},
			},
		},
	})

	metrics, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll should not return error for stage skip: %v", err)
	}

	if _, ok := metrics["s"]; ok {
		t.Error("stage with missing required field should be skipped")
	}
}

func TestPoll_Multiplier(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"busy_seconds": 1.5}`))
	}))
	defer srv.Close()

	src := newTestSource(t, srv.URL, []adapter.StageExtraction{
		{
			Name: "s",
			Fields: map[string]adapter.FieldConfig{
				"busy_ns": {Path: "busy_seconds", Multiplier: 1e9},
			},
		},
	})

	metrics, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	m := metrics["s"]
	if m.BusyNs != 1_500_000_000 {
		t.Errorf("busy_ns = %d, want 1500000000", m.BusyNs)
	}
	if m.Mask&adapter.HasBusyNs == 0 {
		t.Error("mask missing HasBusyNs")
	}
}

func TestPoll_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	src := newTestSource(t, srv.URL, []adapter.StageExtraction{
		{
			Name: "s",
			Fields: map[string]adapter.FieldConfig{
				"completions": {Path: "completed"},
			},
		},
	})

	_, err := src.Poll(context.Background())
	if err == nil {
		t.Fatal("expected error for HTTP 500")
	}
}
