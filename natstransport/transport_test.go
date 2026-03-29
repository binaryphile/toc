package natstransport_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/binaryphile/toc"
	"codeberg.org/binaryphile/toc/core"
	"codeberg.org/binaryphile/toc/natstransport"
	"codeberg.org/binaryphile/toc/tocpb"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// startServer starts an embedded NATS server for testing.
func startServer(t *testing.T, opts ...func(*server.Options)) *server.Server {
	t.Helper()
	o := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true}
	for _, fn := range opts {
		fn(o)
	}
	s, err := server.NewServer(o)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(s.Shutdown)
	return s
}

// connect creates a NATS connection for testing.
func connect(t *testing.T, s *server.Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// newTransport creates a Transport for testing with an error collector.
func newTransport(t *testing.T, nc *nats.Conn, errs *[]error, mu *sync.Mutex) natstransport.Transport {
	t.Helper()
	tr, err := natstransport.New(nc, natstransport.WithErrorHandler(func(e error) {
		mu.Lock()
		*errs = append(*errs, e)
		mu.Unlock()
	}))
	if err != nil {
		t.Fatalf("new transport: %v", err)
	}
	return tr
}

func sampleObservations() []core.StageObservation {
	return []core.StageObservation{
		{
			Stage:        "parse",
			BusyWork:     100,
			CapacityWork: 200,
			Arrivals:     10,
			Workers:      2,
			Mask:         core.HasIdle | core.HasCompleted,
			IdleWork:     50,
			Completions:  8,
		},
	}
}

func sampleBatch() toc.ObservationBatch {
	return toc.ObservationBatch{
		PipelineID:         "pipeline-a",
		TimestampUnixNano:  1000000000,
		WindowDurationNano: 500000000,
		Observations:       sampleObservations(),
	}
}

func sampleDiagnosis() toc.DiagnosisMessage {
	return toc.DiagnosisMessage{
		PipelineID:        "pipeline-a",
		TimestampUnixNano: 2000000000,
		Diagnosis: core.Diagnosis{
			Constraint: "parse",
			SupportFreshness: 0.95,
			Stages: []core.StageDiagnosis{
				{
					Stage:       "parse",
					State:       core.StateSaturated,
					Utilization: 0.85,
					ErrorRate:   0.01,
					Completions: 8,
					Arrivals:    10,
				},
			},
		},
	}
}

// --- Round-trip tests ---

func TestObservationRoundTrip(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	var received toc.ObservationBatch
	done := make(chan struct{})
	sub, err := tr.SubscribeObservations(context.Background(), "pipeline-a",
		func(ctx context.Context, batch toc.ObservationBatch) error {
			received = batch
			close(done)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	batch := sampleBatch()
	if err := tr.PublishObservations(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for observation")
	}

	if received.PipelineID != batch.PipelineID {
		t.Errorf("pipeline: got %q want %q", received.PipelineID, batch.PipelineID)
	}
	if received.TimestampUnixNano != batch.TimestampUnixNano {
		t.Errorf("timestamp: got %d want %d", received.TimestampUnixNano, batch.TimestampUnixNano)
	}
	if received.WindowDurationNano != batch.WindowDurationNano {
		t.Errorf("window: got %d want %d", received.WindowDurationNano, batch.WindowDurationNano)
	}
	if len(received.Observations) != len(batch.Observations) {
		t.Fatalf("observations: got %d want %d", len(received.Observations), len(batch.Observations))
	}
	obs := received.Observations[0]
	if obs.Stage != "parse" || obs.BusyWork != 100 || obs.Completions != 8 {
		t.Errorf("observation mismatch: %+v", obs)
	}

	mu.Lock()
	if len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	mu.Unlock()
}

func TestDiagnosisRoundTrip(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	var received toc.DiagnosisMessage
	done := make(chan struct{})
	sub, err := tr.SubscribeDiagnosis(context.Background(), "pipeline-a",
		func(ctx context.Context, msg toc.DiagnosisMessage) error {
			received = msg
			close(done)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	diagMsg := sampleDiagnosis()
	if err := tr.PublishDiagnosis(context.Background(), diagMsg); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for diagnosis")
	}

	if received.PipelineID != diagMsg.PipelineID {
		t.Errorf("pipeline: got %q want %q", received.PipelineID, diagMsg.PipelineID)
	}
	if received.TimestampUnixNano != diagMsg.TimestampUnixNano {
		t.Errorf("timestamp: got %d want %d", received.TimestampUnixNano, diagMsg.TimestampUnixNano)
	}
	if received.Diagnosis.Constraint != "parse" {
		t.Errorf("constraint: got %q want %q", received.Diagnosis.Constraint, "parse")
	}
	if received.Diagnosis.SupportFreshness != 0.95 {
		t.Errorf("confidence: got %f want %f", received.Diagnosis.SupportFreshness, 0.95)
	}

	mu.Lock()
	if len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	mu.Unlock()
}

// --- Filtering tests ---

func TestSubscribeByPipeline(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	var count atomic.Int32
	sub, err := tr.SubscribeObservations(context.Background(), "target",
		func(ctx context.Context, batch toc.ObservationBatch) error {
			count.Add(1)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// Publish to target and other pipeline.
	batch := sampleBatch()
	batch.PipelineID = "target"
	tr.PublishObservations(context.Background(), batch)

	batch.PipelineID = "other"
	tr.PublishObservations(context.Background(), batch)

	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	if got := count.Load(); got != 1 {
		t.Errorf("got %d messages, want 1", got)
	}
}

func TestSubscribeAll(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	var count atomic.Int32
	sub, err := tr.SubscribeAllObservations(context.Background(),
		func(ctx context.Context, batch toc.ObservationBatch) error {
			count.Add(1)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	batch := sampleBatch()
	batch.PipelineID = "alpha"
	tr.PublishObservations(context.Background(), batch)

	batch.PipelineID = "beta"
	tr.PublishObservations(context.Background(), batch)

	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	if got := count.Load(); got != 2 {
		t.Errorf("got %d messages, want 2", got)
	}
}

// --- Validation tests ---

func TestInvalidPipelineIDPublish(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	tr, _ := natstransport.New(nc)

	for _, id := range []string{"", "a.b", "a*", "a>", "a b"} {
		batch := sampleBatch()
		batch.PipelineID = id
		if err := tr.PublishObservations(context.Background(), batch); err == nil {
			t.Errorf("expected error for pipelineID %q", id)
		}
	}
}

func TestInvalidPipelineIDSubscribe(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	tr, _ := natstransport.New(nc)

	for _, id := range []string{"", "a.b", "a*", "a>", "a b"} {
		_, err := tr.SubscribeObservations(context.Background(), id,
			func(ctx context.Context, batch toc.ObservationBatch) error { return nil })
		if err == nil {
			t.Errorf("expected error for pipelineID %q", id)
		}
	}
}

func TestNilConnRejected(t *testing.T) {
	_, err := natstransport.New(nil)
	if err == nil {
		t.Error("expected error for nil conn")
	}
}

func TestInvalidPrefixRejected(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)

	for _, prefix := range []string{"", ".toc", "toc.", "toc..dev", "toc*", "toc>"} {
		_, err := natstransport.New(nc, natstransport.WithPrefix(prefix))
		if err == nil {
			t.Errorf("expected error for prefix %q", prefix)
		}
	}
}

func TestNilHandlerRejected(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	tr, _ := natstransport.New(nc)

	_, err := tr.SubscribeObservations(context.Background(), "p", nil)
	if err == nil {
		t.Error("expected error for nil handler")
	}
	_, err = tr.SubscribeDiagnosis(context.Background(), "p", nil)
	if err == nil {
		t.Error("expected error for nil handler")
	}
}

// --- Error routing tests ---

func TestHandlerError(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	done := make(chan struct{})
	sub, _ := tr.SubscribeObservations(context.Background(), "p",
		func(ctx context.Context, batch toc.ObservationBatch) error {
			defer close(done)
			return errors.New("handler failed")
		})
	defer sub.Close()

	batch := sampleBatch()
	batch.PipelineID = "p"
	tr.PublishObservations(context.Background(), batch)

	<-done
	time.Sleep(50 * time.Millisecond) // let onError fire

	mu.Lock()
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "handler failed") {
		t.Errorf("error %q does not mention handler", errs[0])
	}
	mu.Unlock()
}

func TestHandlerPanic(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	done := make(chan struct{})
	var callCount atomic.Int32
	sub, _ := tr.SubscribeObservations(context.Background(), "p",
		func(ctx context.Context, batch toc.ObservationBatch) error {
			if callCount.Add(1) == 1 {
				defer close(done)
				panic("boom")
			}
			return nil
		})
	defer sub.Close()

	batch := sampleBatch()
	batch.PipelineID = "p"
	tr.PublishObservations(context.Background(), batch)

	<-done
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "handler panic: boom") {
		t.Errorf("error %q does not mention panic", errs[0])
	}
	mu.Unlock()

	// Subscription should continue after panic — send another message.
	batch.PipelineID = "p"
	tr.PublishObservations(context.Background(), batch)
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	if got := callCount.Load(); got < 2 {
		t.Errorf("subscription stopped after panic: got %d calls", got)
	}
}

func TestMalformedProto(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	sub, _ := tr.SubscribeObservations(context.Background(), "p",
		func(ctx context.Context, batch toc.ObservationBatch) error { return nil })
	defer sub.Close()

	// Publish garbage directly.
	nc.Publish("toc.observe.p", []byte("not-proto"))
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(errs) == 0 {
		t.Error("expected unmarshal error")
	}
	mu.Unlock()
}

func TestSubjectPayloadMismatch(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	sub, _ := tr.SubscribeObservations(context.Background(), "target",
		func(ctx context.Context, batch toc.ObservationBatch) error { return nil })
	defer sub.Close()

	// Publish a valid proto with pipelineID="wrong" to subject "toc.observe.target".
	pb := tocpb.BatchToProto("wrong", 1000, 500, sampleObservations())
	data, _ := proto.Marshal(pb)
	nc.Publish("toc.observe.target", data)
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(errs) == 0 {
		t.Error("expected mismatch error")
	}
	if len(errs) > 0 && !strings.Contains(errs[0].Error(), "mismatch") {
		t.Errorf("error %q does not mention mismatch", errs[0])
	}
	mu.Unlock()
}

// --- Lifecycle tests ---

func TestCloseIdempotent(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	tr, _ := natstransport.New(nc)

	sub, _ := tr.SubscribeObservations(context.Background(), "p",
		func(ctx context.Context, batch toc.ObservationBatch) error { return nil })

	if err := sub.Close(); err != nil {
		t.Errorf("first close: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Errorf("second close: %v", err)
	}
}

func TestCloseWaitsForHandler(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	tr, _ := natstransport.New(nc)

	handlerStarted := make(chan struct{})
	handlerDone := make(chan struct{})

	sub, _ := tr.SubscribeObservations(context.Background(), "p",
		func(ctx context.Context, batch toc.ObservationBatch) error {
			close(handlerStarted)
			<-handlerDone
			return nil
		})

	batch := sampleBatch()
	batch.PipelineID = "p"
	tr.PublishObservations(context.Background(), batch)

	<-handlerStarted

	// Close should block until handler finishes.
	closeDone := make(chan struct{})
	go func() {
		sub.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		t.Error("Close returned before handler finished")
	case <-time.After(100 * time.Millisecond):
		// expected — Close is blocking
	}

	close(handlerDone)

	select {
	case <-closeDone:
		// expected — Close returned after handler
	case <-time.After(5 * time.Second):
		t.Error("Close did not return after handler finished")
	}
}

func TestParentCtxCancellation(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	tr, _ := natstransport.New(nc)

	ctx, cancel := context.WithCancel(context.Background())
	var count atomic.Int32

	sub, _ := tr.SubscribeObservations(ctx, "p",
		func(ctx context.Context, batch toc.ObservationBatch) error {
			count.Add(1)
			return nil
		})
	_ = sub // keep alive

	batch := sampleBatch()
	batch.PipelineID = "p"
	tr.PublishObservations(context.Background(), batch)
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	cancel()
	time.Sleep(100 * time.Millisecond)

	// After cancellation, no more messages should be delivered.
	before := count.Load()
	tr.PublishObservations(context.Background(), batch)
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	if got := count.Load(); got != before {
		t.Errorf("received message after ctx cancel: before=%d after=%d", before, got)
	}
}

func TestHandlerReceivesCanceledCtx(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	tr, _ := natstransport.New(nc)

	handlerStarted := make(chan struct{})
	ctxCanceled := make(chan struct{})

	sub, _ := tr.SubscribeObservations(context.Background(), "p",
		func(ctx context.Context, batch toc.ObservationBatch) error {
			close(handlerStarted)
			<-ctx.Done()
			close(ctxCanceled)
			return nil
		})

	batch := sampleBatch()
	batch.PipelineID = "p"
	tr.PublishObservations(context.Background(), batch)

	<-handlerStarted
	sub.Close()

	select {
	case <-ctxCanceled:
		// expected
	case <-time.After(5 * time.Second):
		t.Error("handler ctx was not canceled on Close")
	}
}

func TestNoDeliveryAfterClose(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	tr, _ := natstransport.New(nc)

	var count atomic.Int32
	sub, _ := tr.SubscribeObservations(context.Background(), "p",
		func(ctx context.Context, batch toc.ObservationBatch) error {
			count.Add(1)
			return nil
		})

	sub.Close()

	batch := sampleBatch()
	batch.PipelineID = "p"
	tr.PublishObservations(context.Background(), batch)
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	if got := count.Load(); got != 0 {
		t.Errorf("received %d messages after Close", got)
	}
}

// --- NATS edge cases ---

func TestPublishAfterConnClose(t *testing.T) {
	s := startServer(t)
	nc, _ := nats.Connect(s.ClientURL())
	tr, _ := natstransport.New(nc)
	nc.Close()

	if err := tr.PublishObservations(context.Background(), sampleBatch()); err == nil {
		t.Error("expected error publishing after conn close")
	}
}

func TestSubscribeAfterConnClose(t *testing.T) {
	s := startServer(t)
	nc, _ := nats.Connect(s.ClientURL())
	tr, _ := natstransport.New(nc)
	nc.Close()

	_, err := tr.SubscribeObservations(context.Background(), "p",
		func(ctx context.Context, batch toc.ObservationBatch) error { return nil })
	if err == nil {
		t.Error("expected error subscribing after conn close")
	}
}

func TestOversizedPayload(t *testing.T) {
	s := startServer(t, func(o *server.Options) {
		o.MaxPayload = 128 // very small
	})
	nc := connect(t, s)
	tr, _ := natstransport.New(nc)

	// Create a batch with enough data to exceed 128 bytes.
	batch := sampleBatch()
	for i := 0; i < 20; i++ {
		batch.Observations = append(batch.Observations, core.StageObservation{
			Stage:        fmt.Sprintf("stage-%d", i),
			BusyWork:     1000,
			CapacityWork: 2000,
			Arrivals:     100,
			Workers:      4,
		})
	}

	err := tr.PublishObservations(context.Background(), batch)
	if err == nil {
		t.Error("expected error for oversized payload")
	}
}

func TestHandlerSeriality(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	tr, _ := natstransport.New(nc)

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32
	var wg sync.WaitGroup

	wg.Add(5)
	sub, _ := tr.SubscribeObservations(context.Background(), "p",
		func(ctx context.Context, batch toc.ObservationBatch) error {
			c := concurrent.Add(1)
			for {
				old := maxConcurrent.Load()
				if c <= old || maxConcurrent.CompareAndSwap(old, c) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			concurrent.Add(-1)
			wg.Done()
			return nil
		})
	defer sub.Close()

	batch := sampleBatch()
	batch.PipelineID = "p"
	for i := 0; i < 5; i++ {
		tr.PublishObservations(context.Background(), batch)
	}

	wg.Wait()

	if got := maxConcurrent.Load(); got > 1 {
		t.Errorf("handlers ran concurrently: max=%d", got)
	}
}

// --- Subject/config tests ---

func TestCustomPrefix(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex

	tr, err := natstransport.New(nc,
		natstransport.WithPrefix("custom.ns"),
		natstransport.WithErrorHandler(func(e error) {
			mu.Lock()
			errs = append(errs, e)
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	sub, _ := tr.SubscribeObservations(context.Background(), "p",
		func(ctx context.Context, batch toc.ObservationBatch) error {
			close(done)
			return nil
		})
	defer sub.Close()

	batch := sampleBatch()
	batch.PipelineID = "p"
	tr.PublishObservations(context.Background(), batch)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout — custom prefix may not be wired correctly")
	}
}

func TestDefaultPrefix(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	tr, _ := natstransport.New(nc)

	// Subscribe to raw NATS subject to verify prefix.
	received := make(chan string, 1)
	nc.Subscribe("toc.observe.p", func(msg *nats.Msg) {
		received <- msg.Subject
	})

	batch := sampleBatch()
	batch.PipelineID = "p"
	tr.PublishObservations(context.Background(), batch)
	nc.Flush()

	select {
	case subj := <-received:
		if subj != "toc.observe.p" {
			t.Errorf("subject: got %q want %q", subj, "toc.observe.p")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

// --- Diagnosis header tests ---

func TestMissingTimestampHeader(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	sub, _ := tr.SubscribeDiagnosis(context.Background(), "p",
		func(ctx context.Context, msg toc.DiagnosisMessage) error { return nil })
	defer sub.Close()

	// Publish diagnosis proto directly without header.
	diag := sampleDiagnosis()
	pb := tocpb.DiagnosisToProto(diag.Diagnosis)
	data, _ := proto.Marshal(pb)
	nc.Publish("toc.diagnose.p", data)
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(errs) == 0 {
		t.Error("expected error for missing header")
	}
	if len(errs) > 0 && !strings.Contains(errs[0].Error(), "missing") {
		t.Errorf("error %q does not mention missing", errs[0])
	}
	mu.Unlock()
}

func TestEmptyTimestampHeader(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	sub, _ := tr.SubscribeDiagnosis(context.Background(), "p",
		func(ctx context.Context, msg toc.DiagnosisMessage) error { return nil })
	defer sub.Close()

	diag := sampleDiagnosis()
	pb := tocpb.DiagnosisToProto(diag.Diagnosis)
	data, _ := proto.Marshal(pb)
	natsMsg := &nats.Msg{Subject: "toc.diagnose.p", Data: data, Header: nats.Header{}}
	natsMsg.Header.Set(natstransport.HeaderTimestamp, "")
	nc.PublishMsg(natsMsg)
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(errs) == 0 {
		t.Error("expected error for empty header")
	}
	mu.Unlock()
}

func TestUnparseableTimestampHeader(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	sub, _ := tr.SubscribeDiagnosis(context.Background(), "p",
		func(ctx context.Context, msg toc.DiagnosisMessage) error { return nil })
	defer sub.Close()

	diag := sampleDiagnosis()
	pb := tocpb.DiagnosisToProto(diag.Diagnosis)
	data, _ := proto.Marshal(pb)
	natsMsg := &nats.Msg{Subject: "toc.diagnose.p", Data: data, Header: nats.Header{}}
	natsMsg.Header.Set(natstransport.HeaderTimestamp, "not-a-number")
	nc.PublishMsg(natsMsg)
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(errs) == 0 {
		t.Error("expected error for unparseable header")
	}
	mu.Unlock()
}

func TestNegativeTimestampHeader(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	sub, _ := tr.SubscribeDiagnosis(context.Background(), "p",
		func(ctx context.Context, msg toc.DiagnosisMessage) error { return nil })
	defer sub.Close()

	diag := sampleDiagnosis()
	pb := tocpb.DiagnosisToProto(diag.Diagnosis)
	data, _ := proto.Marshal(pb)
	natsMsg := &nats.Msg{Subject: "toc.diagnose.p", Data: data, Header: nats.Header{}}
	natsMsg.Header.Set(natstransport.HeaderTimestamp, "-1")
	nc.PublishMsg(natsMsg)
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(errs) == 0 {
		t.Error("expected error for negative header")
	}
	mu.Unlock()
}

func TestValidHeaderRoundTrip(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	var received int64
	done := make(chan struct{})
	sub, _ := tr.SubscribeDiagnosis(context.Background(), "p",
		func(ctx context.Context, msg toc.DiagnosisMessage) error {
			received = msg.TimestampUnixNano
			close(done)
			return nil
		})
	defer sub.Close()

	diagMsg := sampleDiagnosis()
	diagMsg.PipelineID = "p"
	diagMsg.TimestampUnixNano = 9999999999
	tr.PublishDiagnosis(context.Background(), diagMsg)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	if received != 9999999999 {
		t.Errorf("timestamp: got %d want %d", received, int64(9999999999))
	}

	mu.Lock()
	if len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	mu.Unlock()
}

// --- Conversion/contract tests ---

func TestDecodedBatchConversion(t *testing.T) {
	decoded := tocpb.DecodedBatch{
		PipelineID:         "test",
		TimestampUnixNano:  12345,
		WindowDurationNano: 678,
		Observations:       sampleObservations(),
	}

	// Use the transport's publish/subscribe path to verify conversion.
	// Instead, test the round-trip: encode as proto, decode, and check.
	pb := tocpb.BatchToProto(decoded.PipelineID, decoded.TimestampUnixNano, decoded.WindowDurationNano, decoded.Observations)
	data, err := proto.Marshal(pb)
	if err != nil {
		t.Fatal(err)
	}
	var pb2 tocpb.ObservationBatch
	if err := proto.Unmarshal(data, &pb2); err != nil {
		t.Fatal(err)
	}
	decoded2, err := tocpb.BatchFromProto(&pb2)
	if err != nil {
		t.Fatal(err)
	}

	if decoded2.PipelineID != decoded.PipelineID {
		t.Errorf("pipeline: got %q want %q", decoded2.PipelineID, decoded.PipelineID)
	}
	if decoded2.TimestampUnixNano != decoded.TimestampUnixNano {
		t.Errorf("timestamp: got %d want %d", decoded2.TimestampUnixNano, decoded.TimestampUnixNano)
	}
}

func TestPanicRecoveryIncludesStack(t *testing.T) {
	s := startServer(t)
	nc := connect(t, s)
	var errs []error
	var mu sync.Mutex
	tr := newTransport(t, nc, &errs, &mu)

	done := make(chan struct{})
	sub, _ := tr.SubscribeObservations(context.Background(), "p",
		func(ctx context.Context, batch toc.ObservationBatch) error {
			defer close(done)
			panic("stack-test")
		})
	defer sub.Close()

	batch := sampleBatch()
	batch.PipelineID = "p"
	tr.PublishObservations(context.Background(), batch)

	<-done
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(errs) == 0 {
		t.Fatal("expected panic error")
	}
	errStr := errs[0].Error()
	if !strings.Contains(errStr, "goroutine") {
		t.Errorf("panic error should contain stack trace, got: %s", errStr[:min(200, len(errStr))])
	}
}

// suppress unused import
var _ = strconv.Itoa
