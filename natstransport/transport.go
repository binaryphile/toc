package natstransport

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/binaryphile/toc"
	"github.com/binaryphile/toc/tocpb"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// HeaderTimestamp is the NATS header key for diagnosis timestamp.
// Diagnosis proto lacks envelope fields — this header carries
// timestamp_unix_nano as a transport-specific shim.
const HeaderTimestamp = "Toc-Timestamp-Unix-Nano"

// Transport implements [toc.Publisher] and [toc.Subscriber] over core NATS.
//
// Transport is an immutable, copyable handle. Copies share the
// underlying [nats.Conn]. The caller owns connection lifecycle.
type Transport struct {
	conn    *nats.Conn
	prefix  string
	onError func(error)
}

// config holds constructor parameters before validation.
type config struct {
	prefix  string
	onError func(error)
}

// Option configures a [Transport].
type Option func(*config) error

// WithPrefix sets the NATS subject prefix. Default is "toc".
// Multi-token prefixes (e.g. "toc.dev") are allowed.
func WithPrefix(prefix string) Option {
	return func(c *config) error {
		if err := validatePrefix(prefix); err != nil {
			return err
		}
		c.prefix = prefix
		return nil
	}
}

// WithErrorHandler sets the callback for subscriber-side failures.
// The handler may be called concurrently across subscriptions. It is
// called synchronously from the callback path and must return quickly.
// It must not panic and must not call [toc.Subscription.Close]
// synchronously.
//
// Default: discard all errors.
func WithErrorHandler(fn func(error)) Option {
	return func(c *config) error {
		c.onError = fn
		return nil
	}
}

// New creates a Transport. The caller owns the conn lifecycle.
func New(conn *nats.Conn, opts ...Option) (Transport, error) {
	if conn == nil {
		return Transport{}, errors.New("natstransport: conn must not be nil")
	}
	cfg := config{
		prefix:  defaultPrefix,
		onError: func(error) {}, // discard
	}
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return Transport{}, fmt.Errorf("natstransport: %w", err)
		}
	}
	return Transport{
		conn:    conn,
		prefix:  cfg.prefix,
		onError: cfg.onError,
	}, nil
}

// PublishObservations implements [toc.ObservationPublisher].
func (t Transport) PublishObservations(ctx context.Context, batch toc.ObservationBatch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidatePipelineID(batch.PipelineID); err != nil {
		return fmt.Errorf("natstransport: %w", err)
	}

	pb := tocpb.BatchToProto(
		batch.PipelineID,
		batch.TimestampUnixNano,
		batch.WindowDurationNano,
		batch.Observations,
	)
	data, err := proto.Marshal(pb)
	if err != nil {
		return fmt.Errorf("natstransport: marshal: %w", err)
	}

	if err := t.checkPayloadSize(len(data), 0); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	subject := observationSubject(t.prefix, batch.PipelineID)
	if err := t.conn.Publish(subject, data); err != nil {
		return fmt.Errorf("natstransport: publish: %w", err)
	}
	return nil
}

// PublishDiagnosis implements [toc.DiagnosisPublisher].
func (t Transport) PublishDiagnosis(ctx context.Context, msg toc.DiagnosisMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidatePipelineID(msg.PipelineID); err != nil {
		return fmt.Errorf("natstransport: %w", err)
	}

	pb := tocpb.DiagnosisToProto(msg.Diagnosis)
	data, err := proto.Marshal(pb)
	if err != nil {
		return fmt.Errorf("natstransport: marshal: %w", err)
	}

	// Estimate header overhead for size check.
	headerOverhead := len(HeaderTimestamp) + 20 + 16 // key + int64 digits + framing
	if err := t.checkPayloadSize(len(data), headerOverhead); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	subject := diagnosisSubject(t.prefix, msg.PipelineID)
	natsMsg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	natsMsg.Header.Set(HeaderTimestamp, strconv.FormatInt(msg.TimestampUnixNano, 10))

	if err := t.conn.PublishMsg(natsMsg); err != nil {
		return fmt.Errorf("natstransport: publish: %w", err)
	}
	return nil
}

// checkPayloadSize returns an error if the payload plus estimated
// overhead exceeds the server-negotiated max. Non-positive max means
// unknown — skip the check and let NATS decide.
func (t Transport) checkPayloadSize(payloadLen, headerOverhead int) error {
	maxPayload := t.conn.MaxPayload()
	if maxPayload <= 0 {
		return nil
	}
	total := int64(payloadLen + headerOverhead)
	if total > maxPayload {
		return fmt.Errorf("natstransport: payload size %d exceeds max %d", total, maxPayload)
	}
	return nil
}

// SubscribeObservations implements [toc.ObservationSubscriber].
func (t Transport) SubscribeObservations(ctx context.Context, pipelineID string, fn toc.ObservationHandler) (toc.Subscription, error) {
	if fn == nil {
		return nil, errors.New("natstransport: handler must not be nil")
	}
	if err := ValidatePipelineID(pipelineID); err != nil {
		return nil, fmt.Errorf("natstransport: %w", err)
	}
	subject := observationSubject(t.prefix, pipelineID)
	return t.subscribeObservation(ctx, subject, fn)
}

// SubscribeAllObservations implements [toc.ObservationSubscriber].
func (t Transport) SubscribeAllObservations(ctx context.Context, fn toc.ObservationHandler) (toc.Subscription, error) {
	if fn == nil {
		return nil, errors.New("natstransport: handler must not be nil")
	}
	subject := allObservationSubject(t.prefix)
	return t.subscribeObservation(ctx, subject, fn)
}

func (t Transport) subscribeObservation(ctx context.Context, subject string, fn toc.ObservationHandler) (toc.Subscription, error) {
	subCtx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	natsSub, err := t.conn.Subscribe(subject, func(msg *nats.Msg) {
		if !sub.enterCallback() {
			return
		}
		defer sub.exitCallback()
		defer recoverPanic(t.onError)

		var pb tocpb.ObservationBatch
		if err := proto.Unmarshal(msg.Data, &pb); err != nil {
			safeOnError(t.onError, fmt.Errorf("natstransport: unmarshal observation: %w", err))
			return
		}
		decoded, err := tocpb.BatchFromProto(&pb)
		if err != nil {
			safeOnError(t.onError, fmt.Errorf("natstransport: decode observation: %w", err))
			return
		}

		// Subject/payload consistency check.
		subjectPipeline := pipelineFromSubject(msg.Subject)
		if decoded.PipelineID != subjectPipeline {
			safeOnError(t.onError, fmt.Errorf(
				"natstransport: subject/payload pipeline mismatch: subject=%q payload=%q",
				subjectPipeline, decoded.PipelineID,
			))
			return
		}

		batch := observationBatchFromDecoded(decoded)
		if err := fn(subCtx, batch); err != nil {
			safeOnError(t.onError, fmt.Errorf("natstransport: observation handler: %w", err))
		}
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("natstransport: subscribe: %w", err)
	}

	sub.natsSub = natsSub

	// Watch parent context.
	go func() {
		select {
		case <-subCtx.Done():
			sub.Close()
		case <-sub.done:
		}
	}()

	return sub, nil
}

// SubscribeDiagnosis implements [toc.DiagnosisSubscriber].
func (t Transport) SubscribeDiagnosis(ctx context.Context, pipelineID string, fn toc.DiagnosisHandler) (toc.Subscription, error) {
	if fn == nil {
		return nil, errors.New("natstransport: handler must not be nil")
	}
	if err := ValidatePipelineID(pipelineID); err != nil {
		return nil, fmt.Errorf("natstransport: %w", err)
	}
	subject := diagnosisSubject(t.prefix, pipelineID)
	return t.subscribeDiagnosis(ctx, subject, fn)
}

// SubscribeAllDiagnosis implements [toc.DiagnosisSubscriber].
func (t Transport) SubscribeAllDiagnosis(ctx context.Context, fn toc.DiagnosisHandler) (toc.Subscription, error) {
	if fn == nil {
		return nil, errors.New("natstransport: handler must not be nil")
	}
	subject := allDiagnosisSubject(t.prefix)
	return t.subscribeDiagnosis(ctx, subject, fn)
}

func (t Transport) subscribeDiagnosis(ctx context.Context, subject string, fn toc.DiagnosisHandler) (toc.Subscription, error) {
	subCtx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	natsSub, err := t.conn.Subscribe(subject, func(msg *nats.Msg) {
		if !sub.enterCallback() {
			return
		}
		defer sub.exitCallback()
		defer recoverPanic(t.onError)

		var pb tocpb.Diagnosis
		if err := proto.Unmarshal(msg.Data, &pb); err != nil {
			safeOnError(t.onError, fmt.Errorf("natstransport: unmarshal diagnosis: %w", err))
			return
		}
		diag, err := tocpb.DiagnosisFromProto(&pb)
		if err != nil {
			safeOnError(t.onError, fmt.Errorf("natstransport: decode diagnosis: %w", err))
			return
		}

		// Extract envelope from subject and headers.
		pipelineID := pipelineFromSubject(msg.Subject)
		ts, err := parseTimestampHeader(msg)
		if err != nil {
			safeOnError(t.onError, fmt.Errorf("natstransport: diagnosis header: %w", err))
			return
		}

		diagMsg := toc.DiagnosisMessage{
			PipelineID:        pipelineID,
			TimestampUnixNano: ts,
			Diagnosis:         diag,
		}
		if err := fn(subCtx, diagMsg); err != nil {
			safeOnError(t.onError, fmt.Errorf("natstransport: diagnosis handler: %w", err))
		}
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("natstransport: subscribe: %w", err)
	}

	sub.natsSub = natsSub

	// Watch parent context.
	go func() {
		select {
		case <-subCtx.Done():
			sub.Close()
		case <-sub.done:
		}
	}()

	return sub, nil
}

// parseTimestampHeader extracts and validates the diagnosis timestamp
// from NATS message headers. Returns error for missing, empty,
// unparseable, negative, or duplicate values.
func parseTimestampHeader(msg *nats.Msg) (int64, error) {
	values := msg.Header.Values(HeaderTimestamp)
	if len(values) == 0 {
		return 0, fmt.Errorf("missing %s header", HeaderTimestamp)
	}
	if len(values) > 1 {
		return 0, fmt.Errorf("duplicate %s header", HeaderTimestamp)
	}
	raw := strings.TrimSpace(values[0])
	if raw == "" {
		return 0, fmt.Errorf("empty %s header", HeaderTimestamp)
	}
	ts, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unparseable %s header %q: %w", HeaderTimestamp, raw, err)
	}
	if ts < 0 {
		return 0, fmt.Errorf("negative %s header: %d", HeaderTimestamp, ts)
	}
	return ts, nil
}

// observationBatchFromDecoded converts tocpb.DecodedBatch to toc.ObservationBatch.
func observationBatchFromDecoded(d tocpb.DecodedBatch) toc.ObservationBatch {
	return toc.ObservationBatch{
		PipelineID:         d.PipelineID,
		TimestampUnixNano:  d.TimestampUnixNano,
		WindowDurationNano: d.WindowDurationNano,
		Observations:       d.Observations,
	}
}

// Compile-time interface satisfaction checks.
var (
	_ toc.Publisher  = Transport{}
	_ toc.Subscriber = Transport{}
)
