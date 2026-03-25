// Package natstransport implements [toc.Publisher] and [toc.Subscriber]
// over core NATS (not JetStream).
//
// # Delivery semantics
//
// This is an at-most-once transport. A nil publish error means the NATS
// client accepted the message for transmission — it is NOT an
// acknowledgment of server receipt. Messages may be lost during
// disconnect/reconnect windows. For durable delivery, use JetStream
// (requires different interfaces).
//
// # Error handling
//
// All subscriber-side failures — handler errors, unmarshal errors,
// validation errors, and recovered panics — are routed to the error
// handler configured via [WithErrorHandler]. The default error handler
// discards all errors silently. Callers who need observability MUST
// configure an error handler.
//
// # Handler contract
//
// Handlers are invoked serially per subscription. Handlers MUST NOT
// call [toc.Subscription.Close] synchronously — doing so deadlocks
// because the handler holds the active callback count that Close waits
// to reach zero. Handlers should return promptly; a handler that blocks
// indefinitely prevents [toc.Subscription.Close] from completing.
//
// # Diagnosis wire format
//
// The Diagnosis protobuf message lacks pipeline_id and
// timestamp_unix_nano envelope fields (unlike ObservationBatch which
// has them). This transport uses NATS message headers as a shim:
//   - Pipeline ID: extracted from the NATS subject
//   - Timestamp: [HeaderTimestamp] NATS header (base-10 int64, >= 0)
//
// This is a transport-specific compatibility measure. If diagnosis
// needs a transport-agnostic wire contract, add a proto envelope.
//
// # Connection ownership
//
// The caller owns the [nats.Conn] lifecycle. [Transport] is an
// immutable, copyable handle — copies share the underlying connection.
package natstransport
