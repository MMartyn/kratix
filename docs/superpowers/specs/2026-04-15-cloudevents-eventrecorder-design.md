# CloudEvents – Best-Effort Async Emission

## Status

Proposed and validated in brainstorming; pending implementation plan.

## Context

Kratix controllers already use Kubernetes `record.EventRecorder` extensively. We want to add CloudEvents emission in a way that is easy to review, self-contained, and low-risk to reconcile behavior.

The initial explored option was durable at-least-once delivery using an outbox CRD. After trade-off discussion, the selected direction is best-effort delivery with async retries to keep the system simple and avoid additional etcd object lifecycle management.

An earlier iteration proposed wrapping `record.EventRecorder` with a decorator that intercepted kube event calls and inferred operation types (create, edit, delete, status-change) from reason/message strings via a classifier. This was deprioritized because:

- String-based classification is fragile and must track every reason string across all controllers.
- Adding new operations or changing event messages could silently break classification.
- The call site already knows what operation is being performed; that intent should be expressed directly rather than reverse-engineered from free-form text.

The selected approach uses an explicit, typed eventing API that controllers call alongside (not instead of) the existing kube event recorder. This makes the operation unambiguous at the call site and avoids any classifier.

## SDK Dependency

CloudEvents construction and HTTP protocol binding will use the **CloudEvents Go SDK** (`github.com/cloudevents/sdk-go/v2`). This gives us:

- Spec-compliant event construction (`cloudevents.NewEvent()`).
- HTTP protocol binding with built-in retry/backoff via `cloudevents/sdk-go/v2/protocol/http` and `cloudevents/sdk-go/v2/context`.
- Content-type negotiation (structured vs. binary mode).
- Broad community adoption and maintained compatibility with the CloudEvents spec.

We pin to the `v2` module path and track the latest stable release.

## Goals

- Emit CloudEvents for key operations (`create`, `edit`, `delete`, `status-change`).
- Include status payload for `status-change` events.
- Keep reconciler behavior unchanged and non-blocking.
- Preserve current Kubernetes event emission (unchanged).
- Keep changes self-contained and easy to review.
- Express operation intent explicitly at the call site — no string-based inference.

## Non-Goals

- Durable at-least-once guarantees across process restarts.
- Outbox CRD, dead-letter queues, or replay pipelines.
- Exactly-once delivery or strict in-order delivery.
- Wrapping or replacing the Kubernetes `record.EventRecorder` interface.

## Selected Architecture

### 1) Explicit Eventing API

Add a `CloudEventEmitter` with typed methods for each operation:

```go
type Operation string

const (
    OperationCreate       Operation = "create"
    OperationEdit         Operation = "edit"
    OperationDelete       Operation = "delete"
    OperationStatusChange Operation = "status-change"
)

type CloudEventEmitter interface {
    Emit(obj client.Object, op Operation, opts ...EmitOption)
}
```

`EmitOption` allows attaching optional data without polluting the core signature:

```go
type EmitOption func(*emitConfig)

func WithStatus(status any) EmitOption { ... }
func WithMessage(msg string) EmitOption { ... }
```

Controllers call `Emit` directly at the point where the operation is known:

```go
r.CloudEvents.Emit(promise, eventing.OperationCreate)
r.CloudEvents.Emit(rr, eventing.OperationStatusChange, eventing.WithStatus(rr.Status))
```

This means:
- The operation is a typed constant, not a string to be classified.
- Kubernetes event recording continues unchanged via the existing `record.EventRecorder`.
- CloudEvent emission is an additive call, not a side-effect of kube event recording.

### 2) Async Publisher with Retries

Add shared `AsyncPublisher`:

- In-memory buffered channel (`queueSize` capacity).
- `workerCount` background goroutines drain the channel.
- Each worker creates a send context with `cecontext.WithRetryParams()` and calls `ceClient.Send(ctx, event)`. The SDK's HTTP protocol handles the retry loop (`doWithRetry`) and backoff internally based on the `RetryParams` in the context.
- On queue full: drop event, log, increment metrics (best effort).
- On SDK retries exhausted (send returns non-ACK `RetriesResult`): drop event, log, increment metrics.

### 3) No Sink Means No-Op

If global sink is not configured:

- Use `NoopEmitter` (implements `CloudEventEmitter` as no-ops).
- Keep Kubernetes event recording normal.
- Emit one startup log indicating CloudEvents are disabled.
- No periodic reminders.

### 4) Global Configuration

Configuration is split into two groups: CloudEvents SDK settings (mapped directly to SDK types) and our own async publisher settings.

#### SDK-mapped settings

These map 1:1 to the CloudEvents Go SDK's `RetryParams` struct and HTTP protocol options:

- `eventing.cloudEvents.sink` — Target URL passed to `cehttp.WithTarget()`. Required to enable emission.
- `eventing.cloudEvents.source` — CloudEvent `source` attribute (e.g. `"https://kratix.io/controller-manager"`).
- `eventing.cloudEvents.retry.maxTries` — Maps to `RetryParams.MaxTries`. Maximum number of retry attempts before giving up (default: `3`).
- `eventing.cloudEvents.retry.strategy` — Maps to `RetryParams.Strategy`. One of `"none"`, `"constant"`, `"linear"`, `"exponential"` (default: `"constant"`). These correspond directly to the SDK's `BackoffStrategy` constants.
- `eventing.cloudEvents.retry.period` — Maps to `RetryParams.Period`. Base delay duration between retries (default: `"1s"`). Interpretation depends on strategy:
  - `constant`: fixed delay between retries
  - `linear`: delay = period × retry count
  - `exponential`: delay = period × 2^(retry count)

The retry parameters are applied via `cecontext.WithRetryParams()` on the context passed to `client.Send()`. The SDK's HTTP protocol handles the retry loop internally in its `doWithRetry` path.

The SDK's default retriable HTTP status codes (404, 413, 425, 429, 502, 503, 504) are used as-is initially. A custom `IsRetriableFunc` can be added later if needed.

#### Publisher settings (our own)

These control the in-memory async queue that sits between the `Emit` call and the SDK `client.Send`:

- `eventing.cloudEvents.queueSize` — Buffered channel capacity for pending events (default: `1024`).
- `eventing.cloudEvents.workerCount` — Number of background goroutines draining the queue and calling `client.Send` (default: `2`).

Defaults should be conservative and safe for controller-manager memory/CPU.

## Event Model

### CloudEvent Type

Type format:

`io.kratix.<resource>.<operation>.v1`

Examples:

- `io.kratix.work.create.v1`
- `io.kratix.promise.edit.v1`
- `io.kratix.workplacement.status-change.v1`
- `io.kratix.resourcerequest.delete.v1`

### Core CloudEvent Attributes

- `id`: generated UUID
- `source`: configured global source
- `type`: taxonomy above
- `subject`: `<kind>/<namespace>/<name>`
- `time`: emission timestamp
- `datacontenttype`: `application/json`

### Data Payload

Common shape:

- `operation` — the typed operation constant (e.g. `"create"`, `"status-change"`)
- `message` — optional human-readable context (via `WithMessage`)
- `resource`:
  - `apiVersion`
  - `kind`
  - `namespace`
  - `name`
  - `uid`
  - `resourceVersion`
- `status` (only for `status-change`, via `WithStatus`)

For `status-change`, include full status payload initially for correctness and downstream flexibility.

## Operation Specification

Operations are explicit typed constants (`OperationCreate`, `OperationEdit`, `OperationDelete`, `OperationStatusChange`) passed by the controller at the call site. There is no classifier or string matching — the developer adding the `Emit` call selects the correct operation when writing the code.

This means:
- New operations are added by defining a new `Operation` constant and calling `Emit` with it.
- No mapping table or pattern list needs to be maintained.
- The set of emitted operations is discoverable by searching for `Emit(` calls in the codebase.

## Data Flow

1. Controller performs an operation and calls `r.EventRecorder.Event*(...)` as before (unchanged).
2. Controller calls `r.CloudEvents.Emit(obj, operation, ...)` at the same point.
3. Emitter builds a CloudEvent envelope from the object metadata, operation constant, and any options.
4. Envelope is enqueued to the async publisher's in-memory queue.
5. Worker sends via `cloudevents/sdk-go/v2` HTTP protocol binding with retry/backoff.
6. Success/failure/drop outcomes are logged and captured in metrics.

Reconcile loop is never blocked on CloudEvents send. The `Emit` call enqueues and returns immediately.

## Failure Handling

- Queue full: drop event, `dropped_queue_full++`, rate-limited warning.
- Send error with retries exhausted: drop event, `send_failed++`, structured error log.
- Invalid sink config: fail closed to `NoopEmitter` plus startup warning.
- Context shutdown: attempt bounded drain; do not block shutdown indefinitely.

## Testing Strategy

### Unit

- Emitter builds correct CloudEvent attributes for each operation type.
- Emitter includes status payload only when `WithStatus` option is provided.
- Async queue behavior (enqueue, full queue drop, worker send calls).
- Retry behavior invocation and terminal failure metrics.
- No sink -> `NoopEmitter` path.

### Integration

- Controllers call `Emit` at key points; fake sink receives correct CloudEvents.
- Kubernetes events are still emitted via existing `record.EventRecorder` (unchanged).
- CloudEvents disabled mode (`NoopEmitter`) generates no outbound sends.

## Observability

Metrics:

- `cloudevents_enqueued_total`
- `cloudevents_sent_total`
- `cloudevents_send_failed_total`
- `cloudevents_dropped_total`
- `cloudevents_queue_depth`
- `cloudevents_enabled` (0/1)

Logs:

- Startup mode summary (enabled/disabled).
- Rate-limited delivery failures with reason and endpoint.

## Security and Privacy

- Ensure payload only includes expected object metadata and status.
- Avoid accidental inclusion of secrets or large unrelated blobs.
- Allow optional payload redaction/filtering in future if needed.

## Rollout Plan

1. Add `github.com/cloudevents/sdk-go/v2` dependency.
2. Add eventing package (`CloudEventEmitter`, `AsyncPublisher`, `NoopEmitter`, `EmitOption` helpers).
3. Wire emitter creation in `cmd/main.go` (sink configured → real emitter; no sink → `NoopEmitter`).
4. Add `Emit` calls in target controllers (`Promise`, `ResourceRequest`, `Work`, `WorkPlacement`, `HealthRecord`).
5. Add metrics and structured logging.
6. Add tests and docs.

## Alternatives Considered

- **Durable outbox CRD + dispatcher + GC**: Stronger guarantees but higher complexity and etcd churn.
- **EventRecorder decorator with string classifier**: Wraps `record.EventRecorder` and infers operation from reason/message strings. Deprioritized because classification is fragile, hard to extend, and the call site already knows the operation.
- **In-memory direct send without worker queue**: Too coupled to reconcile latency.

## Why This Choice

This design minimizes implementation and operational complexity while preserving existing controller behavior. Using an explicit typed API rather than a recorder wrapper makes operations unambiguous, avoids fragile string classification, and keeps CloudEvent emission clearly visible in code review. The async publisher with best-effort delivery aligns with comparable systems and leaves a clear upgrade path to a durable outbox model if stricter guarantees are needed later.

## References

- [CloudEvents](https://cloudevents.io/)
- [CloudEvents specification](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md)
- [CloudEvents Go SDK](https://github.com/cloudevents/sdk-go)
- [CloudEvents Go SDK retry context](https://github.com/cloudevents/sdk-go/blob/master/v2/context/retry.go)
- [Tekton events docs](https://tekton.dev/docs/pipelines/events/)
