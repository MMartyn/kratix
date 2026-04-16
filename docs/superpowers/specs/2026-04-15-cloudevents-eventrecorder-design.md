# CloudEvents via EventRecorder Wrapper (Best-Effort Async)

## Status

Proposed and validated in brainstorming; pending implementation plan.

## Context

Kratix controllers already use Kubernetes `record.EventRecorder` extensively. We want to add CloudEvents emission in a way that is easy to review, self-contained, and low-risk to reconcile behavior.

The initial explored option was durable at-least-once delivery using an outbox CRD. After trade-off discussion, the selected direction is best-effort delivery with async retries to keep the system simple and avoid additional etcd object lifecycle management.

## Goals

- Emit CloudEvents for key operations (`create`, `edit`, `delete`, `status-change`).
- Include status payload for `status-change` events.
- Keep reconciler behavior unchanged and non-blocking.
- Preserve current Kubernetes event emission.
- Keep changes self-contained and easy to review.

## Non-Goals

- Durable at-least-once guarantees across process restarts.
- Outbox CRD, dead-letter queues, or replay pipelines.
- Exactly-once delivery or strict in-order delivery.

## Selected Architecture

### 1) EventRecorder Decorator

Add `CloudEventRecorder` implementing `record.EventRecorder`:

- Wrap an existing kube recorder (`mgr.GetEventRecorderFor(...)`).
- Forward all calls to kube recorder unchanged.
- Build CloudEvent payloads from recorder calls and enqueue async send jobs.
- Never return errors to reconcile loops.

### 2) Async Publisher with Retries

Add shared `AsyncPublisher`:

- In-memory buffered queue.
- Background worker goroutines send events using `cloudevents/sdk-go`.
- SDK retry/backoff configured per publisher settings.
- On queue full or exhausted retries: log and increment metrics, then drop event (best effort).

### 3) No Sink Means No-Op

If global sink is not configured:

- Use `NoopPublisher`.
- Keep Kubernetes event recording normal.
- Emit one startup log indicating CloudEvents are disabled.
- No periodic reminders.

### 4) Global Configuration

Use a single global sink URL and sender options:

- `eventing.cloudEvents.sink`
- `eventing.cloudEvents.source`
- `eventing.cloudEvents.queueSize`
- `eventing.cloudEvents.workerCount`
- `eventing.cloudEvents.retry.maxTries`
- `eventing.cloudEvents.retry.strategy`
- `eventing.cloudEvents.retry.period`

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

- `operation`
- `controller`
- `reason`
- `message`
- `resource`:
  - `apiVersion`
  - `kind`
  - `namespace`
  - `name`
  - `uid`
  - `resourceVersion`
- `status` (only for `status-change`)

For `status-change`, include full status payload initially for correctness and downstream flexibility.

## Operation Classification

Primary mapping comes from recorder reason/message patterns to operation:

- explicit create reasons -> `create`
- explicit delete reasons -> `delete`
- condition or health-related reasons -> `status-change`
- otherwise -> `edit`

Unknown patterns default to `edit`.

If coverage gaps appear, add minimal explicit event emits in selected controller paths while keeping wrapper as primary mechanism.

## Data Flow

1. Controller calls `EventRecorder.Event*`.
2. Decorator forwards to Kubernetes recorder.
3. Decorator maps call + object metadata to CloudEvent envelope.
4. Envelope is enqueued to async publisher.
5. Worker sends with SDK retries/backoff.
6. Success/failure/drop outcomes are logged and captured in metrics.

Reconcile loop is never blocked on CloudEvents send.

## Failure Handling

- Queue full: drop event, `dropped_queue_full++`, rate-limited warning.
- Send error with retries exhausted: drop event, `send_failed++`, structured error log.
- Invalid sink config: fail closed to `NoopPublisher` plus startup warning.
- Context shutdown: attempt bounded drain; do not block shutdown indefinitely.

## Testing Strategy

### Unit

- Recorder forwards all kube events.
- Mapper produces expected CloudEvent attributes and payloads.
- Classifier maps reasons to correct operations.
- Async queue behavior (enqueue, full queue drop, worker send calls).
- Retry behavior invocation and terminal failure metrics.
- No sink -> `NoopPublisher` path.

### Integration

- Controller wiring uses wrapped recorder for major controllers.
- Fake sink receives CloudEvents while Kubernetes events still emitted.
- CloudEvents disabled mode generates no outbound sends.

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

1. Add eventing package (`Recorder`, `Mapper`, `Publisher`, `NoopPublisher`).
2. Wire wrapped recorders in `cmd/main.go`.
3. Add initial classifier for target resources (`Work`, `WorkPlacement`, `Promise`, `ResourceRequest`).
4. Add metrics and structured logging.
5. Add tests and docs.
6. Optional follow-up: tighten operation mapping and add explicit emits for uncovered transitions.

## Alternatives Considered

- Durable outbox CRD + dispatcher + GC (stronger guarantees, higher complexity).
- Direct publisher calls in each controller (more explicit, broader code churn).
- In-memory direct send without worker queue (too coupled to reconcile latency).

## Why This Choice

This design minimizes implementation and operational complexity while preserving existing controller behavior. It keeps CloudEvents logic centralized and reviewable, aligns with async retry patterns used in comparable systems, and leaves a clear upgrade path to a durable outbox model if stricter guarantees are needed later.

## References

- [CloudEvents](https://cloudevents.io/)
- [CloudEvents specification](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md)
- [CloudEvents Go SDK](https://github.com/cloudevents/sdk-go)
- [CloudEvents Go SDK retry context](https://github.com/cloudevents/sdk-go/blob/master/v2/context/retry.go)
- [Tekton events docs](https://tekton.dev/docs/pipelines/events/)
