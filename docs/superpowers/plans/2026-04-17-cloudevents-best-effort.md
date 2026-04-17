# CloudEvents Best-Effort Async Emission — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add best-effort CloudEvents emission to Kratix controllers via an explicit typed API, backed by an async in-memory publisher using the CloudEvents Go SDK.

**Architecture:** Controllers call `emitter.Emit(obj, operation, opts...)` at key points alongside existing kube event recording. The emitter enqueues CloudEvent envelopes into an in-memory buffered channel. Background workers drain the channel and send events via `cloudevents/sdk-go/v2` HTTP protocol binding with configurable retry/backoff. If no sink is configured, a `NoopEmitter` is used and no CloudEvents are sent.

**Tech Stack:** Go 1.25, `github.com/cloudevents/sdk-go/v2`, controller-runtime, Ginkgo v2 / Gomega, OpenTelemetry (otel.Meter), Counterfeiter for fakes.

**Spec:** `docs/superpowers/specs/2026-04-15-cloudevents-eventrecorder-design.md`

---

## File Structure

### New files (all under `internal/eventing/`)

| File | Responsibility |
|------|---------------|
| `internal/eventing/emitter.go` | `CloudEventEmitter` interface, `Operation` type + constants, `EmitOption` functional options, `emitConfig` internal struct |
| `internal/eventing/publisher.go` | `AsyncPublisher` — buffered channel, workers, SDK client creation, retry context, send loop, metrics, shutdown drain |
| `internal/eventing/noop.go` | `NoopEmitter` — implements `CloudEventEmitter` as no-ops |
| `internal/eventing/config.go` | `Config` struct (maps to `KratixConfig.Eventing.CloudEvents`), `NewEmitter()` factory, config validation |
| `internal/eventing/metrics.go` | Prometheus/OTel counters: enqueued, sent, failed, dropped, queue depth, enabled gauge |
| `internal/eventing/emitter_test.go` | Unit tests for emitter envelope construction |
| `internal/eventing/publisher_test.go` | Unit tests for async publisher (queue, drop, send, drain) |
| `internal/eventing/noop_test.go` | Unit test confirming noop is truly inert |
| `internal/eventing/config_test.go` | Unit tests for config validation and `NewEmitter` factory paths |
| `internal/eventing/suite_test.go` | Ginkgo test suite bootstrap |

### Modified files

| File | Change |
|------|--------|
| `go.mod` / `go.sum` | Add `github.com/cloudevents/sdk-go/v2` |
| `cmd/main.go` | Add `Eventing` to `KratixConfig`, create emitter, pass to controllers |
| `internal/controller/promise_controller.go` | Add `CloudEvents` field, `Emit` calls at key points, thread to dynamic controller |
| `internal/controller/dynamic_resource_request_controller.go` | Add `CloudEvents` field, `Emit` calls |
| `internal/controller/work_controller.go` | Add `CloudEvents` field, `Emit` calls |
| `internal/controller/workplacement_controller.go` | Add `CloudEvents` field, `Emit` calls |
| `internal/controller/healthrecord_controller.go` | Add `CloudEvents` field, `Emit` calls |
| `internal/controller/scheduler.go` | Add `CloudEvents` field, `Emit` calls |
| `internal/controller/promise_controller_test.go` | Provide `NoopEmitter` in test setup |
| Controller test files that construct reconciler structs | Provide `NoopEmitter` in test setup |

---

## Task 1: Add SDK dependency

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Add the CloudEvents Go SDK dependency**

```bash
go get github.com/cloudevents/sdk-go/v2@latest
```

- [ ] **Step 2: Tidy modules**

```bash
go mod tidy
```

- [ ] **Step 3: Verify the dependency is in go.mod**

```bash
grep 'cloudevents/sdk-go' go.mod
```

Expected: a line like `github.com/cloudevents/sdk-go/v2 v2.x.x`

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "deps: add cloudevents/sdk-go/v2"
```

---

## Task 2: Eventing types — interface, operations, options

**Files:**
- Create: `internal/eventing/emitter.go`
- Create: `internal/eventing/noop.go`

- [ ] **Step 1: Create `internal/eventing/emitter.go`**

```go
package eventing

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
)

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

type emitConfig struct {
	status  any
	message string
}

type EmitOption func(*emitConfig)

func WithStatus(status any) EmitOption {
	return func(c *emitConfig) {
		c.status = status
	}
}

func WithMessage(msg string) EmitOption {
	return func(c *emitConfig) {
		c.message = msg
	}
}

func applyOpts(opts []EmitOption) emitConfig {
	cfg := emitConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}
```

- [ ] **Step 2: Create `internal/eventing/noop.go`**

```go
package eventing

import "sigs.k8s.io/controller-runtime/pkg/client"

type NoopEmitter struct{}

var _ CloudEventEmitter = (*NoopEmitter)(nil)

func (n *NoopEmitter) Emit(_ client.Object, _ Operation, _ ...EmitOption) {}
```

- [ ] **Step 3: Verify it compiles**

```bash
go build ./internal/eventing/...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add internal/eventing/emitter.go internal/eventing/noop.go
git commit -m "feat(eventing): add CloudEventEmitter interface, Operation types, NoopEmitter"
```

---

## Task 3: Eventing configuration

**Files:**
- Create: `internal/eventing/config.go`

- [ ] **Step 1: Create `internal/eventing/config.go`**

```go
package eventing

import (
	"fmt"
	"time"

	cecontext "github.com/cloudevents/sdk-go/v2/context"
)

type Config struct {
	Sink        string      `json:"sink,omitempty"`
	Source      string      `json:"source,omitempty"`
	QueueSize   int         `json:"queueSize,omitempty"`
	WorkerCount int         `json:"workerCount,omitempty"`
	Retry       RetryConfig `json:"retry,omitempty"`
}

type RetryConfig struct {
	MaxTries int    `json:"maxTries,omitempty"`
	Strategy string `json:"strategy,omitempty"`
	Period   string `json:"period,omitempty"`
}

const (
	defaultQueueSize   = 1024
	defaultWorkerCount = 2
	defaultMaxTries    = 3
	defaultStrategy    = "constant"
	defaultPeriod      = "1s"
	defaultSource      = "https://kratix.io/controller-manager"
)

func (c *Config) withDefaults() Config {
	out := *c
	if out.Source == "" {
		out.Source = defaultSource
	}
	if out.QueueSize <= 0 {
		out.QueueSize = defaultQueueSize
	}
	if out.WorkerCount <= 0 {
		out.WorkerCount = defaultWorkerCount
	}
	if out.Retry.MaxTries <= 0 {
		out.Retry.MaxTries = defaultMaxTries
	}
	if out.Retry.Strategy == "" {
		out.Retry.Strategy = defaultStrategy
	}
	if out.Retry.Period == "" {
		out.Retry.Period = defaultPeriod
	}
	return out
}

func (c *Config) retryParams() (*cecontext.RetryParams, error) {
	cfg := c.withDefaults()

	period, err := time.ParseDuration(cfg.Retry.Period)
	if err != nil {
		return nil, fmt.Errorf("invalid retry period %q: %w", cfg.Retry.Period, err)
	}

	strategy, err := parseStrategy(cfg.Retry.Strategy)
	if err != nil {
		return nil, err
	}

	return &cecontext.RetryParams{
		Strategy: strategy,
		MaxTries: cfg.Retry.MaxTries,
		Period:   period,
	}, nil
}

func parseStrategy(s string) (cecontext.BackoffStrategy, error) {
	switch s {
	case "none":
		return cecontext.BackoffStrategyNone, nil
	case "constant":
		return cecontext.BackoffStrategyConstant, nil
	case "linear":
		return cecontext.BackoffStrategyLinear, nil
	case "exponential":
		return cecontext.BackoffStrategyExponential, nil
	default:
		return "", fmt.Errorf("unknown retry strategy %q: must be one of none, constant, linear, exponential", s)
	}
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/eventing/...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/eventing/config.go
git commit -m "feat(eventing): add Config struct with defaults and SDK retry param mapping"
```

---

## Task 4: Metrics registration

**Files:**
- Create: `internal/eventing/metrics.go`

This follows the existing pattern from `internal/telemetry/metrics.go`: lazy `sync.Once` init with `otel.Meter`, plus a test-reset helper.

- [ ] **Step 1: Create `internal/eventing/metrics.go`**

```go
package eventing

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/syntasso/kratix/internal/eventing"

var (
	enqueuedCounter metric.Int64Counter
	sentCounter     metric.Int64Counter
	failedCounter   metric.Int64Counter
	droppedCounter  metric.Int64Counter
	enabledGauge    metric.Int64Gauge

	metricsOnce sync.Once
	metricsErr  error
)

func initMetrics() {
	metricsOnce.Do(func() {
		m := otel.Meter(meterName)
		enqueuedCounter, metricsErr = m.Int64Counter("cloudevents_enqueued_total",
			metric.WithDescription("Total CloudEvents enqueued for sending"))
		if metricsErr != nil {
			return
		}
		sentCounter, metricsErr = m.Int64Counter("cloudevents_sent_total",
			metric.WithDescription("Total CloudEvents successfully sent"))
		if metricsErr != nil {
			return
		}
		failedCounter, metricsErr = m.Int64Counter("cloudevents_send_failed_total",
			metric.WithDescription("Total CloudEvents that failed to send after retries"))
		if metricsErr != nil {
			return
		}
		droppedCounter, metricsErr = m.Int64Counter("cloudevents_dropped_total",
			metric.WithDescription("Total CloudEvents dropped (queue full)"))
		if metricsErr != nil {
			return
		}
		enabledGauge, metricsErr = m.Int64Gauge("cloudevents_enabled",
			metric.WithDescription("Whether CloudEvents emission is enabled (0 or 1)"))
	})
}

func recordEnqueued(ctx context.Context) {
	initMetrics()
	if enqueuedCounter != nil {
		enqueuedCounter.Add(ctx, 1)
	}
}

func recordSent(ctx context.Context) {
	initMetrics()
	if sentCounter != nil {
		sentCounter.Add(ctx, 1)
	}
}

func recordFailed(ctx context.Context) {
	initMetrics()
	if failedCounter != nil {
		failedCounter.Add(ctx, 1)
	}
}

func recordDropped(ctx context.Context) {
	initMetrics()
	if droppedCounter != nil {
		droppedCounter.Add(ctx, 1)
	}
}

func recordEnabled(ctx context.Context, enabled bool) {
	initMetrics()
	if enabledGauge != nil {
		v := int64(0)
		if enabled {
			v = 1
		}
		enabledGauge.Record(ctx, v)
	}
}

func ResetMetricsForTest() {
	enqueuedCounter = nil
	sentCounter = nil
	failedCounter = nil
	droppedCounter = nil
	enabledGauge = nil
	metricsErr = nil
	metricsOnce = sync.Once{}
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/eventing/...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/eventing/metrics.go
git commit -m "feat(eventing): add OTel metrics for CloudEvents emission"
```

---

## Task 5: Async publisher — test first

**Files:**
- Create: `internal/eventing/suite_test.go`
- Create: `internal/eventing/publisher_test.go`

- [ ] **Step 1: Create `internal/eventing/suite_test.go`**

```go
package eventing_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEventing(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Eventing Suite")
}
```

- [ ] **Step 2: Create `internal/eventing/publisher_test.go`**

These tests exercise the `AsyncPublisher` through the `CloudEventEmitter` interface. They use a fake CE client to capture sent events and verify queue behavior.

```go
package eventing_test

import (
	"context"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/protocol"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/syntasso/kratix/internal/eventing"
)

type fakeSender struct {
	events chan cloudevents.Event
	result protocol.Result
}

func (f *fakeSender) Send(ctx context.Context, e cloudevents.Event) protocol.Result {
	f.events <- e
	return f.result
}

var _ = Describe("AsyncPublisher", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		sender *fakeSender
		pub    *eventing.AsyncPublisher
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		sender = &fakeSender{
			events: make(chan cloudevents.Event, 100),
			result: protocol.ResultACK,
		}
	})

	AfterEach(func() {
		if pub != nil {
			pub.Shutdown(ctx)
		}
		cancel()
	})

	It("sends a CloudEvent with correct attributes for a create operation", func() {
		pub = eventing.NewAsyncPublisher(sender, "https://kratix.io/test", 10, 1)
		pub.Start(ctx)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "my-pod",
				Namespace:       "default",
				UID:             types.UID("abc-123"),
				ResourceVersion: "42",
			},
		}
		pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))

		pub.Emit(pod, eventing.OperationCreate)

		var sent cloudevents.Event
		Eventually(sender.events, 2*time.Second).Should(Receive(&sent))

		Expect(sent.Type()).To(Equal("io.kratix.pod.create.v1"))
		Expect(sent.Source()).To(Equal("https://kratix.io/test"))
		Expect(sent.Subject()).To(Equal("Pod/default/my-pod"))
	})

	It("includes status in the payload when WithStatus is used", func() {
		pub = eventing.NewAsyncPublisher(sender, "https://kratix.io/test", 10, 1)
		pub.Start(ctx)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-pod",
				Namespace: "default",
				UID:       types.UID("abc-123"),
			},
		}
		pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))

		statusData := map[string]string{"phase": "Running"}
		pub.Emit(pod, eventing.OperationStatusChange, eventing.WithStatus(statusData))

		var sent cloudevents.Event
		Eventually(sender.events, 2*time.Second).Should(Receive(&sent))

		Expect(sent.Type()).To(Equal("io.kratix.pod.status-change.v1"))

		var data map[string]any
		Expect(sent.DataAs(&data)).To(Succeed())
		Expect(data).To(HaveKey("status"))
	})

	It("drops events when the queue is full instead of blocking", func() {
		pub = eventing.NewAsyncPublisher(sender, "https://kratix.io/test", 1, 0)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		}
		pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))

		pub.Emit(pod, eventing.OperationCreate)
		pub.Emit(pod, eventing.OperationCreate)
	})
})
```

- [ ] **Step 3: Run the tests — they should fail (publisher doesn't exist yet)**

```bash
go run github.com/onsi/ginkgo/v2/ginkgo -r ./internal/eventing/
```

Expected: compilation errors — `eventing.NewAsyncPublisher` undefined, `eventing.AsyncPublisher` undefined.

- [ ] **Step 4: Commit the failing tests**

```bash
git add internal/eventing/suite_test.go internal/eventing/publisher_test.go
git commit -m "test(eventing): add failing tests for AsyncPublisher"
```

---

## Task 6: Async publisher — implementation

**Files:**
- Create: `internal/eventing/publisher.go`

- [ ] **Step 1: Create `internal/eventing/publisher.go`**

```go
package eventing

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/google/uuid"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var publisherLog = ctrl.Log.WithName("eventing").WithName("publisher")

type Sender interface {
	Send(ctx context.Context, e cloudevents.Event) protocol.Result
}

type envelope struct {
	event cloudevents.Event
}

type AsyncPublisher struct {
	sender  Sender
	source  string
	queue   chan envelope
	wg      sync.WaitGroup
	workers int
}

var _ CloudEventEmitter = (*AsyncPublisher)(nil)

func NewAsyncPublisher(sender Sender, source string, queueSize, workerCount int) *AsyncPublisher {
	return &AsyncPublisher{
		sender:  sender,
		source:  source,
		queue:   make(chan envelope, queueSize),
		workers: workerCount,
	}
}

func (p *AsyncPublisher) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
	publisherLog.Info("async publisher started", "workers", p.workers, "queueSize", cap(p.queue))
}

func (p *AsyncPublisher) Shutdown(ctx context.Context) {
	close(p.queue)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		publisherLog.Info("async publisher drained and shut down")
	case <-ctx.Done():
		publisherLog.Info("async publisher shutdown timed out; some events may have been lost")
	}
}

func (p *AsyncPublisher) Emit(obj client.Object, op Operation, opts ...EmitOption) {
	cfg := applyOpts(opts)

	ce, err := p.buildEvent(obj, op, cfg)
	if err != nil {
		publisherLog.Error(err, "failed to build CloudEvent; dropping", "operation", op)
		return
	}

	select {
	case p.queue <- envelope{event: ce}:
		recordEnqueued(context.Background())
	default:
		recordDropped(context.Background())
		publisherLog.V(1).Info("queue full; dropping CloudEvent",
			"type", ce.Type(), "subject", ce.Subject())
	}
}

func (p *AsyncPublisher) buildEvent(obj client.Object, op Operation, cfg emitConfig) (cloudevents.Event, error) {
	e := cloudevents.NewEvent()
	e.SetID(uuid.NewString())
	e.SetSource(p.source)
	e.SetTime(time.Now().UTC())

	kind := obj.GetObjectKind().GroupVersionKind().Kind
	if kind == "" {
		kind = "unknown"
	}
	e.SetType(fmt.Sprintf("io.kratix.%s.%s.v1", strings.ToLower(kind), string(op)))

	ns := obj.GetNamespace()
	name := obj.GetName()
	if ns != "" {
		e.SetSubject(fmt.Sprintf("%s/%s/%s", kind, ns, name))
	} else {
		e.SetSubject(fmt.Sprintf("%s/%s", kind, name))
	}

	gvk := obj.GetObjectKind().GroupVersionKind()
	payload := map[string]any{
		"operation": string(op),
		"resource": map[string]any{
			"apiVersion":      gvk.GroupVersion().String(),
			"kind":            kind,
			"namespace":       ns,
			"name":            name,
			"uid":             string(obj.GetUID()),
			"resourceVersion": obj.GetResourceVersion(),
		},
	}
	if cfg.message != "" {
		payload["message"] = cfg.message
	}
	if cfg.status != nil {
		statusJSON, err := json.Marshal(cfg.status)
		if err != nil {
			return e, fmt.Errorf("marshalling status: %w", err)
		}
		var statusAny any
		if err := json.Unmarshal(statusJSON, &statusAny); err != nil {
			return e, fmt.Errorf("unmarshalling status: %w", err)
		}
		payload["status"] = statusAny
	}

	if err := e.SetData(cloudevents.ApplicationJSON, payload); err != nil {
		return e, fmt.Errorf("setting event data: %w", err)
	}
	return e, nil
}

func (p *AsyncPublisher) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	for env := range p.queue {
		result := p.sender.Send(ctx, env.event)
		if protocol.IsACK(result) {
			recordSent(ctx)
		} else {
			recordFailed(ctx)
			publisherLog.V(1).Info("CloudEvent send failed after retries",
				"type", env.event.Type(), "subject", env.event.Subject(),
				"worker", id, "result", result)
		}
	}
}
```

- [ ] **Step 2: Run the tests — they should pass**

```bash
go run github.com/onsi/ginkgo/v2/ginkgo -r ./internal/eventing/
```

Expected: all 3 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/eventing/publisher.go
git commit -m "feat(eventing): implement AsyncPublisher with queue, workers, and metrics"
```

---

## Task 7: Config-driven factory — test first

**Files:**
- Create: `internal/eventing/config_test.go`

- [ ] **Step 1: Create `internal/eventing/config_test.go`**

```go
package eventing_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/syntasso/kratix/internal/eventing"
)

var _ = Describe("NewEmitter", func() {
	It("returns a NoopEmitter when config is nil", func() {
		emitter, shutdown, err := eventing.NewEmitter(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(emitter).To(BeAssignableToTypeOf(&eventing.NoopEmitter{}))
		Expect(shutdown).NotTo(BeNil())
	})

	It("returns a NoopEmitter when sink is empty", func() {
		cfg := &eventing.Config{Sink: ""}
		emitter, shutdown, err := eventing.NewEmitter(cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(emitter).To(BeAssignableToTypeOf(&eventing.NoopEmitter{}))
		Expect(shutdown).NotTo(BeNil())
	})

	It("returns an error for an invalid retry strategy", func() {
		cfg := &eventing.Config{
			Sink:  "http://localhost:8080",
			Retry: eventing.RetryConfig{Strategy: "bogus"},
		}
		_, _, err := eventing.NewEmitter(cfg)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unknown retry strategy"))
	})

	It("returns an error for an invalid retry period", func() {
		cfg := &eventing.Config{
			Sink:  "http://localhost:8080",
			Retry: eventing.RetryConfig{Period: "not-a-duration"},
		}
		_, _, err := eventing.NewEmitter(cfg)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid retry period"))
	})
})
```

- [ ] **Step 2: Run the tests — they should fail**

```bash
go run github.com/onsi/ginkgo/v2/ginkgo -r ./internal/eventing/
```

Expected: compilation error — `eventing.NewEmitter` undefined.

- [ ] **Step 3: Commit the failing test**

```bash
git add internal/eventing/config_test.go
git commit -m "test(eventing): add failing tests for NewEmitter factory"
```

---

## Task 8: Config-driven factory — implementation

**Files:**
- Modify: `internal/eventing/config.go`

- [ ] **Step 1: Add `NewEmitter` to `internal/eventing/config.go`**

Append the following to the end of `config.go`:

```go
import (
	"context"

	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	ceclient "github.com/cloudevents/sdk-go/v2/client"
	cecontext "github.com/cloudevents/sdk-go/v2/context"
	ctrl "sigs.k8s.io/controller-runtime"
)

var configLog = ctrl.Log.WithName("eventing").WithName("config")

// NewEmitter creates the appropriate CloudEventEmitter based on config.
// Returns the emitter plus a shutdown function that should be deferred.
func NewEmitter(cfg *Config) (CloudEventEmitter, func(context.Context), error) {
	noopShutdown := func(context.Context) {}

	if cfg == nil || cfg.Sink == "" {
		configLog.Info("CloudEvents sink not configured; emission disabled")
		recordEnabled(context.Background(), false)
		return &NoopEmitter{}, noopShutdown, nil
	}

	withDefaults := cfg.withDefaults()

	retryParams, err := cfg.retryParams()
	if err != nil {
		return nil, nil, err
	}

	p, err := cehttp.New(cehttp.WithTarget(withDefaults.Sink))
	if err != nil {
		return nil, nil, fmt.Errorf("creating CE HTTP protocol: %w", err)
	}

	ceClient, err := ceclient.New(p, ceclient.WithUUIDs(), ceclient.WithTimeNow())
	if err != nil {
		return nil, nil, fmt.Errorf("creating CE client: %w", err)
	}

	retrySender := &retrySenderWrapper{
		client:      ceClient,
		retryParams: retryParams,
	}

	pub := NewAsyncPublisher(retrySender, withDefaults.Source, withDefaults.QueueSize, withDefaults.WorkerCount)
	pub.Start(context.Background())

	configLog.Info("CloudEvents emission enabled",
		"sink", withDefaults.Sink,
		"source", withDefaults.Source,
		"queueSize", withDefaults.QueueSize,
		"workerCount", withDefaults.WorkerCount,
		"retryStrategy", withDefaults.Retry.Strategy,
		"retryMaxTries", withDefaults.Retry.MaxTries,
		"retryPeriod", withDefaults.Retry.Period,
	)
	recordEnabled(context.Background(), true)

	shutdown := func(ctx context.Context) {
		pub.Shutdown(ctx)
	}
	return pub, shutdown, nil
}

// retrySenderWrapper decorates a CE client.Client with retry params in the context.
type retrySenderWrapper struct {
	client      ceclient.Client
	retryParams *cecontext.RetryParams
}

func (w *retrySenderWrapper) Send(ctx context.Context, e cloudevents.Event) protocol.Result {
	ctx = cecontext.WithRetryParams(ctx, w.retryParams)
	return w.client.Send(ctx, e)
}
```

Note: you'll need to merge the imports with the existing imports at the top of `config.go`. The final import block should include all necessary packages from both the original file and the additions above. The `cloudevents` and `protocol` imports come from the top-level `github.com/cloudevents/sdk-go/v2` and `github.com/cloudevents/sdk-go/v2/protocol` respectively.

- [ ] **Step 2: Run all eventing tests — they should pass**

```bash
go run github.com/onsi/ginkgo/v2/ginkgo -r ./internal/eventing/
```

Expected: all tests PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/eventing/config.go
git commit -m "feat(eventing): implement NewEmitter factory with SDK client and retry wiring"
```

---

## Task 9: Noop emitter test

**Files:**
- Create: `internal/eventing/noop_test.go`

- [ ] **Step 1: Create `internal/eventing/noop_test.go`**

```go
package eventing_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/syntasso/kratix/internal/eventing"
)

var _ = Describe("NoopEmitter", func() {
	It("implements CloudEventEmitter and does not panic", func() {
		noop := &eventing.NoopEmitter{}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		}
		Expect(func() {
			noop.Emit(pod, eventing.OperationCreate)
			noop.Emit(pod, eventing.OperationStatusChange, eventing.WithStatus("ok"))
		}).NotTo(Panic())
	})
})
```

- [ ] **Step 2: Run the test**

```bash
go run github.com/onsi/ginkgo/v2/ginkgo -r ./internal/eventing/
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/eventing/noop_test.go
git commit -m "test(eventing): add NoopEmitter test"
```

---

## Task 10: Wire emitter into KratixConfig and cmd/main.go

**Files:**
- Modify: `cmd/main.go`

- [ ] **Step 1: Add `Eventing` field to `KratixConfig`**

In `cmd/main.go`, find the `KratixConfig` struct (around line 75) and add the eventing config field:

```go
type KratixConfig struct {
	Workflows                Workflows             `json:"workflows"`
	NumberOfJobsToKeep       int                   `json:"numberOfJobsToKeep,omitempty"`
	ControllerLeaderElection *LeaderElectionConfig `json:"controllerLeaderElection,omitempty"`
	SelectiveCache           bool                  `json:"selectiveCache,omitempty"`
	ReconciliationInterval   *metav1.Duration      `json:"reconciliationInterval,omitempty"`
	Telemetry                *telemetry.Config     `json:"telemetry,omitempty"`
	Logging                  *LoggingConfig        `json:"logging,omitempty"`
	FeatureFlags             *FeatureFlags         `json:"featureFlags,omitempty"`
	Eventing                 *EventingConfig       `json:"eventing,omitempty"`
}

type EventingConfig struct {
	CloudEvents *eventing.Config `json:"cloudEvents,omitempty"`
}
```

Add the import: `"github.com/syntasso/kratix/internal/eventing"`

- [ ] **Step 2: Create the emitter after manager creation**

In `cmd/main.go`, after the line `repositoryCache := controller.NewRepositoryCache()` (around line 268), add:

```go
	var cloudEventsConfig *eventing.Config
	if kratixConfig != nil && kratixConfig.Eventing != nil {
		cloudEventsConfig = kratixConfig.Eventing.CloudEvents
	}
	cloudEventEmitter, cloudEventsShutdown, err := eventing.NewEmitter(cloudEventsConfig)
	if err != nil {
		setupLog.Error(err, "unable to create CloudEvents emitter")
		os.Exit(1)
	}
	defer cloudEventsShutdown(ctx)
```

- [ ] **Step 3: Pass `cloudEventEmitter` to each controller that has EventRecorder**

For each controller struct literal in `main.go`, add a `CloudEvents: cloudEventEmitter,` field. The controllers to update are:

- `Scheduler` (line ~270)
- `PromiseReconciler` (line ~276)
- `WorkReconciler` (line ~300)
- `DestinationReconciler` (line ~310)
- `PromiseReleaseReconciler` (line ~325)
- `HealthRecordReconciler` (line ~339)
- `BucketStateStoreReconciler` (line ~352)
- `GitStateStoreReconciler` (line ~363)
- `WorkPlacementReconciler` (line ~373)

Example for `PromiseReconciler`:

```go
	if err = (&controller.PromiseReconciler{
		ApiextensionsClient:    apiextensionsClient.ApiextensionsV1(),
		Client:                 mgr.GetClient(),
		Log:                    ctrl.Log.WithName("controllers").WithName("Promise"),
		Manager:                mgr,
		Scheme:                 mgr.GetScheme(),
		NumberOfJobsToKeep:     getNumJobsToKeep(kratixConfig),
		ReconciliationInterval: getRegularReconciliationInterval(kratixConfig),
		EventRecorder:          mgr.GetEventRecorderFor("PromiseController"),
		PromiseUpgrade:         promiseUpgradeEnabled(kratixConfig),
		CloudEvents:            cloudEventEmitter,
	}).SetupWithManager(mgr); err != nil {
```

Repeat the same pattern for all controllers listed above.

- [ ] **Step 4: Verify it compiles (it won't yet — controllers don't have the field)**

This is expected to fail at this point. We verify that `cmd/main.go` has no syntax errors:

```bash
go vet ./cmd/...
```

Expected: errors about `CloudEvents` field not existing on controller structs. This is expected — Task 11 will add those fields.

- [ ] **Step 5: Commit the main.go changes**

```bash
git add cmd/main.go
git commit -m "feat(eventing): wire CloudEventEmitter into KratixConfig and main.go"
```

---

## Task 11: Add CloudEvents field to controller structs

**Files:**
- Modify: `internal/controller/promise_controller.go`
- Modify: `internal/controller/dynamic_resource_request_controller.go`
- Modify: `internal/controller/work_controller.go`
- Modify: `internal/controller/workplacement_controller.go`
- Modify: `internal/controller/healthrecord_controller.go`
- Modify: `internal/controller/scheduler.go`
- Modify: `internal/controller/destination_controller.go`
- Modify: `internal/controller/promiserelease_controller.go`
- Modify: `internal/controller/bucketstatestore_controller.go`
- Modify: `internal/controller/gitstatestore_controller.go`

- [ ] **Step 1: Add `CloudEvents` field to each reconciler struct**

For each controller struct listed above, add:

```go
CloudEvents eventing.CloudEventEmitter
```

And add the import:

```go
"github.com/syntasso/kratix/internal/eventing"
```

For example, in `internal/controller/promise_controller.go` the struct becomes:

```go
type PromiseReconciler struct {
	Scheme                    *runtime.Scheme
	Client                    client.Client
	ApiextensionsClient       apiextensionsv1cs.CustomResourceDefinitionsGetter
	Log                       logr.Logger
	Manager                   ctrl.Manager
	StartedDynamicControllers map[string]*DynamicResourceRequestController
	NumberOfJobsToKeep        int
	ReconciliationInterval    time.Duration
	EventRecorder             record.EventRecorder
	PromiseUpgrade            bool
	CloudEvents               eventing.CloudEventEmitter
}
```

Repeat for all structs. For `Scheduler`, the struct is in `scheduler.go`. For `DynamicResourceRequestController`, it is in `dynamic_resource_request_controller.go`.

- [ ] **Step 2: Thread `CloudEvents` into the dynamic controller in `promise_controller.go`**

In `ensureDynamicControllerIsStarted`, around lines 1138 and 1170, where `dynamicController.EventRecorder` is set, add:

```go
dynamicController.CloudEvents = r.CloudEvents
```

Do this in both the "reuse existing" path (line ~1138) and the "create new" path (line ~1170).

- [ ] **Step 3: Fix test compilation — provide NoopEmitter in test setups**

In each test file that constructs a reconciler struct (e.g. `promise_controller_test.go` line ~77, `work_controller_test.go`, etc.), add `CloudEvents: &eventing.NoopEmitter{}` to the struct literal and add the import `"github.com/syntasso/kratix/internal/eventing"`.

Example for `promise_controller_test.go`:

```go
reconciler = &controller.PromiseReconciler{
	Client:                 fakeK8sClient,
	ApiextensionsClient:    fakeApiExtensionsClient,
	Log:                    l,
	Manager:                m,
	ReconciliationInterval: controller.DefaultReconciliationInterval,
	EventRecorder:          eventRecorder,
	CloudEvents:            &eventing.NoopEmitter{},
}
```

Repeat for every test file that constructs a controller struct directly.

- [ ] **Step 4: Verify everything compiles and existing tests pass**

```bash
go build ./...
go run github.com/onsi/ginkgo/v2/ginkgo -r --skip-package=system,core,git
```

Expected: all tests PASS (no behavior change yet).

- [ ] **Step 5: Commit**

```bash
git add internal/controller/ cmd/main.go
git commit -m "feat(eventing): add CloudEvents field to all controller structs and thread through"
```

---

## Task 12: Add Emit calls in Promise controller

**Files:**
- Modify: `internal/controller/promise_controller.go`

This task adds `Emit` calls at key reconciliation points in the Promise controller. The existing `EventRecorder` calls remain unchanged.

- [ ] **Step 1: Identify emit points and add calls**

Add `Emit` calls next to the existing `r.EventRecorder.Event*` calls in `promise_controller.go`. For each existing kube event, add the corresponding `Emit` call. Key points:

When a Promise is first reconciled successfully (around `ReconcileSucceeded` event, ~line 531):
```go
r.EventRecorder.Event(promise, v1.EventTypeNormal, "ReconcileSucceeded", "Successfully reconciled")
r.CloudEvents.Emit(promise, eventing.OperationEdit, eventing.WithMessage("Successfully reconciled"))
```

When a Promise becomes available (~line 871):
```go
r.EventRecorder.Eventf(promise, "Normal", "Available", "Promise is available")
r.CloudEvents.Emit(promise, eventing.OperationStatusChange, eventing.WithMessage("Promise is available"))
```

When a revision is created (~line 400):
```go
r.EventRecorder.Eventf(promise, v1.EventTypeNormal, "RevisionCreated", ...)
r.CloudEvents.Emit(promise, eventing.OperationCreate, eventing.WithMessage(fmt.Sprintf("Revision %s created", revision.GetName())))
```

Do NOT add `Emit` at every single kube event — focus on the key lifecycle events: create/edit/delete/status-change. Use your judgment: if the kube event signals an actual state transition, add an `Emit`; if it's an intermediate retry/warning, skip it.

- [ ] **Step 2: Run the promise controller tests**

```bash
go run github.com/onsi/ginkgo/v2/ginkgo -r ./internal/controller/ --focus="PromiseController"
```

Expected: all existing tests PASS (Emit goes to NoopEmitter in tests).

- [ ] **Step 3: Commit**

```bash
git add internal/controller/promise_controller.go
git commit -m "feat(eventing): add CloudEvent Emit calls to Promise controller"
```

---

## Task 13: Add Emit calls in DynamicResourceRequest controller

**Files:**
- Modify: `internal/controller/dynamic_resource_request_controller.go`

- [ ] **Step 1: Add Emit calls at key points**

Similar to Task 12, add `Emit` calls next to significant kube events:

- `ReconcileStarted` → `OperationEdit` (resource request reconciliation started)
- `ReconcileSucceeded` → `OperationEdit`
- `WorksSucceeded` / `WorksFailing` / `WorksMisplaced` → `OperationStatusChange`
- Delete pipeline completed → `OperationDelete`

- [ ] **Step 2: Run the tests**

```bash
go run github.com/onsi/ginkgo/v2/ginkgo -r ./internal/controller/
```

Expected: all tests PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/controller/dynamic_resource_request_controller.go
git commit -m "feat(eventing): add CloudEvent Emit calls to DynamicResourceRequest controller"
```

---

## Task 14: Add Emit calls in Work, WorkPlacement, and HealthRecord controllers

**Files:**
- Modify: `internal/controller/work_controller.go`
- Modify: `internal/controller/workplacement_controller.go`
- Modify: `internal/controller/healthrecord_controller.go`
- Modify: `internal/controller/scheduler.go`

- [ ] **Step 1: Add Emit calls to Work controller**

Key points:
- `WorkplacementsFailing` → `OperationStatusChange`
- Scheduling events → `OperationEdit`

- [ ] **Step 2: Add Emit calls to Scheduler**

Key points:
- `AllWorkplacementsScheduled` → `OperationStatusChange`
- `WorkplacementReconciled` → `OperationEdit`

- [ ] **Step 3: Add Emit calls to WorkPlacement controller**

Key points: status transitions on workplacement reconciliation.

- [ ] **Step 4: Add Emit calls to HealthRecord controller**

In `fireEvent` method (~line 200), alongside the existing `r.EventRecorder.Eventf`, add:

```go
func (r *HealthRecordReconciler) fireEvent(
	healthRecord *platformv1alpha1.HealthRecord,
	resReq *unstructured.Unstructured,
) {
	if healthRecord.Data.State != "healthy" && healthRecord.Data.State != "ready" {
		r.EventRecorder.Eventf(resReq, "Warning", "HealthRecord", "Health state is %s", healthRecord.Data.State)
	} else {
		r.EventRecorder.Eventf(resReq, "Normal", "HealthRecord", "Health state is %s", healthRecord.Data.State)
	}
	r.CloudEvents.Emit(resReq, eventing.OperationStatusChange,
		eventing.WithMessage(fmt.Sprintf("Health state is %s", healthRecord.Data.State)))
}
```

Note: `resReq` is an `*unstructured.Unstructured` which implements `client.Object`, so it can be passed directly to `Emit`.

- [ ] **Step 5: Run all controller tests**

```bash
go run github.com/onsi/ginkgo/v2/ginkgo -r --skip-package=system,core,git
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/controller/work_controller.go internal/controller/workplacement_controller.go internal/controller/healthrecord_controller.go internal/controller/scheduler.go
git commit -m "feat(eventing): add CloudEvent Emit calls to Work, WorkPlacement, HealthRecord controllers"
```

---

## Task 15: Full test run and cleanup

**Files:** (none new)

- [ ] **Step 1: Run the full test suite**

```bash
make test
```

Expected: all tests PASS, no regressions.

- [ ] **Step 2: Run linter/vet**

```bash
go vet ./...
```

Expected: no errors.

- [ ] **Step 3: Check for any missed compilation issues**

```bash
go build ./...
```

Expected: clean build.

- [ ] **Step 4: Final commit if any cleanup was needed**

```bash
git add -A
git status
```

If there are changes:

```bash
git commit -m "chore: cleanup after CloudEvents integration"
```

---

## Self-Review

### Spec coverage check

| Spec section | Task(s) |
|---|---|
| SDK Dependency | Task 1 |
| Explicit Eventing API (`CloudEventEmitter`, `Operation`, `EmitOption`) | Task 2 |
| Async Publisher with Retries | Tasks 5-6 |
| No Sink → NoopEmitter | Tasks 2, 7-8 |
| Global Configuration (SDK-mapped + publisher) | Tasks 3, 8, 10 |
| Event Model (type format, attributes, payload) | Task 6 (`buildEvent`) |
| Operation Specification (typed constants) | Task 2 |
| Data Flow | Tasks 6, 10-14 |
| Failure Handling (queue full, retries exhausted, no sink) | Tasks 6, 8 |
| Testing Strategy — Unit | Tasks 5, 7, 9 |
| Testing Strategy — Integration | Task 15 (existing tests + NoopEmitter) |
| Observability (metrics) | Task 4 |
| Rollout Plan step 1 (SDK dep) | Task 1 |
| Rollout Plan step 2 (eventing package) | Tasks 2-9 |
| Rollout Plan step 3 (wire in main) | Task 10 |
| Rollout Plan step 4 (Emit calls) | Tasks 11-14 |
| Rollout Plan step 5 (metrics + logging) | Tasks 4, 6, 8 |
| Rollout Plan step 6 (tests + docs) | Tasks 5, 7, 9, 15 |

### Placeholder scan

No TODOs, TBDs, or "implement later" patterns found.

### Type consistency

- `CloudEventEmitter` — used consistently as the interface name
- `Operation` / `OperationCreate` / etc. — consistent across emitter.go and all Emit call sites
- `EmitOption` / `WithStatus` / `WithMessage` — consistent across emitter.go and all call sites
- `AsyncPublisher` — consistent between publisher.go and config.go
- `NoopEmitter` — consistent between noop.go, config.go, and test files
- `Config` / `RetryConfig` — consistent between config.go and main.go
- `Sender` interface — consistent between publisher.go and config.go
