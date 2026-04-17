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

// Sender wraps the CloudEvents client's Send method for injection and testing.
type Sender interface {
	Send(ctx context.Context, e cloudevents.Event) protocol.Result
}

type envelope struct {
	event cloudevents.Event
}

// AsyncPublisher enqueues CloudEvents and sends them from background workers.
type AsyncPublisher struct {
	sender  Sender
	source  string
	queue   chan envelope
	wg      sync.WaitGroup
	workers int
}

var _ CloudEventEmitter = (*AsyncPublisher)(nil)

// NewAsyncPublisher constructs an AsyncPublisher. Call Start before Emit and Shutdown to stop.
func NewAsyncPublisher(sender Sender, source string, queueSize, workerCount int) *AsyncPublisher {
	return &AsyncPublisher{
		sender:  sender,
		source:  source,
		queue:   make(chan envelope, queueSize),
		workers: workerCount,
	}
}

// Start launches workerCount goroutines that drain the queue until Shutdown closes it.
func (p *AsyncPublisher) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
	publisherLog.Info("async publisher started", "workers", p.workers, "queueSize", cap(p.queue))
}

// Shutdown closes the queue and waits for workers to finish, bounded by ctx.
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

// Emit builds a CloudEvent and enqueues it non-blocking; drops when the queue is full.
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
