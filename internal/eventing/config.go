package eventing

import (
	"context"
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	ceclient "github.com/cloudevents/sdk-go/v2/client"
	cecontext "github.com/cloudevents/sdk-go/v2/context"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	ctrl "sigs.k8s.io/controller-runtime"
)

var configLog = ctrl.Log.WithName("eventing").WithName("config")

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

// NewEmitter creates the appropriate CloudEventEmitter based on config.
// It returns a shutdown function that should be called when the emitter is no longer needed.
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

// retrySenderWrapper decorates a CE client with retry params in the send context.
type retrySenderWrapper struct {
	client      ceclient.Client
	retryParams *cecontext.RetryParams
}

func (w *retrySenderWrapper) Send(ctx context.Context, e cloudevents.Event) protocol.Result {
	ctx = cecontext.WithRetryParams(ctx, w.retryParams)
	return w.client.Send(ctx, e)
}
