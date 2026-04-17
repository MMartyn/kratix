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
