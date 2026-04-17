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
