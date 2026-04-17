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
