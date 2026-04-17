package eventing

import "sigs.k8s.io/controller-runtime/pkg/client"

type NoopEmitter struct{}

var _ CloudEventEmitter = (*NoopEmitter)(nil)

func (n *NoopEmitter) Emit(_ client.Object, _ Operation, _ ...EmitOption) {}
