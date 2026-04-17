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
