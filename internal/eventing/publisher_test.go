package eventing_test

import (
	"context"
	"encoding/json"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/protocol"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
		eventing.ResetMetricsForTest()
	})

	It("sends a CloudEvent with correct attributes for a create operation", func() {
		sender := &fakeSender{
			events: make(chan cloudevents.Event, 10),
			result: nil,
		}
		pub := eventing.NewAsyncPublisher(sender, "https://kratix.example/controller-manager", 10, 1)
		pub.Start(ctx)
		defer pub.Shutdown(context.Background())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "my-pod",
				Labels: map[string]string{
					"app": "test",
					"env": "staging",
				},
			},
		}
		pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))

		pub.Emit(pod, eventing.OperationCreate)

		var sent cloudevents.Event
		Eventually(sender.events, 2*time.Second).Should(Receive(&sent))

		Expect(sent.Type()).To(Equal("io.kratix.pod.create.v1"))
		Expect(sent.Source()).To(Equal("https://kratix.example/controller-manager"))
		Expect(sent.Subject()).To(Equal("Pod/default/my-pod"))

		var payload map[string]any
		Expect(json.Unmarshal(sent.Data(), &payload)).To(Succeed())
		resource, ok := payload["resource"].(map[string]any)
		Expect(ok).To(BeTrue())
		labels, ok := resource["labels"].(map[string]any)
		Expect(ok).To(BeTrue(), "resource should contain labels")
		Expect(labels).To(HaveKeyWithValue("app", "test"))
		Expect(labels).To(HaveKeyWithValue("env", "staging"))
	})

	It("includes status in the payload when WithStatus is used", func() {
		sender := &fakeSender{
			events: make(chan cloudevents.Event, 10),
			result: nil,
		}
		pub := eventing.NewAsyncPublisher(sender, "https://kratix.example/controller-manager", 10, 1)
		pub.Start(ctx)
		defer pub.Shutdown(context.Background())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "my-pod",
			},
		}
		pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))

		statusData := map[string]any{"phase": "Pending"}
		pub.Emit(pod, eventing.OperationStatusChange, eventing.WithStatus(statusData))

		var sent cloudevents.Event
		Eventually(sender.events, 2*time.Second).Should(Receive(&sent))

		Expect(sent.Type()).To(Equal("io.kratix.pod.status-change.v1"))

		var payload map[string]any
		Expect(json.Unmarshal(sent.Data(), &payload)).To(Succeed())
		Expect(payload).To(HaveKey("status"))
		status, ok := payload["status"].(map[string]any)
		Expect(ok).To(BeTrue(), "status should decode as a JSON object")
		Expect(status).To(HaveKeyWithValue("phase", "Pending"))

		resource, ok := payload["resource"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(resource).NotTo(HaveKey("labels"), "labels should be omitted when none exist")
	})

	It("drops events when the queue is full instead of blocking", func() {
		sender := &fakeSender{
			events: make(chan cloudevents.Event, 10),
			result: nil,
		}
		pub := eventing.NewAsyncPublisher(sender, "https://kratix.example/controller-manager", 1, 0)
		pub.Start(ctx)
		defer pub.Shutdown(context.Background())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "my-pod",
			},
		}
		pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))

		pub.Emit(pod, eventing.OperationCreate)

		start := time.Now()
		pub.Emit(pod, eventing.OperationCreate)
		Expect(time.Since(start)).To(BeNumerically("<", 1*time.Second))
	})
})
