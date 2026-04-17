package eventing_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/syntasso/kratix/internal/eventing"
)

var _ = Describe("NewEmitter", func() {
	BeforeEach(func() {
		eventing.ResetMetricsForTest()
	})

	It("returns a NoopEmitter when config is nil", func() {
		emitter, shutdown, err := eventing.NewEmitter(nil)
		Expect(err).NotTo(HaveOccurred())
		defer shutdown(context.Background())
		Expect(emitter).To(BeAssignableToTypeOf(&eventing.NoopEmitter{}))
	})

	It("returns a NoopEmitter when sink is empty", func() {
		emitter, shutdown, err := eventing.NewEmitter(&eventing.Config{Sink: ""})
		Expect(err).NotTo(HaveOccurred())
		defer shutdown(context.Background())
		Expect(emitter).To(BeAssignableToTypeOf(&eventing.NoopEmitter{}))
	})

	It("returns an error for an invalid retry strategy", func() {
		emitter, shutdown, err := eventing.NewEmitter(&eventing.Config{
			Sink:  "http://localhost:8080",
			Retry: eventing.RetryConfig{Strategy: "bogus"},
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unknown retry strategy"))
		Expect(emitter).To(BeNil())
		Expect(shutdown).To(BeNil())
	})

	It("returns an error for an invalid retry period", func() {
		emitter, shutdown, err := eventing.NewEmitter(&eventing.Config{
			Sink:  "http://localhost:8080",
			Retry: eventing.RetryConfig{Period: "not-a-duration"},
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid retry period"))
		Expect(emitter).To(BeNil())
		Expect(shutdown).To(BeNil())
	})
})
