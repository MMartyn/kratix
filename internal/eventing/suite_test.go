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
