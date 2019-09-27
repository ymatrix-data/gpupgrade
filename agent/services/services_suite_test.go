package services_test

import (
	"github.com/greenplum-db/gpupgrade/testutils/exectest"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCommands(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Services Suite")
}

// Enable exectest.NewCommand mocking.
func TestMain(m *testing.M) {
	os.Exit(exectest.Run(m))
}
