package commanders_test

import (
	"errors"

	"github.com/greenplum-db/gpupgrade/cli/commanders"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("reporter", func() {
	var (
		hubClient *testutils.MockHubClient
	)

	BeforeEach(func() {
		hubClient = testutils.NewMockHubClient()
	})

	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
	})

	Describe("Finalize", func() {
		It("returns nil error when hub reports success", func() {
			err := commanders.Finalize(hubClient)
			Expect(err).ToNot(HaveOccurred())

			Expect(hubClient.FinalizeRequest).To(Equal(&idl.FinalizeRequest{}))
		})

		It("returns error when hub reports failure", func() {
			hubClient.Err = errors.New("finalize failed")

			err := commanders.Finalize(hubClient)
			Expect(err).To(HaveOccurred())
		})
	})
})
