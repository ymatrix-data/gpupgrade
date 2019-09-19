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
		hubClient    *testutils.MockHubClient
		upgrader     *commanders.Upgrader
	)

	BeforeEach(func() {
		hubClient = testutils.NewMockHubClient()
		upgrader = commanders.NewUpgrader(hubClient)
	})

	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
	})

	Describe("ReconfigurePorts", func() {
		It("returns nil error when ports are reconfigured successfully", func() {
			err := upgrader.ReconfigurePorts()
			Expect(err).ToNot(HaveOccurred())

			Expect(hubClient.UpgradeReconfigurePortsRequest).To(Equal(&idl.UpgradeReconfigurePortsRequest{}))
		})

		It("returns error when ports cannot be reconfigured", func() {
			hubClient.Err = errors.New("reconfigure ports failed")

			err := upgrader.ReconfigurePorts()
			Expect(err).To(HaveOccurred())
		})
	})
})
