package utils_test

import (
	"io/ioutil"
	"os"

	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ClusterPair", func() {
	var (
		filesLaidDown []string
		clusterPair   *utils.ClusterPair
		testExecutor  *testhelper.TestExecutor
		testStateDir  string
		err           error
	)

	BeforeEach(func() {
		testStateDir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		testhelper.SetupTestLogger()
		testExecutor = &testhelper.TestExecutor{}
		clusterPair = testutils.CreateSampleClusterPair()
		clusterPair.OldBinDir = "old/path"
		clusterPair.NewBinDir = "new/path"
		clusterPair.OldCluster.Executor = testExecutor
	})

	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
		filesLaidDown = []string{}
	})

	Describe("WriteClusterConfig", func() {
		It("successfully write cluster config to disk if no file exists", func() {
			sampleCluster := testutils.CreateSampleCluster(-1, 25437, "hostone", "/old/datadir")
			configFilePath := utils.GetConfigFilePath(testStateDir)
			err := utils.WriteClusterConfig(configFilePath, sampleCluster, "/old/bin/dir")
			Expect(err).ToNot(HaveOccurred())

			_, err = os.Open(configFilePath)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	It("can commit and load to/from disk", func() {
		expected := testutils.CreateMultinodeSampleClusterPair("/tmp")
		expected.Commit(testStateDir)

		actual := &utils.ClusterPair{}
		actual.Load(testStateDir)

		// Executors aren't serialized, so copy them over for ease of testing
		actual.OldCluster.Executor = expected.OldCluster.Executor
		actual.NewCluster.Executor = expected.NewCluster.Executor
		Expect(actual).To(Equal(expected))
	})
})
