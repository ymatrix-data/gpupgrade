package services_test

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/greenplum-db/gpupgrade/hub/services"
	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/utils"

	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ConvertMasterHub", func() {
	var (
		dir          string
		hub          *services.Hub
		clusterPair  *utils.ClusterPair
		cm           *testutils.MockChecklistManager
		actualCmdStr string
	)

	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())
		conf := &services.HubConfig{
			StateDir: dir,
		}

		clusterPair = testutils.CreateSampleClusterPair()
		cm = testutils.NewMockChecklistManager()
		hub = services.NewHub(clusterPair, grpc.DialContext, conf, cm)
		utils.System.RunCommandAsync = func(cmdStr string, logFile string) error {
			actualCmdStr = cmdStr
			return nil
		}
	})

	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
		os.RemoveAll(dir)
	})

	It("returns with no error when convert master runs successfully", func() {
		err := hub.ConvertMaster()
		Expect(err).ToNot(HaveOccurred())

		pgupgrade_dir := filepath.Join(dir, "pg_upgrade")
		Expect(actualCmdStr).To(Equal("unset PGHOST; unset PGPORT; cd " + pgupgrade_dir +
			` && nohup /new/bindir/pg_upgrade --old-bindir=/old/bindir ` +
			`--old-datadir=/old/datadir --new-bindir=/new/bindir ` +
			`--new-datadir=/new/datadir --old-port=25437 --new-port=35437 --dispatcher-mode --progress`))
	})

	It("returns an error when convert master fails", func() {
		utils.System.RunCommandAsync = func(cmdStr string, logFile string) error {
			return errors.New("upgrade failed")
		}

		err := hub.ConvertMaster()
		Expect(err).To(HaveOccurred())
	})

	It("returns an error if the upgrade directory cannot be created", func() {
		utils.System.MkdirAll = func(path string, perm os.FileMode) error {
			return errors.New("failed to create directory")
		}

		err := hub.ConvertMaster()
		Expect(err).To(HaveOccurred())
	})
})
