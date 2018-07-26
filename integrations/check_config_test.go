package integrations_test

import (
	"path/filepath"

	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

// needs the cli and the hub
var _ = Describe("check config", func() {
	It("happy: the database configuration is saved to a specified location", func() {
		// `check config` needs a running DB. Since this is currently the only
		// test that does, skip it if the user doesn't have one up and running.
		//
		// TODO: see if we can either mock out the DB or move this to a true
		// end-to-end-plus-DB test
		{
			conn := dbconn.NewDBConnFromEnvironment("template1")
			err := conn.Connect(1)
			if err != nil {
				Skip("this test requires a running GPDB cluster")
			}
		}

		session := runCommand("check", "config")
		Expect(session).To(Exit(0))

		source := &utils.Cluster{ConfigPath: filepath.Join(testStateDir, utils.SOURCE_CONFIG_FILENAME)}
		err := source.Load()
		testutils.Check("cannot read config", err)

		Expect(len(source.Segments)).To(BeNumerically(">", 1))
	})
})
