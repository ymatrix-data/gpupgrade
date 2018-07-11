package integrations_test

import (
	"github.com/greenplum-db/gpupgrade/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

var _ = Describe("config set", func() {
	It("fails if no flags are passed", func() {
		configSetSession := runCommand("config", "set")
		Expect(configSetSession).Should(Exit(1))
		Expect(string(configSetSession.Err.Contents())).To(ContainSubstring("the set command requires exactly one flag to be specified"))
	})

	// XXX: Cobra handles this, so do we care to test this here?
	It("fails if an invalid flag is passed", func() {
		nonExistentFlag := "--no-existent-flag"
		configSetSession := runCommand("config", "set", nonExistentFlag, "foo")
		Expect(configSetSession).Should(Exit(1))
	})

	It("sets the old binary directory in the configuration file", func() {
		expected := "/my/amazing/old/bin/dir"
		configSetSession := runCommand("config", "set", "--old-bindir", expected)
		Expect(configSetSession).Should(Exit(0))

		clusters := &utils.ClusterPair{}
		clusters.Load(testStateDir)

		Expect(clusters.OldBinDir).To(Equal(expected))
	})

	It("sets the new binary directory in the configuration file", func() {
		expected := "/my/amazing/new/bin/dir"
		configSetSession := runCommand("config", "set", "--new-bindir", expected)
		Expect(configSetSession).Should(Exit(0))

		clusters := &utils.ClusterPair{}
		clusters.Load(testStateDir)

		Expect(clusters.NewBinDir).To(Equal(expected))
	})
})
