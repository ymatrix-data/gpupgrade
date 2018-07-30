package utils_test

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cluster", func() {
	var (
		expectedCluster *utils.Cluster
		testStateDir    string
		err             error
	)

	BeforeEach(func() {
		testStateDir, err = ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		testhelper.SetupTestLogger()
		expectedCluster = &utils.Cluster{
			Cluster:    testutils.CreateMultinodeSampleCluster("/tmp"),
			BinDir:     "/fake/path",
			ConfigPath: path.Join(testStateDir, "cluster_config.json"),
		}
	})

	AfterEach(func() {
		os.RemoveAll(testStateDir)
	})

	Describe("Commit and Load", func() {
		It("can save a config and successfully load it back in", func() {
			err := expectedCluster.Commit()
			Expect(err).ToNot(HaveOccurred())
			givenCluster := &utils.Cluster{
				ConfigPath: path.Join(testStateDir, "cluster_config.json"),
			}
			err = givenCluster.Load()
			Expect(err).ToNot(HaveOccurred())

			// We don't serialize the Executor
			givenCluster.Executor = expectedCluster.Executor

			Expect(expectedCluster).To(Equal(givenCluster))
		})
	})

	Describe("PrimaryHostnames", func() {
		It("returns a list of hosts for only the primaries", func() {
			hostnames := expectedCluster.PrimaryHostnames()
			Expect(hostnames).To(ConsistOf([]string{"host1", "host2"}))
		})
	})

	Describe("SegmentsOn", func() {
		It("returns an error for an unknown hostname", func() {
			c := utils.Cluster{Cluster: &cluster.Cluster{}}
			_, err := c.SegmentsOn("notahost")
			Expect(err).To(HaveOccurred())
		})

		It("maps all hosts to segment configurations", func() {
			expected := map[string][]cluster.SegConfig{
				"localhost": {expectedCluster.Segments[-1]},
				"host1":     {expectedCluster.Segments[0]},
				"host2":     {expectedCluster.Segments[1]},
			}
			for host, expectedSegments := range expected {
				segments, err := expectedCluster.SegmentsOn(host)
				Expect(err).NotTo(HaveOccurred())
				Expect(segments).To(ConsistOf(expectedSegments))
			}
		})

		It("groups all segments by hostname", func() {
			c := utils.Cluster{
				Cluster: &cluster.Cluster{
					ContentIDs: []int{-1, 0, 1},
					Segments: map[int]cluster.SegConfig{
						-1: {ContentID: -1, DbID: 1, Port: 15432, Hostname: "mdw", DataDir: "/seg-1"},
						0:  {ContentID: 0, DbID: 2, Port: 25432, Hostname: "sdw1", DataDir: "/seg1"},
						1:  {ContentID: 1, DbID: 3, Port: 25433, Hostname: "sdw1", DataDir: "/seg2"},
					},
				},
			}

			expected := map[string][]cluster.SegConfig{
				"mdw":  {c.Segments[-1]},
				"sdw1": {c.Segments[0], c.Segments[1]},
			}
			for host, expectedSegments := range expected {
				segments, err := c.SegmentsOn(host)
				Expect(err).NotTo(HaveOccurred())
				Expect(segments).To(ConsistOf(expectedSegments))
			}
		})
	})
})
