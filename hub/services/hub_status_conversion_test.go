package services_test

import (
	"errors"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gpupgrade/testutils"

	"google.golang.org/grpc"

	pb "github.com/greenplum-db/gpupgrade/idl"

	"github.com/greenplum-db/gpupgrade/hub/services"

	"github.com/greenplum-db/gpupgrade/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("hub", func() {
	var (
		hub         *services.Hub
		agentA      *testutils.MockAgentServer
		clusterPair *utils.ClusterPair
		cm          *testutils.MockChecklistManager
	)

	BeforeEach(func() {
		var port int
		agentA, port = testutils.NewMockAgentServer()

		clusterPair = &utils.ClusterPair{
			OldCluster: &cluster.Cluster{
				Segments: map[int]cluster.SegConfig{
					0: {DbID: 2, ContentID: 0, Hostname: "localhost", DataDir: "/first/data/dir"},
					1: {DbID: 3, ContentID: 1, Hostname: "localhost", DataDir: "/second/data/dir"},
				},
			},
		}
		conf := &services.HubConfig{
			HubToAgentPort: port,
		}

		cm = testutils.NewMockChecklistManager()
		hub = services.NewHub(clusterPair, grpc.DialContext, conf, cm)
	})

	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
		agentA.Stop()
	})

	It("receives conversion statuses from the agent and returns all as single message", func() {
		statusMessages := []string{"status", "status"}
		agentA.StatusConversionResponse = &pb.CheckConversionStatusReply{
			Statuses: statusMessages,
		}

		status, err := hub.StatusConversion(nil, &pb.StatusConversionRequest{})
		Expect(err).ToNot(HaveOccurred())

		Expect(status.GetConversionStatuses()).To(Equal([]string{"status", "status"}))
		Expect(agentA.StatusConversionRequest.GetHostname()).To(Equal("localhost"))
		Expect(agentA.StatusConversionRequest.GetSegments()).To(ConsistOf([]*pb.SegmentInfo{
			{
				Content: 0,
				Dbid:    2,
				DataDir: "/first/data/dir",
			},
			{
				Content: 1,
				Dbid:    3,
				DataDir: "/second/data/dir",
			},
		}))
	})

	It("returns an error when Agent server returns an error", func() {
		agentA.Err <- errors.New("any error")

		_, err := hub.StatusConversion(nil, &pb.StatusConversionRequest{})
		Expect(err).To(HaveOccurred())
	})

	Describe("PrimaryConversionStatus", func() {
		It("returns FAILED if any agents report failure", func() {
			agentA.StatusConversionResponse = &pb.CheckConversionStatusReply{
				Statuses: []string{"FAILED", "RUNNING"},
			}

			status := services.PrimaryConversionStatus(hub)
			Expect(status).To(Equal(pb.StepStatus_FAILED))
		})

		It("returns RUNNING if any agents report progress", func() {
			agentA.StatusConversionResponse = &pb.CheckConversionStatusReply{
				Statuses: []string{"COMPLETE", "RUNNING"},
			}

			status := services.PrimaryConversionStatus(hub)
			Expect(status).To(Equal(pb.StepStatus_RUNNING))
		})

		It("returns COMPLETE if all agents report completion", func() {
			agentA.StatusConversionResponse = &pb.CheckConversionStatusReply{
				Statuses: []string{"COMPLETE", "COMPLETE"},
			}

			status := services.PrimaryConversionStatus(hub)
			Expect(status).To(Equal(pb.StepStatus_COMPLETE))
		})

		It("returns PENDING if no agents report any other state", func() {
			agentA.StatusConversionResponse = &pb.CheckConversionStatusReply{
				Statuses: []string{"PENDING", "PENDING"},
			}

			status := services.PrimaryConversionStatus(hub)
			Expect(status).To(Equal(pb.StepStatus_PENDING))
		})

		It("returns PENDING if the status is not retrievable", func() {
			agentA.Err <- errors.New("any error")

			status := services.PrimaryConversionStatus(hub)
			Expect(status).To(Equal(pb.StepStatus_PENDING))
		})
	})
})
