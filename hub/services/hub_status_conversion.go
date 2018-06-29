package services

import (
	"fmt"
	"strings"

	pb "github.com/greenplum-db/gpupgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/net/context"
)

func (h *Hub) StatusConversion(ctx context.Context, in *pb.StatusConversionRequest) (*pb.StatusConversionReply, error) {
	conns, err := h.AgentConns()
	if err != nil {
		return &pb.StatusConversionReply{}, err
	}

	segments := h.segmentsByHost()

	var statuses []string
	for _, conn := range conns {
		var agentSegments []*pb.SegmentInfo
		for _, segment := range segments[conn.Hostname] {
			agentSegments = append(agentSegments, &pb.SegmentInfo{
				Content: int32(segment.ContentID),
				Dbid:    int32(segment.DbID),
				DataDir: segment.DataDir,
			})
		}

		// TODO: allow the client to be mocked out.
		status, err := pb.NewAgentClient(conn.Conn).CheckConversionStatus(context.Background(), &pb.CheckConversionStatusRequest{
			Segments: agentSegments,
			Hostname: conn.Hostname,
		})
		if err != nil {
			return &pb.StatusConversionReply{}, fmt.Errorf("agent on host %s returned an error when checking conversion status: %s", conn.Hostname, err)
		}

		statuses = append(statuses, status.GetStatuses()...)
	}

	return &pb.StatusConversionReply{
		ConversionStatuses: statuses,
	}, nil
}

func PrimaryConversionStatus(hub *Hub) pb.StepStatus {
	// We can't determine the actual status if there's an error, so we log it and return PENDING
	conversionStatus, err := hub.StatusConversion(nil, &pb.StatusConversionRequest{})
	if err != nil {
		gplog.Error("Could not get primary conversion status: %s", err)
		return pb.StepStatus_PENDING
	}

	statuses := strings.Join(conversionStatus.GetConversionStatuses(), "\n")
	switch {
	case strings.Contains(statuses, "FAILED"):
		return pb.StepStatus_FAILED
	case strings.Contains(statuses, "RUNNING"):
		return pb.StepStatus_RUNNING
	case strings.Contains(statuses, "COMPLETE"):
		return pb.StepStatus_COMPLETE
	default:
		return pb.StepStatus_PENDING
	}
}
