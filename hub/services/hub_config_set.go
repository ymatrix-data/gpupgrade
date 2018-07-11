package services

import (
	pb "github.com/greenplum-db/gpupgrade/idl"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/net/context"
)

func (h *Hub) ConfigSet(ctx context.Context, in *pb.ConfigSetRequest) (*pb.ConfigSetReply, error) {
	switch in.FlagName {
	case "old-bindir":
		h.clusterPair.OldBinDir = in.FlagVal
	case "new-bindir":
		h.clusterPair.NewBinDir = in.FlagVal
	default:
		return nil, status.Errorf(codes.NotFound, "%s is not a valid configuration key", in.FlagName)
	}

	// Persist.
	h.clusterPair.Commit(h.conf.StateDir)

	gplog.Info("Successfully set %s to %s", in.FlagName, in.FlagVal)
	return &pb.ConfigSetReply{}, nil
}

func (h *Hub) GetConfig(ctx context.Context, in *pb.GetConfigRequest) (*pb.GetConfigReply, error) {
	resp := &pb.GetConfigReply{}

	switch in.Name {
	case "old-bindir":
		resp.Value = h.clusterPair.OldBinDir
	case "new-bindir":
		resp.Value = h.clusterPair.NewBinDir
	default:
		return nil, status.Errorf(codes.NotFound, "%s is not a valid configuration key", in.Name)
	}

	return resp, nil
}
