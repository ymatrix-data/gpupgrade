package agent

import (
	"context"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func (s *Server) CreateSegmentDataDirectories(ctx context.Context, in *idl.CreateSegmentDataDirRequest) (*idl.CreateSegmentDataDirReply, error) {
	gplog.Info("got a request to create segment data directories from the hub")

	datadirs := in.Datadirs
	for _, segDataDir := range datadirs {
		err := utils.CreateDataDirectory(segDataDir)
		if err != nil {
			return &idl.CreateSegmentDataDirReply{}, err
		}

	}
	return &idl.CreateSegmentDataDirReply{}, nil
}
