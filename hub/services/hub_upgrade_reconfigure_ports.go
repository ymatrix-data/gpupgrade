package services

import (
	"context"
	"fmt"

	"github.com/greenplum-db/gpupgrade/hub/upgradestatus"
	pb "github.com/greenplum-db/gpupgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

const (
	SedAndMvString = "sed 's/port=%d/port=%d/' %[3]s/postgresql.conf > %[3]s/postgresql.conf.updated && " +
		"mv %[3]s/postgresql.conf %[3]s/postgresql.conf.bak && " +
		"mv %[3]s/postgresql.conf.updated %[3]s/postgresql.conf"
)

func (h *Hub) UpgradeReconfigurePorts(ctx context.Context, in *pb.UpgradeReconfigurePortsRequest) (*pb.UpgradeReconfigurePortsReply, error) {
	gplog.Info("Started processing reconfigure-ports request")

	step := h.checklist.StepWriter(upgradestatus.RECONFIGURE_PORTS)

	err := step.ResetStateDir()
	if err != nil {
		gplog.Error("error from ResetStateDir " + err.Error())
	}
	err = step.MarkInProgress()
	if err != nil {
		gplog.Error("error from MarkInProgress " + err.Error())
	}

	oldMasterPort, newMasterPort, newMasterDataDir := h.clusterPair.GetPortsAndDataDirForReconfiguration()
	sedCommand := fmt.Sprintf(SedAndMvString, newMasterPort, oldMasterPort, newMasterDataDir)
	gplog.Info("reconfigure-ports sed command: %+v", sedCommand)

	output, err := h.clusterPair.OldCluster.Executor.ExecuteLocalCommand(sedCommand)
	if err != nil {
		gplog.Error("reconfigure-ports failed %s: %s", output, err)

		step.MarkFailed()
		return nil, err
	}

	gplog.Info("reconfigure-ports succeeded")
	step.MarkComplete()

	return &pb.UpgradeReconfigurePortsReply{}, nil
}
