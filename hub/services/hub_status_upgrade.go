package services

import (
	pb "github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/net/context"
)

func (h *Hub) StatusUpgrade(ctx context.Context, in *pb.StatusUpgradeRequest) (*pb.StatusUpgradeReply, error) {
	gplog.Info("starting StatusUpgrade")

	steps := h.checklist.AllSteps()
	statuses := make([]*pb.UpgradeStepStatus, len(steps))

	for i, step := range steps {
		gplog.Info("Checking %s...", step.Name())
		statuses[i] = &pb.UpgradeStepStatus{Step: step.Code(), Status: step.Status()}
	}

	return &pb.StatusUpgradeReply{
		ListOfUpgradeStepStatuses: statuses,
	}, nil
}

func GetPrepareNewClusterConfigStatus(statedir string) pb.StepStatus {
	/* Treat all stat failures as cannot find file. Conceal worse failures atm.*/
	_, err := utils.System.Stat(utils.GetNewConfigFilePath(statedir))

	if err != nil {
		gplog.Debug("%v", err)
		return pb.StepStatus_PENDING
	}

	return pb.StepStatus_COMPLETE
}
