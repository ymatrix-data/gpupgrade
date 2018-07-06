package upgradestatus

import (
	"github.com/greenplum-db/gpupgrade/hub/upgradestatus/file"
	pb "github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func ClusterShutdownStatus(gpstopStatePath string, execer cluster.Executor) pb.StepStatus {
	_, err := utils.System.Stat(gpstopStatePath)
	switch {
	case utils.System.IsNotExist(err):
		return pb.StepStatus_PENDING

	/* There can be cases where gpstop is running but not as part of the pre-setup
	 * in which case, we shouldn't be detecting that as a running state.
	 * We only care if the inprogress file exists. We are relying on the hub to never go down
	 * for this state processing to work.
	 */
	case isGpstopRunning(execer) && stopProgressFilesExist(gpstopStatePath):
		return pb.StepStatus_RUNNING

	case !stopProgressFilesExist(gpstopStatePath) && isStopComplete(gpstopStatePath):
		return pb.StepStatus_COMPLETE

	default:
		return pb.StepStatus_FAILED
	}
}

// XXX This is all pretty much a copy-paste from PGUpgradeStatusChecker... is
// there a way to consolidate?

func isGpstopRunning(execer cluster.Executor) bool {
	//if pgrep doesnt find target, ExecCmdOutput will return empty byte array and err.Error()="exit status 1"
	pgUpgradePids, err := execer.ExecuteLocalCommand("pgrep -f gpstop")
	if err == nil && len(pgUpgradePids) != 0 {
		return true
	}
	return false
}

func stopProgressFilesExist(gpstopStatePath string) bool {
	files, err := utils.System.FilePathGlob(gpstopStatePath + "/*/" + file.InProgress)
	if files == nil {
		return false
	}

	if err != nil {
		gplog.Error("err is: ", err)
		return false
	}

	return true
}

func isStopComplete(gpstopStatePath string) bool {
	completeFiles, completeErr := utils.System.FilePathGlob(gpstopStatePath + "/*/" + file.Complete)
	if completeFiles == nil {
		return false
	}

	if completeErr != nil {
		gplog.Error(completeErr.Error())
		return false
	}

	/* There should only be two completed files.
	 * One for gpstop.old and one for gpstop.new
	 */
	if len(completeFiles) == 2 {
		return true
	}

	return false
}
