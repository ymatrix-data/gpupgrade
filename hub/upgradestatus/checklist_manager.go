package upgradestatus

import (
	"os"
	"path"
	"path/filepath"

	"github.com/greenplum-db/gpupgrade/utils"
)

const (
	CONFIG                 = "check-config"
	VERSION                = "check-version"
	SEGINSTALL             = "check-seginstall"
	START_AGENTS           = "start-agents"
	INIT_CLUSTER           = "init-cluster"
	SHUTDOWN_CLUSTERS      = "shutdown-clusters"
	CONVERT_MASTER         = "convert-master"
	SHARE_OIDS             = "share-oids"
	CONVERT_PRIMARY        = "convert-primary"
	VALIDATE_START_CLUSTER = "validate-start-cluster"
	RECONFIGURE_PORTS      = "reconfigure-ports"
)

const (
	fs_inprogress = "in.progress"
	fs_failed     = "failed"
	fs_completed  = "completed"
)

type StateWriter interface {
	MarkInProgress() error
	ResetStateDir() error
	MarkFailed() error
	MarkComplete() error
}

type ChecklistManager struct {
	pathToStateDir string
}

func NewChecklistManager(stateDirPath string) *ChecklistManager {
	return &ChecklistManager{
		pathToStateDir: stateDirPath,
	}
}

func (c *ChecklistManager) StepWriter(step string) StateWriter {
	stepdir := filepath.Join(c.pathToStateDir, step)
	return StepWriter{stepdir: stepdir}
}

type StepWriter struct {
	stepdir string // path to step-specific state directory
}

// FIXME: none of these operations are atomic on the FS; just move the progress
// file from name to name instead
func (sw StepWriter) MarkFailed() error {
	err := utils.System.Remove(filepath.Join(sw.stepdir, fs_inprogress))
	if err != nil {
		return err
	}

	_, err = utils.System.OpenFile(path.Join(sw.stepdir, fs_failed), os.O_CREATE, 0700)
	if err != nil {
		return err
	}

	return nil
}

func (sw StepWriter) MarkComplete() error {
	err := utils.System.Remove(filepath.Join(sw.stepdir, fs_inprogress))
	if err != nil {
		return err
	}

	_, err = utils.System.OpenFile(path.Join(sw.stepdir, fs_completed), os.O_CREATE, 0700)
	if err != nil {
		return err
	}

	return nil
}

func (sw StepWriter) MarkInProgress() error {
	_, err := utils.System.OpenFile(path.Join(sw.stepdir, fs_inprogress), os.O_CREATE, 0700)
	if err != nil {
		return err
	}

	return nil
}

func (sw StepWriter) ResetStateDir() error {
	err := utils.System.RemoveAll(sw.stepdir)
	if err != nil {
		return err
	}

	err = utils.System.MkdirAll(sw.stepdir, 0700)
	if err != nil {
		return err
	}

	return nil
}
