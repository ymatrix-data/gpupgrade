package hub

import (
	"fmt"
	"path/filepath"

	"github.com/greenplum-db/gpupgrade/utils"
)

func (s *Server) UpdateConfFiles() error {
	if err := UpdateGpperfmonConf(s.Target.MasterDataDir()); err != nil {
		return err
	}

	if err := UpdatePostgresqlConf(s.TargetInitializeConfig.Master.Port, s.Target, s.Source); err != nil {
		return err
	}

	return nil
}

func UpdateGpperfmonConf(masterDataDir string) error {
	configFile := filepath.Join(masterDataDir, "gpperfmon", "conf", "gpperfmon.conf")
	logDir := filepath.Join(masterDataDir, "gpperfmon", "logs")
	replacement := fmt.Sprintf("log_location = %s", logDir)

	// TODO: allow arbitrary whitespace around the = sign?
	return ReplaceStringWithinFile(
		"^log_location = .*$",
		replacement,
		configFile,
	)
}

// oldTargetPort is the old port on which the target cluster was initialized.
// This is used to search the postgresql.conf rather than target.MasterPort()
// which has been changed to match the source cluster after updating the
// catalog in a previous substep.
func UpdatePostgresqlConf(oldTargetPort int, target *utils.Cluster, source *utils.Cluster) error {
	return ReplaceStringWithinFile(
		fmt.Sprintf(`(^port[ \t]*=[ \t]*)%d([^0-9]|$)`, oldTargetPort),
		fmt.Sprintf(`\1%d\2`, source.MasterPort()),
		filepath.Join(target.MasterDataDir(), "postgresql.conf"),
	)
}
