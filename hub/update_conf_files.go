package hub

import (
	"fmt"
	"path/filepath"

	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/utils"
)

func (s *Server) UpdateConfFiles(streams step.OutStreams) error {
	if err := UpdateGpperfmonConf(streams, s.Target.MasterDataDir()); err != nil {
		return err
	}

	if err := UpdatePostgresqlConf(streams, s.TargetInitializeConfig.Master.Port, s.Target, s.Source); err != nil {
		return err
	}

	return nil
}

func UpdateGpperfmonConf(streams step.OutStreams, masterDataDir string) error {
	logDir := filepath.Join(masterDataDir, "gpperfmon", "logs")

	pattern := `^log_location = .*$`
	replacement := fmt.Sprintf("log_location = %s", logDir)

	// TODO: allow arbitrary whitespace around the = sign?
	cmd := execCommand(
		"sed",
		"-i.bak", // in-place substitution with .bak backup extension
		fmt.Sprintf(`s|%s|%s|`, pattern, replacement),
		filepath.Join(masterDataDir, "gpperfmon", "conf", "gpperfmon.conf"),
	)

	cmd.Stdout, cmd.Stderr = streams.Stdout(), streams.Stderr()
	return cmd.Run()
}

// oldTargetPort is the old port on which the target cluster was initialized.
// This is used to search the postgresql.conf rather than target.MasterPort()
// which has been changed to match the source cluster after updating the
// catalog in a previous substep.
func UpdatePostgresqlConf(streams step.OutStreams, oldTargetPort int, target *utils.Cluster, source *utils.Cluster) error {
	// NOTE: any additions of forward slashes (/) here require an update to the
	// sed script below
	pattern := fmt.Sprintf(`(^port[ \t]*=[ \t]*)%d([^0-9]|$)`, oldTargetPort)
	replacement := fmt.Sprintf(`\1%d\2`, source.MasterPort())

	path := filepath.Join(target.MasterDataDir(), "postgresql.conf")

	cmd := execCommand(
		"sed",
		"-E",     // use POSIX extended regexes
		"-i.bak", // in-place substitution with .bak backup extension
		fmt.Sprintf(`s/%s/%s/`, pattern, replacement),
		path,
	)

	cmd.Stdout, cmd.Stderr = streams.Stdout(), streams.Stderr()
	return cmd.Run()
}
