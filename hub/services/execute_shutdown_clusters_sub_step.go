package services

import (
	"fmt"
	"io"
	"os/exec"

	"github.com/greenplum-db/gpupgrade/idl"

	"github.com/greenplum-db/gpupgrade/utils"
)

var stopClusterCmd = exec.Command
var isPostmasterRunningCmd = exec.Command

func StopCluster(stream messageSender, log io.Writer, cluster *utils.Cluster) error {
	err := IsPostmasterRunning(stream, log, cluster)
	if err != nil {
		return err
	}

	cmd := stopClusterCmd("bash", "-c",
		fmt.Sprintf("source %[1]s/../greenplum_path.sh && %[1]s/gpstop -a -d %[2]s",
			cluster.BinDir,
			cluster.MasterDataDir(),
		))

	mux := newMultiplexedStream(stream, log)
	cmd.Stdout = mux.NewStreamWriter(idl.Chunk_STDOUT)
	cmd.Stderr = mux.NewStreamWriter(idl.Chunk_STDERR)

	return cmd.Run()
}

func IsPostmasterRunning(stream messageSender, log io.Writer, cluster *utils.Cluster) error {
	cmd := isPostmasterRunningCmd("bash", "-c",
		fmt.Sprintf("pgrep -F %s/postmaster.pid",
			cluster.MasterDataDir(),
		))

	mux := newMultiplexedStream(stream, log)
	cmd.Stdout = mux.NewStreamWriter(idl.Chunk_STDOUT)
	cmd.Stderr = mux.NewStreamWriter(idl.Chunk_STDERR)

	return cmd.Run()
}
