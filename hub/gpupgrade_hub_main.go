package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"time"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpupgrade/hub/services"
	"github.com/greenplum-db/gpupgrade/hub/upgradestatus"
	pb "github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

// This directory to have the implementation code for the gRPC server to serve
// Minimal CLI command parsing to embrace that booting this binary to run the hub might have some flags like a log dir

func main() {
	var logdir string
	var daemonize bool
	var daemon bool
	var RootCmd = &cobra.Command{
		Use:   os.Args[0],
		Short: "Start the gpupgrade_hub (blocks)",
		Long:  `Start the gpupgrade_hub (blocks)`,
		RunE: func(cmd *cobra.Command, args []string) error {
			gplog.InitializeLogging("gpupgrade_hub", logdir)
			debug.SetTraceback("all")

			if daemon && terminal.IsTerminal(int(os.Stdout.Fd())) {
				// Shouldn't be calling this from the command line.
				return fmt.Errorf("--daemon is an internal option (did you mean --daemonize?)")
			}

			if daemonize {
				// Strip out the --daemonize option and add --daemon.
				daemonArgs := make([]string, 0)
				for _, arg := range os.Args[1:] {
					if arg == "--daemonize" {
						arg = "--daemon"
					}
					daemonArgs = append(daemonArgs, arg)
				}

				command := exec.Command(os.Args[0], daemonArgs...)
				// TODO: what's a good timeout?
				err := utils.Daemonize(command, os.Stdout, os.Stderr, 2*time.Second)

				if err != nil {
					exitError, ok := err.(*exec.ExitError)
					if ok {
						// Exit with the same code as the child, if we can
						// figure it out.
						code := 1

						status, ok := exitError.Sys().(syscall.WaitStatus)
						if ok {
							code = status.ExitStatus()
						}

						os.Exit(code)
					}

					// Otherwise, deal with the error normally.
				}

				return err
			}

			conf := &services.HubConfig{
				CliToHubPort:   7527,
				HubToAgentPort: 6416,
				StateDir:       utils.GetStateDir(),
				LogDir:         logdir,
			}
			cp := &utils.ClusterPair{}
			cm := upgradestatus.NewChecklistManager(conf.StateDir)

			hub := services.NewHub(cp, grpc.DialContext, conf, cm)

			// TODO: make sure the implementations here, and the Checklist below, are
			// fully exercised in end-to-end tests. It feels like we should be able to
			// pull these into a Hub method or helper function, but currently the
			// interfaces aren't well componentized.
			stateCheck := func(step upgradestatus.StateReader) pb.StepStatus {
				checker := upgradestatus.StateCheck{
					Path: filepath.Join(conf.StateDir, step.Name()),
					Step: step.Code(),
				}
				return checker.GetStatus()
			}

			initStatus := func(step upgradestatus.StateReader) pb.StepStatus {
				return services.GetPrepareNewClusterConfigStatus(conf.StateDir)
			}

			shutDownStatus := func(step upgradestatus.StateReader) pb.StepStatus {
				// TODO: get rid of the "helper struct" layer here; it's not
				// getting us much.
				shutDownClusterPath := filepath.Join(conf.StateDir, step.Name())
				checker := upgradestatus.NewShutDownClusters(shutDownClusterPath, cp.OldCluster.Executor)
				return checker.GetStatus()
			}

			convertMasterStatus := func(step upgradestatus.StateReader) pb.StepStatus {
				// TODO: get rid of the "helper struct" layer here; it's not
				// getting us much.
				convertMasterPath := filepath.Join(conf.StateDir, step.Name())
				oldDataDir := cp.OldCluster.GetDirForContent(-1)
				checker := upgradestatus.NewPGUpgradeStatusChecker(upgradestatus.MASTER, convertMasterPath, oldDataDir, cp.OldCluster.Executor)
				return checker.GetStatus()
			}

			convertPrimariesStatus := func(step upgradestatus.StateReader) pb.StepStatus {
				return services.PrimaryConversionStatus(hub)
			}

			cm.LoadSteps([]upgradestatus.Step{
				{upgradestatus.CONFIG, pb.UpgradeSteps_CHECK_CONFIG, stateCheck},
				{upgradestatus.SEGINSTALL, pb.UpgradeSteps_SEGINSTALL, stateCheck},
				{upgradestatus.INIT_CLUSTER, pb.UpgradeSteps_PREPARE_INIT_CLUSTER, initStatus},
				{upgradestatus.SHUTDOWN_CLUSTERS, pb.UpgradeSteps_STOPPED_CLUSTER, shutDownStatus},
				{upgradestatus.CONVERT_MASTER, pb.UpgradeSteps_MASTERUPGRADE, convertMasterStatus},
				{upgradestatus.START_AGENTS, pb.UpgradeSteps_PREPARE_START_AGENTS, stateCheck},
				{upgradestatus.SHARE_OIDS, pb.UpgradeSteps_SHARE_OIDS, stateCheck},
				{upgradestatus.VALIDATE_START_CLUSTER, pb.UpgradeSteps_VALIDATE_START_CLUSTER, stateCheck},
				{upgradestatus.CONVERT_PRIMARY, pb.UpgradeSteps_CONVERT_PRIMARIES, convertPrimariesStatus},
				{upgradestatus.RECONFIGURE_PORTS, pb.UpgradeSteps_RECONFIGURE_PORTS, stateCheck},
			})

			if daemon {
				hub.MakeDaemon()
			}

			hub.Start()

			hub.Stop()

			return nil
		},
	}

	RootCmd.PersistentFlags().StringVar(&logdir, "log-directory", "", "gpupgrade_hub log directory")

	RootCmd.Flags().BoolVar(&daemonize, "daemonize", false, "start hub in the background")
	RootCmd.Flags().BoolVar(&daemon, "daemon", false, "disconnect standard streams (internal option; use --daemonize instead)")
	RootCmd.Flags().MarkHidden("daemon")

	if err := RootCmd.Execute(); err != nil {
		if gplog.GetLogger() == nil {
			// In case we didn't get through RootCmd.Execute(), set up logging
			// here. Otherwise we crash.
			// XXX it'd be really nice to have a "ReinitializeLogging" building
			// block somewhere.
			gplog.InitializeLogging("gpupgrade_hub", "")
		}

		gplog.Error(err.Error())
		os.Exit(1)
	}
}
