package main

import (
	"context"
	"fmt"
	"os"

	"github.com/greenplum-db/gpupgrade/cli/commanders"
	pb "github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
)

var root = &cobra.Command{Use: "gpupgrade"}

var prepare = &cobra.Command{
	Use:   "prepare",
	Short: "subcommands to help you get ready for a gpupgrade",
	Long:  "subcommands to help you get ready for a gpupgrade",
}

var status = &cobra.Command{
	Use:   "status",
	Short: "subcommands to show the status of a gpupgrade",
	Long:  "subcommands to show the status of a gpupgrade",
}

var check = &cobra.Command{
	Use:   "check",
	Short: "collects information and validates the target Greenplum installation can be upgraded",
	Long:  `collects information and validates the target Greenplum installation can be upgraded`,
}

var version = &cobra.Command{
	Use:   "version",
	Short: "Version of gpupgrade",
	Long:  `Version of gpupgrade`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(commanders.VersionString())
	},
}

var upgrade = &cobra.Command{
	Use:   "upgrade",
	Short: "starts upgrade process",
	Long:  `starts upgrade process`,
}

var config = &cobra.Command{
	Use:   "config",
	Short: "subcommands to set parameters for subsequent gpupgrade commands",
	Long:  "subcommands to set parameters for subsequent gpupgrade commands",
}

var subStartHub = &cobra.Command{
	Use:   "start-hub",
	Short: "starts the hub",
	Long:  "starts the hub",
	Run: func(cmd *cobra.Command, args []string) {
		preparer := commanders.Preparer{}
		err := preparer.StartHub()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}

		conn, connConfigErr := grpc.Dial("localhost:"+hubPort, grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}
		client := pb.NewCliToHubClient(conn)
		err = preparer.VerifyConnectivity(client)

		if err != nil {
			gplog.Error("gpupgrade is unable to connect via gRPC to the hub")
			gplog.Error("%v", err)
			os.Exit(1)
		}
	},
}

var subShutdownClusters = &cobra.Command{
	Use:   "shutdown-clusters",
	Short: "shuts down both old and new cluster",
	Long:  "Current assumptions is both clusters exist.",
	Run: func(cmd *cobra.Command, args []string) {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort, grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}
		client := pb.NewCliToHubClient(conn)
		preparer := commanders.NewPreparer(client)
		err := preparer.ShutdownClusters()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}
	},
}

var subStartAgents = &cobra.Command{
	Use:   "start-agents",
	Short: "start agents on segment hosts",
	Long:  "start agents on all segments",
	Run: func(cmd *cobra.Command, args []string) {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort, grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}
		client := pb.NewCliToHubClient(conn)
		preparer := commanders.NewPreparer(client)
		err := preparer.StartAgents()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}
	},
}

var subInitCluster = &cobra.Command{
	Use:   "init-cluster",
	Short: "inits the cluster",
	Long:  "Current assumptions is that the cluster already exists. And will only generate json config file for now.",
	Run: func(cmd *cobra.Command, args []string) {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort, grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}
		client := pb.NewCliToHubClient(conn)
		preparer := commanders.NewPreparer(client)
		err := preparer.InitCluster()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}
	},
}

func createSetSubcommand() *cobra.Command {
	subSet := &cobra.Command{
		Use:   "set",
		Short: "set an upgrade parameter",
		Long:  "set an upgrade parameter",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().NFlag() == 0 {
				return errors.New("the set command requires at least one flag to be specified")
			}

			conn, connConfigErr := grpc.Dial("localhost:"+hubPort, grpc.WithInsecure())
			if connConfigErr != nil {
				return connConfigErr
			}
			client := pb.NewCliToHubClient(conn)

			var requests []*pb.ConfigSetRequest
			cmd.Flags().Visit(func(flag *pflag.Flag) {
				requests = append(requests, &pb.ConfigSetRequest{
					FlagName: flag.Name,
					FlagVal:  flag.Value.String(),
				})
			})

			for _, request := range requests {
				_, err := client.ConfigSet(context.Background(), request)
				if err != nil {
					return err
				}
				gplog.Info("Successfully set %s to %s", request.FlagName, request.FlagVal)
			}

			return nil
		},
	}

	subSet.Flags().String("old-bindir", "", "install directory for old gpdb version")
	subSet.Flags().String("new-bindir", "", "install directory for new gpdb version")

	return subSet
}

func createShowSubcommand() *cobra.Command {
	subShow := &cobra.Command{
		Use:   "show",
		Short: "show configuration settings",
		Long:  "show configuration settings",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, connConfigErr := grpc.Dial("localhost:"+hubPort, grpc.WithInsecure())
			if connConfigErr != nil {
				return connConfigErr
			}

			client := pb.NewCliToHubClient(conn)

			// Build a list of GetConfigRequests, one for each flag. If no flags
			// are passed, assume we want to retrieve all of them.
			var requests []*pb.GetConfigRequest
			getRequest := func(flag *pflag.Flag) {
				if flag.Name != "help" {
					requests = append(requests, &pb.GetConfigRequest{
						Name: flag.Name,
					})
				}
			}

			if cmd.Flags().NFlag() > 0 {
				cmd.Flags().Visit(getRequest)
			} else {
				cmd.Flags().VisitAll(getRequest)
			}

			// Make the requests and print every response.
			for _, request := range requests {
				resp, err := client.GetConfig(context.Background(), request)
				if err != nil {
					return err
				}

				if cmd.Flags().NFlag() == 1 {
					// Don't prefix with the setting name if the user only asked for one.
					fmt.Println(resp.Value)
				} else {
					fmt.Printf("%s - %s\n", request.Name, resp.Value)
				}
			}

			return nil
		},
	}

	subShow.Flags().Bool("old-bindir", false, "show install directory for old gpdb version")
	subShow.Flags().Bool("new-bindir", false, "show install directory for new gpdb version")

	return subShow
}

var subUpgrade = &cobra.Command{
	Use:   "upgrade",
	Short: "the status of the upgrade",
	Long:  "the status of the upgrade",
	Run: func(cmd *cobra.Command, args []string) {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort, grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}
		client := pb.NewCliToHubClient(conn)
		reporter := commanders.NewReporter(client)
		err := reporter.OverallUpgradeStatus()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}
	},
}

var subVersion = &cobra.Command{
	Use:     "version",
	Short:   "validate current version is upgradable",
	Long:    `validate current version is upgradable`,
	Aliases: []string{"ver"},
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
			grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}
		client := pb.NewCliToHubClient(conn)
		return commanders.NewVersionChecker(client).Execute()
	},
}

var subObjectCount = &cobra.Command{
	Use:     "object-count",
	Short:   "count database objects and numeric objects",
	Long:    "count database objects and numeric objects",
	Aliases: []string{"oc"},
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
			grpc.WithInsecure())
		if connConfigErr != nil {
			fmt.Println(connConfigErr)
			os.Exit(1)
		}
		client := pb.NewCliToHubClient(conn)
		return commanders.NewObjectCountChecker(client).Execute()
	},
}

var subDiskSpace = &cobra.Command{
	Use:     "disk-space",
	Short:   "check that disk space usage is less than 80% on all segments",
	Long:    "check that disk space usage is less than 80% on all segments",
	Aliases: []string{"du"},
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
			grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}
		client := pb.NewCliToHubClient(conn)
		return commanders.NewDiskSpaceChecker(client).Execute()
	},
}

var subConversion = &cobra.Command{
	Use:   "conversion",
	Short: "the status of the conversion",
	Long:  "the status of the conversion",
	Run: func(cmd *cobra.Command, args []string) {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort, grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}
		client := pb.NewCliToHubClient(conn)
		reporter := commanders.NewReporter(client)
		err := reporter.OverallConversionStatus()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}
	},
}

var subConfig = &cobra.Command{
	Use:   "config",
	Short: "gather cluster configuration",
	Long:  "gather cluster configuration",
	Run: func(cmd *cobra.Command, args []string) {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
			grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}
		client := pb.NewCliToHubClient(conn)
		err := commanders.NewConfigChecker(client).Execute()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}
	},
}

var subSeginstall = &cobra.Command{
	Use:   "seginstall",
	Short: "confirms that the new software is installed on all segments",
	Long: "Running this command will validate that the new software is installed on all segments, " +
		"and register successful or failed validation (available in `gpupgrade status upgrade`)",
	Run: func(cmd *cobra.Command, args []string) {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort, grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}
		client := pb.NewCliToHubClient(conn)

		err := commanders.NewSeginstallChecker(client).Execute()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}

		fmt.Println("Seginstall is underway. Use command \"gpupgrade status upgrade\" " +
			"to check its current status, and/or hub logs for possible errors.")
	},
}

var subConvertMaster = &cobra.Command{
	Use:   "convert-master",
	Short: "start upgrade process on master",
	Long:  `start upgrade process on master`,
	Run: func(cmd *cobra.Command, args []string) {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
			grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}

		client := pb.NewCliToHubClient(conn)
		err := commanders.NewUpgrader(client).ConvertMaster()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}
	},
}

var subConvertPrimaries = &cobra.Command{
	Use:   "convert-primaries",
	Short: "start upgrade process on primary segments",
	Long:  `start upgrade process on primary segments`,
	Run: func(cmd *cobra.Command, args []string) {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
			grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}

		client := pb.NewCliToHubClient(conn)
		err := commanders.NewUpgrader(client).ConvertPrimaries()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}
	},
}

var subShareOids = &cobra.Command{
	Use:   "share-oids",
	Short: "share oid files across cluster",
	Long:  `share oid files generated by pg_upgrade on master, across cluster`,
	Run: func(cmd *cobra.Command, args []string) {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
			grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}

		client := pb.NewCliToHubClient(conn)
		err := commanders.NewUpgrader(client).ShareOids()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}
	},
}

var subValidateStartCluster = &cobra.Command{
	Use:   "validate-start-cluster",
	Short: "Attempt to start upgraded cluster",
	Long:  `Use gpstart in order to validate that the new cluster can successfully transition from a stopped to running state`,
	Run: func(cmd *cobra.Command, args []string) {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
			grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}

		client := pb.NewCliToHubClient(conn)
		err := commanders.NewUpgrader(client).ValidateStartCluster()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}
	},
}

var subReconfigurePorts = &cobra.Command{
	Use:   "reconfigure-ports",
	Short: "Set master port on upgraded cluster to the value from the older cluster",
	Long:  `Set master port on upgraded cluster to the value from the older cluster`,
	Run: func(cmd *cobra.Command, args []string) {
		conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
			grpc.WithInsecure())
		if connConfigErr != nil {
			gplog.Error(connConfigErr.Error())
			os.Exit(1)
		}

		client := pb.NewCliToHubClient(conn)
		err := commanders.NewUpgrader(client).ReconfigurePorts()
		if err != nil {
			gplog.Error(err.Error())
			os.Exit(1)
		}
	},
}

// gpupgrade prepare init
func createInitSubcommand() *cobra.Command {
	var oldBinDir, newBinDir string

	subInit := &cobra.Command{
		Use:   "init",
		Short: "Setup state dir and config file",
		Long:  `Setup state dir and config file`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// If we got here, the args are okay and the user doesn't need a usage
			// dump on failure.
			cmd.SilenceUsage = true

			stateDir := utils.GetStateDir()
			return commanders.DoInit(stateDir, oldBinDir, newBinDir)
		},
	}

	subInit.PersistentFlags().StringVar(&oldBinDir, "old-bindir", "", "install directory for old gpdb version")
	subInit.MarkPersistentFlagRequired("old-bindir")
	subInit.PersistentFlags().StringVar(&newBinDir, "new-bindir", "", "install directory for new gpdb version")
	subInit.MarkPersistentFlagRequired("new-bindir")

	return subInit
}
