package main

import (
	"fmt"
	"log"
	"os"
	"runtime/debug"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	_ "github.com/lib/pq"
)

var (
	hubPort = "7527"
)

func main() {
	upgradePort := os.Getenv("GPUPGRADE_HUB_PORT")
	if upgradePort != "" {
		hubPort = upgradePort
	}

	setUpLogging()

	addFlagOptions()

	confirmValidCommand()

	root.AddCommand(prepare, config, status, check, version, upgrade)

	prepare.AddCommand(subStartHub, subInitCluster, subShutdownClusters, subStartAgents, subInit)
	config.AddCommand(subGet, subSet, subShow)
	status.AddCommand(subUpgrade, subConversion)
	check.AddCommand(subVersion, subObjectCount, subDiskSpace, subConfig, subSeginstall)
	upgrade.AddCommand(subConvertMaster, subConvertPrimaries, subShareOids, subValidateStartCluster, subReconfigurePorts)

	err := root.Execute()
	if err != nil {
		// Use v to print the stack trace of an object errors.
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}
}

func confirmValidCommand() {
	if len(os.Args[1:]) < 1 {
		log.Fatal("Please specify one command of: check, config, prepare, status, upgrade, or version")
	}
}

func setUpLogging() {
	debug.SetTraceback("all")
	//empty logdir defaults to ~/gpAdminLogs
	gplog.InitializeLogging("gpupgrade_cli", "")
}

func addFlagOptions() {
	addFlagOptionsToInit()
	addFlagOptionsToConfig()
}

func addFlagOptionsToInit() {
	subInit.PersistentFlags().StringVar(&oldBinDir, "old-bindir", "", "install directory for old gpdb version")
	subInit.MarkPersistentFlagRequired("old-bindir")
}
func addFlagOptionsToConfig() {
	subSet.Flags().StringVar(&oldBinDir, "old-bindir", "", "install directory for old gpdb version")
	subSet.Flags().StringVar(&oldBinDir, "new-bindir", "", "install directory for new gpdb version")
}
