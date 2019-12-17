package main

import (
	"fmt"
	"log"
	"os"
	"runtime/debug"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	_ "github.com/lib/pq"

	"github.com/greenplum-db/gpupgrade/cli/commands"
	"github.com/greenplum-db/gpupgrade/utils/daemon"
)

func main() {
	setUpLogging()

	confirmValidCommand()

	root := commands.BuildRootCommand()
	root.SilenceErrors = true // we'll print these ourselves

	err := root.Execute()
	if err != nil && err != daemon.ErrSuccessfullyDaemonized {
		// Use v to print the stack trace of an object errors.
		fmt.Printf("\n%+v\n", err)
		os.Exit(1)
	}
}

func confirmValidCommand() {
	if len(os.Args[1:]) < 1 {
		log.Fatal("Please specify one command of: check, config, initialize, prepare, status, upgrade, or version")
	}
}

func setUpLogging() {
	debug.SetTraceback("all")
	//empty logdir defaults to ~/gpAdminLogs
	gplog.InitializeLogging("gpupgrade_cli", "")
}
