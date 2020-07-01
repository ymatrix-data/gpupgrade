// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package cli

import "fmt"

// NextActions attaches the PrintHelp method to an existing error. This is used
// to tell the CLI's top level to print additional helper text AFTER the error
// message is printed.
type NextActions struct {
	error
	Subcommand string // the gpupgrade subcommand name to print
}

func NewNextActions(err error, subcommand string) NextActions {
	return NextActions{
		error:      err,
		Subcommand: subcommand,
	}
}

func (n NextActions) PrintHelp() {
	// TODO: consider making the "revert" text optional, if we end up using this
	// in contexts (such as finalize) where revert is not an option.
	fmt.Printf(`
NEXT ACTIONS
------------
Please address the above issue and run "gpupgrade %s" again.

If you would like to return the cluster to its original state, please run "gpupgrade revert".
`, n.Subcommand)
}
