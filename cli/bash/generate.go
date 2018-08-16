// This binary exists purely for the purpose of generating bash completion for
// the CLI.  You can run `go generate ./cli/bash` to regen the
// bash-completion.sh script.
package main

import (
	"os"

	"github.com/greenplum-db/gpupgrade/cli/commands"
)

//go:generate go run generate.go bash-completion.sh

func main() {
	root := commands.BuildRootCommand()
	root.GenBashCompletionFile(os.Args[1])
}
