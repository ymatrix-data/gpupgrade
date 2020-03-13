package hub

import (
	"fmt"

	"golang.org/x/xerrors"
)

// For test stubbing purposes
// see common_test.go for stubbing function
var replaceStringWithinFileFunc = replaceStringWithinFile

//
// Use sed-like syntax to edit the text of a file:
//
// for example:
//
// sed s/pattern/replacement/ /path/to/some/file.txt
//
func ReplaceStringWithinFile(pattern string, replacement string, file string) error {
	return replaceStringWithinFileFunc(pattern, replacement, file)
}

func replaceStringWithinFile(pattern string, replacement string, pathToFile string) error {
	// XXX this isn't safe if the pattern or replacement contains @-signs
	substitutionString := fmt.Sprintf("s@%s@%s@", pattern, replacement)

	cmd := execCommand("sed", "-i.bak", substitutionString, pathToFile)

	// TODO: stream back output through the connection
	_, err := cmd.Output()
	if err != nil {
		return xerrors.Errorf("updating %s: %w", pathToFile, err)
	}
	return nil
}
