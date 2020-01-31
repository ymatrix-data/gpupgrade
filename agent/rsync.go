package agent

import (
	"os/exec"

	"golang.org/x/xerrors"
)

var rsyncCommand = exec.Command

type RsyncError struct {
	errorText string
}

func (e RsyncError) Error() string {
	return e.errorText
}

func Rsync(sourceDir, targetDir string, excludedFiles []string) error {
	arguments := append([]string{
		"--archive", "--delete",
		sourceDir + "/", targetDir,
	}, makeExclusionList(excludedFiles)...)

	if _, err := rsyncCommand("rsync", arguments...).Output(); err != nil {
		return RsyncError{
			errorText: extractTextFromError(err),
		}
	}

	return nil
}

func extractTextFromError(err error) string {
	var exitError *exec.ExitError
	errorText := err.Error()

	if xerrors.As(err, &exitError) {
		errorText = string(exitError.Stderr)
	}
	return errorText
}

func makeExclusionList(excludedFiles []string) []string {
	var exclusions []string
	for _, excludedFile := range excludedFiles {
		exclusions = append(exclusions, "--exclude", excludedFile)
	}
	return exclusions
}
