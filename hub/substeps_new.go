package hub

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/step"
)

func BeginStep(stateDir string, name string, sender messageSender) (*step.Step, error) {
	path := filepath.Join(stateDir, fmt.Sprintf("%s.log", name))
	log, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, xerrors.Errorf(`step "%s": %w`, name, err)
	}

	_, err = fmt.Fprintf(log, "\n%s in progress.\n", strings.Title(name))
	if err != nil {
		log.Close()
		return nil, xerrors.Errorf(`logging step "%s": %w`, name, err)
	}

	statusPath, err := getStatusFile(stateDir)
	if err != nil {
		return nil, xerrors.Errorf("step %q: %w", name, err)
	}

	streams := newMultiplexedStream(sender, log)
	return step.New(name, sender, step.NewFileStore(statusPath), streams), nil
}

// Returns path to status file, and if one does not exist it creates an empty
// JSON file.
func getStatusFile(stateDir string) (path string, err error) {
	path = filepath.Join(stateDir, "status.json")

	f, err := os.OpenFile(path, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0600)
	if os.IsExist(err) {
		return path, nil
	}
	if err != nil {
		return "", err
	}

	defer func() {
		if cErr := f.Close(); cErr != nil {
			err = multierror.Append(err, cErr).ErrorOrNil()
		}
	}()

	// MarshallJSON requires a well-formed JSON file
	_, err = f.WriteString("{}")
	if err != nil {
		return "", err
	}

	return path, nil
}
