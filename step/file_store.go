package step

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/google/renameio"
	"github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/idl"
)

// FileStore implements step.Store by providing persistent storage on disk.
type FileStore struct {
	path string
}

func NewFileStore(path string) *FileStore {
	return &FileStore{path}
}

// PrettyStatus exists only to write a string description of idl.StepStatus to
// the JSON representation, instead of an integer.
type PrettyStatus struct {
	idl.StepStatus
}

func (p PrettyStatus) MarshalText() ([]byte, error) {
	return []byte(p.String()), nil
}

func (p *PrettyStatus) UnmarshalText(buf []byte) error {
	name := string(buf)

	val, ok := idl.StepStatus_value[name]
	if !ok {
		return fmt.Errorf("unknown substep name %q", name)
	}

	p.StepStatus = idl.StepStatus(val)
	return nil
}

func (f *FileStore) load() (map[string]idl.StepStatus, error) {
	data, err := ioutil.ReadFile(f.path)
	if err != nil {
		return nil, err
	}

	var prettySubsteps map[string]PrettyStatus
	err = json.Unmarshal(data, &prettySubsteps)
	if err != nil {
		return nil, err
	}

	substeps := make(map[string]idl.StepStatus)
	for k, v := range prettySubsteps {
		substeps[k] = v.StepStatus
	}
	return substeps, nil
}

func (f *FileStore) Read(substep idl.UpgradeSteps) (idl.StepStatus, error) {
	steps, err := f.load()
	if err != nil {
		return idl.StepStatus_UNKNOWN_STATUS, err
	}

	status, ok := steps[substep.String()]
	if !ok {
		return idl.StepStatus_UNKNOWN_STATUS, nil
	}

	return status, nil
}

// Write atomically updates the status file.
// Load the latest values from the filesystem, rather than storing
// in-memory on a struct to avoid having two sources of truth.
func (f *FileStore) Write(substep idl.UpgradeSteps, status idl.StepStatus) (err error) {
	steps, err := f.load()
	if err != nil {
		return err
	}

	prettySteps := make(map[string]PrettyStatus)
	for k, v := range steps {
		prettySteps[k] = PrettyStatus{v}
	}
	prettySteps[substep.String()] = PrettyStatus{status}

	data, err := json.MarshalIndent(prettySteps, "", "  ") // pretty print JSON
	if err != nil {
		return err
	}

	// Use renameio to ensure atomicity when writing the status file.
	t, err := renameio.TempFile("", f.path)
	if err != nil {
		return err
	}
	defer func() {
		if cErr := t.Cleanup(); cErr != nil {
			err = multierror.Append(err, cErr).ErrorOrNil()
		}
	}()

	_, err = t.Write(data)
	if err != nil {
		return err
	}

	return t.CloseAtomicallyReplace()
}
