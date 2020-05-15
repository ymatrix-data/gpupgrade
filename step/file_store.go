// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package step

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/google/renameio"
	"github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/idl"
)

type Store interface {
	Read(string, idl.Substep) (idl.Status, error)
	Write(string, idl.Substep, idl.Status) error
}

// FileStore implements step.Store by providing persistent storage on disk.
type FileStore struct {
	path string
}

type statusMap = map[string]map[string]idl.Status

func NewFileStore(path string) *FileStore {
	return &FileStore{path}
}

// PrettyStatus exists only to write a string description of idl.Status to
// the JSON representation, instead of an integer.
type PrettyStatus struct {
	idl.Status
}

func (p PrettyStatus) MarshalText() ([]byte, error) {
	return []byte(p.String()), nil
}

func (p *PrettyStatus) UnmarshalText(buf []byte) error {
	name := string(buf)

	val, ok := idl.Status_value[name]
	if !ok {
		return fmt.Errorf("unknown substep name %q", name)
	}

	p.Status = idl.Status(val)
	return nil
}

func (f *FileStore) load() (statusMap, error) {
	data, err := ioutil.ReadFile(f.path)
	if err != nil {
		return nil, err
	}

	var prettySubsteps map[string]map[string]PrettyStatus
	err = json.Unmarshal(data, &prettySubsteps)
	if err != nil {
		return nil, err
	}

	substeps := make(statusMap)
	for section, statuses := range prettySubsteps {
		substeps[section] = make(map[string]idl.Status)

		for k, v := range statuses {
			substeps[section][k] = v.Status
		}
	}
	return substeps, nil
}

func (f *FileStore) Read(section string, substep idl.Substep) (idl.Status, error) {
	steps, err := f.load()
	if err != nil {
		return idl.Status_UNKNOWN_STATUS, err
	}

	sectionMap, ok := steps[section]
	if !ok {
		return idl.Status_UNKNOWN_STATUS, nil
	}

	status, ok := sectionMap[substep.String()]
	if !ok {
		return idl.Status_UNKNOWN_STATUS, nil
	}

	return status, nil
}

// Write atomically updates the status file.
// Load the latest values from the filesystem, rather than storing
// in-memory on a struct to avoid having two sources of truth.
func (f *FileStore) Write(section string, substep idl.Substep, status idl.Status) (err error) {
	steps, err := f.load()
	if err != nil {
		return err
	}

	prettySteps := make(map[string]map[string]PrettyStatus)

	prettySteps[section] = make(map[string]PrettyStatus)
	for section := range steps {
		prettySteps[section] = make(map[string]PrettyStatus)
	}

	for section, statuses := range steps {
		for k, v := range statuses {
			prettySteps[section][k] = PrettyStatus{v}
		}
	}
	prettySteps[section][substep.String()] = PrettyStatus{status}

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
