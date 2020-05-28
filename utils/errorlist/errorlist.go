// Copyright (c) 2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package errorlist

import "github.com/hashicorp/go-multierror"

// Append takes at least two errors and combines them into a list. The rules are
// as follows:
//
//  - If all of the errors being appended are nil, Append returns nil.
//  - If exactly one of the errors being appended is non-nil, Append returns
//    that error unchanged.
//  - Otherwise, Append returns a slice of errors (wrapped in an Errors
//    instance) consisting of the inputs, minus any nil errors.
//
// If any of the passed errors are themselves Errors slices, their contents will
// be flattened into the resulting error. This behavior can be prevented by
// wrapping input Errors slices in their own context, for example using
// fmt.Errorf.
func Append(a, b error, e ...error) error {
	errs := append([]error{a, b}, e...)

	var all Errors
	for _, err := range errs {
		switch v := err.(type) {
		case nil:
			continue
		case Errors:
			all = append(all, v...)
		default:
			all = append(all, v)
		}
	}

	switch len(all) {
	case 0:
		return nil
	case 1:
		return all[0]
	default:
		return all
	}
}

// Errors is a slice of error values that can be treated like a single error
// value. Use Append to create Errors instances.
type Errors []error

func (e Errors) Error() string {
	// TODO: For now we maintain the old multierror output, but this should be
	// redesigned.
	return multierror.ListFormatFunc(e)
}
