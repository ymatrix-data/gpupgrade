// Copyright (c) 2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package exectest_test

import (
	"errors"
	"fmt"
	"os/exec"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/greenplum-db/gpupgrade/testutils/exectest"
)

const printedStdout = "stdout for PrintingMain"

func PrintingMain() {
	fmt.Print(printedStdout)
}

func init() {
	exectest.RegisterMains(PrintingMain)
}

func TestCommandMock(t *testing.T) {
	t.Run("records Command calls", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		command, mock := exectest.NewCommandMock(ctrl)

		gomock.InOrder(
			mock.EXPECT().Command("bash", "-c", "false || true"),
			mock.EXPECT().Command("echo", "hello"),
		)

		_ = command("bash", "-c", "false || true")
		_ = command("echo", "hello")
	})

	t.Run("can switch Main implementations based on expectations", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		command, mock := exectest.NewCommandMock(ctrl)

		gomock.InOrder(
			mock.EXPECT().Command("bash", gomock.Any()).
				Return(exectest.Failure),
			mock.EXPECT().Command("echo", gomock.Any()).
				Return(PrintingMain),
		)

		cmd := command("bash", "-c", "false || true")
		err := cmd.Run()

		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Errorf("got error %#v, want type %T", err, exitErr)
		} else if exitErr.ExitCode() != 1 {
			t.Errorf("mock bash call returned code %d, want %d", exitErr.ExitCode(), 1)
		}

		cmd = command("echo", "hello")
		out, err := cmd.Output()
		if err != nil {
			t.Errorf("running echo: %+v", err)
		}

		stdout := string(out)
		if stdout != printedStdout {
			t.Errorf("echo printed %q, want %q", out, printedStdout)
		}
	})

	t.Run("calls Fatalf when extra commands are invoked", func(t *testing.T) {
		mockT := newTestReporter()
		ctrl := gomock.NewController(mockT)

		command, _ := exectest.NewCommandMock(ctrl)
		// Expect no calls.

		defer mockT.ExpectFatalf(t)
		command("bash") // mockT should force a panic here
	})

	t.Run("calls Fatalf when incorrect commands are invoked", func(t *testing.T) {
		mockT := newTestReporter()
		ctrl := gomock.NewController(mockT)

		command, mock := exectest.NewCommandMock(ctrl)
		mock.EXPECT().Command("vim")

		defer mockT.ExpectFatalf(t)
		command("emacs") // mockT should force a panic here
	})

	t.Run("sets up controller to Fatalf when expected commands are not invoked", func(t *testing.T) {
		mockT := newTestReporter()
		ctrl := gomock.NewController(mockT)

		_, mock := exectest.NewCommandMock(ctrl)
		mock.EXPECT().Command("vim")

		// make no calls to command

		defer mockT.ExpectFatalf(t)
		ctrl.Finish() // mockT should force a panic here
	})
}

// testReporter implements a fake testing.T so that this file's tests may ensure
// that t.Fatalf() is called appropriately.
type testReporter struct {
	sentinel interface{} // for differentiating expected panics from others
}

func newTestReporter() testReporter {
	return testReporter{sentinel: fmt.Errorf("sentinel panic from Fatalf")}
}

func (r testReporter) Errorf(fmt string, args ...interface{}) {
}

func (r testReporter) Fatalf(fmt string, args ...interface{}) {
	// Fatalf is documented to invoke runtime.Goexit(), but we need the test's
	// goroutine to continue. Use a panic to break out of the call stack,
	// instead.
	panic(r.sentinel)
}

// ExpectFatalf checks to see if this testReporter's Fatalf() function was
// invoked, and calls t.Errorf() if it was not. It's meant to be called in a
// defer statement.
func (r testReporter) ExpectFatalf(t *testing.T) {
	t.Helper()

	v := recover()
	if v == nil {
		t.Errorf("Fatalf() was not called")
		return
	}

	if v != r.sentinel {
		panic(v) // re-raise; unexpected panic
	}
}
