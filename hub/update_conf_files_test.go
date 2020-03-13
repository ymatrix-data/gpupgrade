package hub_test

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	"github.com/greenplum-db/gpupgrade/hub"
	"github.com/greenplum-db/gpupgrade/utils"
)

func TestUpdateGpperfmonConf(t *testing.T) {
	testhelper.SetupTestLogger()

	hub.SetExecCommand(exec.Command)
	defer func() { hub.SetExecCommand(nil) }()

	t.Run("it replaces the location line within the config to the new data directory location ", func(t *testing.T) {
		var usedPattern string
		var usedReplacement string
		var usedFile string

		hub.SetReplaceStringWithinFile(func(pattern string, replacement string, file string) error {
			usedPattern = pattern
			usedReplacement = replacement
			usedFile = file
			return nil
		})

		hub.UpdateGpperfmonConf("/some/master/data/dir")

		if usedPattern != "log_location = .*$" {
			t.Errorf("got %v, expected log_location = .*", usedPattern)
		}

		if usedReplacement != "log_location = /some/master/data/dir/gpperfmon/logs" {
			t.Errorf("got %v, expected %q", usedReplacement, "log_location = /some/master/data/dir/gpperfmon/logs")
		}

		if usedFile != "/some/master/data/dir/gpperfmon/conf/gpperfmon.conf" {
			t.Errorf("got %v, expected %q", usedFile, "/some/master/data/dir/gpperfmon/conf/gpperfmon.conf")
		}
	})
}

// TODO: this is an integration test; move it
func TestUpdatePostgresqlConf(t *testing.T) {
	// Make execCommand "live" again
	hub.SetExecCommand(exec.Command)
	defer hub.ResetExecCommand()

	hub.ResetReplaceStringWithinFile()

	// This will be our "master data directory".
	dir, err := ioutil.TempDir("", "gpupgrade-unit-")
	if err != nil {
		t.Fatalf("creating temporary directory: %+v", err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("removing temporary directory: %+v", err)
		}
	}()

	// Set up an example postgresql.conf.
	path := filepath.Join(dir, "postgresql.conf")
	original := `
port=5000
port=5000 # comment
port = 5000 # make sure we can handle spaces

# should not be replaced
gpperfmon_port=5000
port=50000
#port=5000
`

	if err := ioutil.WriteFile(path, []byte(original), 0640); err != nil {
		t.Fatalf("writing file contents: %+v", err)
	}

	// NOTE: we set the source and target cluster ports equal to enforce that
	// it's the oldTargetPort, and not target.MasterPort(), that matters.
	//
	// XXX That distinction seems unhelpful.
	source := hub.MustCreateCluster(t, []utils.SegConfig{
		{ContentID: -1, Role: "p", DataDir: "/does/not/exist", Port: 6000},
	})
	target := hub.MustCreateCluster(t, []utils.SegConfig{
		{ContentID: -1, Role: "p", DataDir: dir, Port: 6000},
	})

	// Perform the replacement.
	err = hub.UpdatePostgresqlConf(5000, target, source)
	if err != nil {
		t.Errorf("UpdatePostgresqlConf() returned error %+v", err)
	}

	// Check the contents.
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("reading final contents: %+v", err)
	}

	actual := string(contents)
	expected := `
port=6000
port=6000 # comment
port = 6000 # make sure we can handle spaces

# should not be replaced
gpperfmon_port=5000
port=50000
#port=5000
`

	if actual != expected {
		t.Errorf("replaced contents: %s\nwant: %s", actual, expected)
	}
}
