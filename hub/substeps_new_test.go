package hub

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestStatusFile(t *testing.T) {
	stateDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(stateDir); err != nil {
			t.Errorf("removing temp directory: %v", err)
		}
	}()

	path := filepath.Join(stateDir, "status.json")

	t.Run("creates status file if it does not exist", func(t *testing.T) {
		_, err := os.Open(path)
		if !os.IsNotExist(err) {
			t.Errorf("returned error %#v want ErrNotExist", err)
		}

		statusFile, err := getStatusFile(stateDir)
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}

		actual, err := ioutil.ReadFile(statusFile)
		if err != nil {
			t.Fatalf("ReadFile(%q) returned error %#v", statusFile, err)
		}

		expected := "{}"
		if string(actual) != expected {
			t.Errorf("read %v want %v", string(actual), expected)
		}
	})

	t.Run("does not create status file if it already exists", func(t *testing.T) {
		expected := "1234"
		err := ioutil.WriteFile(path, []byte(expected), 0600)
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}

		statusFile, err := getStatusFile(stateDir)
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}

		actual, err := ioutil.ReadFile(statusFile)
		if err != nil {
			t.Errorf("ReadFile(%q) returned error %#v", statusFile, err)
		}

		if string(actual) != expected {
			t.Errorf("read %v want %v", string(actual), expected)
		}
	})
}
