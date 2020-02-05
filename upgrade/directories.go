package upgrade

import (
	"fmt"
	"path/filepath"
)

// pgUpgradeDirectory returns a path to a directory underneath the state
// directory that is to be used for storing pg_upgrade state. It does not ensure
// the directory exists first.
func pgUpgradeDirectory(stateDir string) string {
	return filepath.Join(stateDir, "pg_upgrade")
}

// SegmentWorkingDirectory returns a path to a directory underneath the
// pgUpgradeDirectory that is to be used for pg_upgrade state for a given
// segment. Like pgUpgradeDirectory, it does not ensure that the directory
// exists.
func SegmentWorkingDirectory(stateDir string, contentID int) string {
	return filepath.Join(pgUpgradeDirectory(stateDir), fmt.Sprintf("seg%d", contentID))
}

// MasterWorkingDirectory is a convenience method equivalent to
// SegmentWorkingDirectory(stateDir, -1).
func MasterWorkingDirectory(stateDir string) string {
	return SegmentWorkingDirectory(stateDir, -1)
}
