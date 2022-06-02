// Copyright (c) 2017-2022 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import (
	"fmt"
	"strings"

	"github.com/blang/semver/v4"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
)

// Note that we represent the source and target versions separately.  Another
// option is a matrix explicitly listing supported source/target combinations.
// However, pg_upgrade supports upgrade from any version to any version.
// We are not sure yet if we are doing that for gpupgrade.

var (
	// sourceVersionAllowed returns whether or not the given semver.Version is a
	// valid source GPDB cluster version.
	sourceVersionAllowed semver.Range

	// targetVersionAllowed returns whether or not the given semver.Version is a
	// valid target GPDB cluster version.
	targetVersionAllowed semver.Range
)

// Source and Target Versions: modify these lists to control what will be allowed
// by the utility.  Map entries are of the form: GPDB_VERSION : MIN_ALLOWED_SEMVER

var minSourceVersions = map[int]string{
	7: "7.0.0",
}

var minTargetVersions = map[int]string{
	7: "7.0.0",
}

// The below boilerplate turns the source/targetRanges variables into
// source/targetVersionAllowed. You shouldn't need to touch it.

func init() {
	accumulateRanges(&sourceVersionAllowed, minSourceVersions)
	accumulateRanges(&targetVersionAllowed, minTargetVersions)
}

func accumulateRanges(a *semver.Range, minVersions map[int]string) {
	for v, min := range minVersions {
		// for example, 5: "5.28.0" becomes the Range string ">=5.28.0 <6.0.0"
		str := fmt.Sprintf(">=%s <%d.0.0", min, v+1)
		r := semver.MustParseRange(str)

		if *a == nil {
			*a = r
		} else {
			*a = a.OR(r)
		}
	}
}

// returns the version string that is the lowest of the major version of "version"
// or the lowest version supported in minVersions otherwise
func getMinVersion(version semver.Version, minVersions map[int]string) string {

	major := version.Major
	min, ok := minVersions[int(major)]
	if ok {
		return min
	}

	var lowest int
	for major := range minVersions {
		if lowest == 0 {
			lowest = major
		}
		if major < lowest {
			lowest = major
		}
	}
	return semver.MustParse(minVersions[lowest]).String()
}

func VerifyCompatibleGPDBVersions(sourceGPHome, targetGPHome string) error {
	var errs error

	sourceVersion, err := Version(sourceGPHome)
	if err != nil {
		return err
	}

	err = validateVersion(sourceVersion, idl.ClusterDestination_SOURCE)
	errs = errorlist.Append(errs, err)

	targetVersion, err := Version(targetGPHome)
	if err != nil {
		return err
	}

	err = validateVersion(targetVersion, idl.ClusterDestination_TARGET)
	errs = errorlist.Append(errs, err)

	return errs
}

func validateVersion(versionStr string, destination idl.ClusterDestination) error {
	versionsAllowed := sourceVersionAllowed
	minVersions := minSourceVersions
	if destination == idl.ClusterDestination_TARGET {
		versionsAllowed = targetVersionAllowed
		minVersions = minTargetVersions
	}

	version := semver.MustParse(versionStr)
	if !versionsAllowed(version) {
		min := getMinVersion(version, minVersions)
		return fmt.Errorf("%s cluster version %s is not supported.  "+
			"The minimum required version is %s. "+
			"We recommend the latest version.",
			strings.ToLower(destination.String()), version, min)
	}

	return nil
}
