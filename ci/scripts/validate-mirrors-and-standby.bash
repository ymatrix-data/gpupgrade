#! /bin/bash

set -eux

# Retrieves the installed GPHOME for a given GPDB RPM.
# XXX this is the third usage. Deduplicate.
rpm_gphome() {
    local package_name=$1

    local version=$(ssh -n gpadmin@mdw rpm -q --qf '%{version}' "$package_name")
    echo /usr/local/greenplum-db-$version
}

cp -R cluster_env_files/.ssh /root/.ssh

# Load the finalize test library.
source gpupgrade_src/test/finalize_checks.bash

echo 'Doing failover tests of mirrors and standby...'
validate_mirrors_and_standby $(rpm_gphome ${NEW_PACKAGE}) mdw 5432
