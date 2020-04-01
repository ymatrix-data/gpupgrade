#! /bin/bash

set -eux

cp -R cluster_env_files/.ssh /root/.ssh

# Load the finalize test library.
source gpupgrade_src/test/finalize_checks.bash

echo 'Doing failover tests of mirrors and standby...'
validate_mirrors_and_standby /usr/local/greenplum-db-new mdw 5432
