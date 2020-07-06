#! /usr/bin/env bats
#
# Copyright (c) 2017-2020 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

load helpers

SCRIPTS_DIR=$BATS_TEST_DIRNAME/../migration_scripts

setup() {
    skip_if_no_gpdb

    PSQL="$GPHOME_SOURCE/bin/psql -X --no-align --tuples-only"

    TEST_DBNAME=testdb
    DEFAULT_DBNAME=postgres
    GPHDFS_USER=gphdfs_user

    $PSQL -c "DROP DATABASE IF EXISTS $TEST_DBNAME;" -d $DEFAULT_DBNAME
    $PSQL -c "DROP ROLE IF EXISTS $GPHDFS_USER;" -d $DEFAULT_DBNAME

    STATE_DIR=`mktemp -d /tmp/gpupgrade.XXXXXX`
    export GPUPGRADE_HOME="${STATE_DIR}/gpupgrade"

    gpupgrade kill-services
}

teardown() {
    # XXX Beware, BATS_TEST_SKIPPED is not a documented export.
    if [ -n "${BATS_TEST_SKIPPED}" ]; then
        return
    fi

    if [ -n "$NEW_CLUSTER" ]; then
        delete_finalized_cluster $GPHOME_TARGET $NEW_CLUSTER
    fi

    if [ -n "$MIGRATION_DIR" ]; then
        rm -r $MIGRATION_DIR
    fi

    gpupgrade kill-services
    rm -r "$STATE_DIR"

    # XXX Instead of coupling against restore_cluster's internals, we should
    # introduce the TEARDOWN_FUNCTIONS cleanup pattern here.
    if [ -n "$RSYNC_PAIRS" ] || [ -n "$MASTER_AND_PRIMARY_DATADIRS" ]; then
        restore_cluster
    fi

    start_source_cluster

    $GPHOME_SOURCE/bin/psql -c "DROP ROLE IF EXISTS ${GPHDFS_USER}" -d $DEFAULT_DBNAME
    $GPHOME_SOURCE/bin/psql -c "DROP DATABASE IF EXISTS ${TEST_DBNAME}" -d $DEFAULT_DBNAME
}

drop_unfixable_objects() {
    # the migration script should not remove primary / unique key constraints on partitioned tables, so
    # remove them manually by dropping the table as they can't be dropped.
    $GPHOME_SOURCE/bin/psql -d $TEST_DBNAME -p $PGPORT -c "DROP TABLE table_with_unique_constraint_p;"
    $GPHOME_SOURCE/bin/psql -d $TEST_DBNAME -p $PGPORT -c "DROP TABLE table_with_primary_constraint_p;"
}

@test "migration scripts generate sql to modify non-upgradeable objects and fix pg_upgrade check errors" {
    setup_restore_cluster "--mode=copy"

    $PSQL -c "CREATE DATABASE $TEST_DBNAME;" -d $DEFAULT_DBNAME
    $PSQL -f $BATS_TEST_DIRNAME/../migration_scripts/test/create_nonupgradable_objects.sql -d $TEST_DBNAME

    run gpupgrade initialize \
        --source-gphome="$GPHOME_SOURCE" \
        --target-gphome="$GPHOME_TARGET" \
        --source-master-port="${PGPORT}" \
        --temp-port-range 6020-6040 \
        --disk-free-ratio 0 \
        --verbose
    [ "$status" -ne 0 ] || fail "expected initialize to fail due to pg_upgrade check: $output"

    egrep "\"CHECK_UPGRADE\": \"FAILED\"" $GPUPGRADE_HOME/status.json
    egrep "^Checking.*fatal$" $GPUPGRADE_HOME/pg_upgrade/seg-1/pg_upgrade_internal.log

    MIGRATION_DIR=`mktemp -d /tmp/migration.XXXXXX`
    $SCRIPTS_DIR/generate_migration_sql.bash $GPHOME_SOURCE $PGPORT $MIGRATION_DIR
    $SCRIPTS_DIR/execute_migration_sql.bash $GPHOME_SOURCE $PGPORT $MIGRATION_DIR/pre-upgrade

    drop_unfixable_objects

    gpupgrade initialize \
        --source-gphome="$GPHOME_SOURCE" \
        --target-gphome="$GPHOME_TARGET" \
        --source-master-port="${PGPORT}" \
        --temp-port-range 6020-6040 \
        --disk-free-ratio 0 \
        --verbose
    gpupgrade execute --verbose
    gpupgrade finalize --verbose

    NEW_CLUSTER="$MASTER_DATA_DIRECTORY"
}

@test "after reverting recreate scripts restore dropped objects" {
    $PSQL -c "CREATE DATABASE $TEST_DBNAME;" -d $DEFAULT_DBNAME
    $PSQL -f $BATS_TEST_DIRNAME/../migration_scripts/test/create_nonupgradable_objects.sql -d $TEST_DBNAME

    drop_unfixable_objects # don't test what we won't fix

    # XXX We don't properly handle index constraints after revert, yet. Ignore
    # the test tables that break the diff for now.
    EXCLUSIONS="-T table_with_primary_constraint "
    EXCLUSIONS+="-T table_with_unique_constraint "
    EXCLUSIONS+="-T pt_with_index "
    EXCLUSIONS+="-T sales "

    MIGRATION_DIR=`mktemp -d /tmp/migration.XXXXXX`
    "$GPHOME_SOURCE"/bin/pg_dump --schema-only $TEST_DBNAME $EXCLUSIONS -f "$MIGRATION_DIR"/before.sql

    $SCRIPTS_DIR/generate_migration_sql.bash $GPHOME_SOURCE $PGPORT $MIGRATION_DIR
    $SCRIPTS_DIR/execute_migration_sql.bash $GPHOME_SOURCE $PGPORT $MIGRATION_DIR/pre-upgrade

    gpupgrade initialize \
        --source-gphome="$GPHOME_SOURCE" \
        --target-gphome="$GPHOME_TARGET" \
        --source-master-port="${PGPORT}" \
        --temp-port-range 6020-6040 \
        --disk-free-ratio 0 \
        --verbose
    gpupgrade execute --verbose
    gpupgrade revert --verbose

    $SCRIPTS_DIR/execute_migration_sql.bash $GPHOME_SOURCE $PGPORT $MIGRATION_DIR/post-revert

    "$GPHOME_SOURCE"/bin/pg_dump --schema-only $TEST_DBNAME $EXCLUSIONS -f "$MIGRATION_DIR"/after.sql
    diff -U3 --speed-large-files "$MIGRATION_DIR"/before.sql "$MIGRATION_DIR"/after.sql
}
