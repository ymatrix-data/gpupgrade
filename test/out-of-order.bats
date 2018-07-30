#! /usr/bin/env bats
#
# This file provides negative test cases for when the user does not execute
# upgrade steps in the correct order after starting the hub.

load helpers

setup() {
    STATE_DIR=`mktemp -d`
    export GPUPGRADE_HOME="${STATE_DIR}/gpupgrade"
    gpupgrade prepare init --old-bindir /dummy --new-bindir /dummy

    kill_hub
    gpupgrade prepare start-hub
}

teardown() {
    kill_hub
    rm -r "${STATE_DIR}"
}

@test "seginstall requires segments to have been loaded into the configuration" {
    gpupgrade check seginstall
    run gpupgrade status upgrade
    [[ "$output" = *"FAILED - Install binaries on segments"* ]]
}

@test "start-agents requires segments to have been loaded into the configuration" {
    gpupgrade prepare start-agents
    run gpupgrade status upgrade
    [[ "$output" = *"FAILED - Agents Started on Cluster"* ]]
}
