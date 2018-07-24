#! /usr/bin/env bats

@test "gpupgrade outputs help text when no params are given" {
    run gpupgrade

    [ "$status" -eq 1 ]
    [[ "$output" = *"Please specify one command of: check, config, prepare, status, upgrade, or version"* ]]
}

@test "gpupgrade subcommands fail when passed insufficient arguments" {
    run gpupgrade prepare init
    [ "$status" -eq 1 ]
    [[ "$output" = *'Required flag(s) "new-bindir", "old-bindir" have/has not been set'* ]]
}
