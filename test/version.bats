#! /usr/bin/env bats

# TODO: test also that we have a git version in the version string as well 

@test "gpupgrade version subcommand reasonable" {
    run gpupgrade version
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ ^gpupgrade[[:space:]]version[[:space:]][[:digit:]]\.[[:digit:]]\.[[:digit:]] ]]
}

@test "gpupgrade hub version subcommand reasonable" {
    run gpupgrade hub --version
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ ^gpupgrade[[:space:]]hub[[:space:]]version[[:space:]][[:digit:]]\.[[:digit:]]\.[[:digit:]] ]]
}

@test "gpupgrade agent version subcommand reasonable" {
    run gpupgrade agent --version
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ ^gpupgrade[[:space:]]agent[[:space:]]version[[:space:]][[:digit:]]\.[[:digit:]]\.[[:digit:]] ]]
}
