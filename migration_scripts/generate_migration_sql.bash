#!/usr/bin/env bash
#
# Copyright (c) 2017-2020 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

if [ "$#" -ne 3 ]; then
    echo "Illegal number of parameters"
    echo "Usage: $(basename $0) <GPHOME> <PGPORT> <OUTPUT_DIR>"
    exit 1
fi

GPHOME=$1
PGPORT=$2
OUTPUT_DIR=$3
APPLY_ONCE_FILES=("gen_alter_gphdfs_roles.sql")

get_databases(){
    databases=$("$GPHOME"/bin/psql -d postgres -p "$PGPORT" -Atc "SELECT datname FROM pg_database WHERE datname != 'template0';")
    echo "$databases"
}

exec_sql_file(){
    local database=$1
    local path=$2
    local output_dir=$3

    local output_file=migration_${database}_$(basename "$path")

    records=$("$GPHOME"/bin/psql -d "$database" -p "$PGPORT" -Atf "$path")
    if [[ -n "$records" ]]; then
        echo "\c $database" > "${output_dir}/${output_file}"
        echo "$records" >> "${output_dir}/${output_file}"
    fi
}

should_apply_once(){
    local path=$1
    local file=$(basename "$path")
    [[ " ${APPLY_ONCE_FILES[*]} " =~ ${file} ]]
}

execute_sql_directory() {
    local dir=$1; shift
    local databases=( "$@" )

    local paths=($(find "$(dirname "$0")/${dir}" -type f -name "*.sql"))
    local output_dir="${OUTPUT_DIR}/${dir}"

    mkdir -p "$output_dir"
    rm -rf "$output_dir"/*.sql

    for database in "${databases[@]}"; do
        for path in "${paths[@]}"; do
            # generate sql modifying shared objects only for default database
            if ! should_apply_once "$path" || [ "$database" == "postgres" ]; then
                exec_sql_file "$database" "$path" "$output_dir"
            fi
        done
    done

    echo "Output files are located in: $output_dir"
}

main(){
    local dirs=(pre-upgrade)
    local databases=($(get_databases))

    for dir in "${dirs[@]}"; do
        execute_sql_directory "$dir" "${databases[@]}"
    done
}

main
