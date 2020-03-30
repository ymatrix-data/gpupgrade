#! /bin/sh

set -ex

apk add --no-progress openssh-client
cp -R cluster_env_files/.ssh /root/.ssh

scp sqldump/dump.sql.xz gpadmin@mdw:/tmp/

echo 'Loading SQL dump into source cluster...'
time ssh -n gpadmin@mdw "
    set -eux -o pipefail

    version=\$(rpm -q --qf '%{version}' '$OLD_PACKAGE')

    source /usr/local/greenplum-db-\${version}/greenplum_path.sh
    export PGOPTIONS='--client-min-messages=warning'
    unxz < /tmp/dump.sql.xz | psql -f - postgres
"
