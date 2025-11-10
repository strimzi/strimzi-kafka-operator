#!/usr/bin/env bash
set -e
set +x

# Clean-up /tmp directory from files which might have remained from previous container restart
# We ignore any errors which might be caused by files injected by different agents which we do not have the rights to delete
rm -rfv /tmp/* || true

# Run the script shared between Connect and MirrorMaker 2
exec ./kafka_connect_mm2_shared_run.sh
