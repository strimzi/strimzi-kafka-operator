#!/bin/bash

# Write the config file
cat <<EOF
# the directory where the snapshot is stored.
dataDir=${ZOOKEEPER_DATA_DIR}
clientPort=2181

# Provided configuration
${ZOOKEEPER_CONFIGURATION}
# Zookeeper nodes configuration
EOF

NODE=1
while [ $NODE -le $ZOOKEEPER_NODE_COUNT ]; do
    echo "server.${NODE}=${BASE_HOSTNAME}-$((NODE-1)).${BASE_FQDN}:2888:3888"
    let NODE=NODE+1
done