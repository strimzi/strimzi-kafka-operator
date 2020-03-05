#!/usr/bin/env bash

# We need to first identify which version of ZK we are starting in, so we know which config format to use
ZK_MINOR=$(ls libs | grep -Po 'zookeeper-\K\d+.\d+.\d+' | head -1 | cut -d. -f2)

# Write the config file
cat <<EOF
# the directory where the snapshot is stored.
dataDir=${ZOOKEEPER_DATA_DIR}
EOF

if [[ ZK_MINOR -lt 5 ]]; then
    echo "clientPort=$(expr 10 \* 2181 + $ZOOKEEPER_ID - 1)"
else
    echo "4lw.commands.whitelist=*"
    echo "standaloneEnabled=false"
    echo "reconfigEnabled=true"
fi

cat <<EOF

# Provided configuration
${ZOOKEEPER_CONFIGURATION}
# Zookeeper nodes configuration
EOF

NODE=1
FOLLOWER_PORT=$(expr 10 \* 2888)
ELECTION_PORT=$(expr 10 \* 3888)
CLIENT_PORT=$(expr 10 \* 2181)
while [[ $NODE -le $ZOOKEEPER_NODE_COUNT ]]; do
    if [[ ZK_MINOR -lt 5 ]]; then
        echo "server.${NODE}=127.0.0.1:$(expr $FOLLOWER_PORT + $NODE - 1):$(expr $ELECTION_PORT + $NODE - 1)"
    else
        echo "server.${NODE}=127.0.0.1:$(expr $FOLLOWER_PORT + $NODE - 1):$(expr $ELECTION_PORT + $NODE - 1):participant;127.0.0.1:$(expr $CLIENT_PORT + $NODE - 1)"
    fi
    let NODE=NODE+1
done
