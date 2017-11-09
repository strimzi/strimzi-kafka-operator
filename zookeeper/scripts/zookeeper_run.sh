#!/bin/bash

# volume for saving Zookeeper server logs
export ZOOKEEPER_VOLUME="/var/lib/zookeeper/"
# base name for Zookeeper server data dir and application logs
export ZOOKEEPER_DATA_BASE_NAME="data"
export ZOOKEEPER_LOG_BASE_NAME="logs"
# Disable JMX until we need it
export JMXDISABLE=true
export BASE_HOSTNAME=$(hostname | rev | cut -d "-" -f2- | rev)
export BASE_FQDN=$(hostname -f | cut -d "." -f2-)

# Detect the server ID based on the hostname.
# StatefulSets are numbered from 0 so we have to always increment by 1
export ZOOKEEPER_ID=$(hostname | awk -F'-' '{print $NF+1}')
echo "Detected Zookeeper ID $ZOOKEEPER_ID"

# dir for saving application logs
export LOG_DIR=$ZOOKEEPER_VOLUME$ZOOKEEPER_LOG_BASE_NAME

# create data dir
export ZOOKEEPER_DATA_DIR=$ZOOKEEPER_VOLUME$ZOOKEEPER_DATA_BASE_NAME
mkdir -p $ZOOKEEPER_DATA_DIR

# Create myid file
echo $ZOOKEEPER_ID > $ZOOKEEPER_DATA_DIR/myid

# Write the config file
cat > /tmp/zookeeper.properties <<EOF
timeTick=2000
initLimit=5
syncLimit=2

# the directory where the snapshot is stored.
dataDir=${ZOOKEEPER_DATA_DIR}
clientPort=2181
quorumListenOnAllIPs=true
maxClientCnxns=0

# Snapshot autopurging
autopurge.snapRetainCount=3
autopurge.purgeInterval=1

# Ensemble configuration
EOF

NODE=1
while [ $NODE -le $ZOOKEEPER_NODE_COUNT ]; do
    echo "server.${NODE}=${BASE_HOSTNAME}-$((NODE-1)).${BASE_FQDN}:2888:3888" >> /tmp/zookeeper.properties
    let NODE=NODE+1 
done

echo "Starting Zookeeper with configuration:"
cat /tmp/zookeeper.properties
echo ""

if [ -z "$ZOO_LOG4J_PROP" ]; then
  export ZOO_LOG4J_PROP="DEBUG, CONSOLE"
fi

# starting Zookeeper with final configuration
exec $ZOOKEEPER_HOME/bin/zkServer.sh start-foreground /tmp/zookeeper.properties
