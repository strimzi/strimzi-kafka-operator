#!/usr/bin/env bash
set -e

# Write the config file
cat <<EOF
# The directory where the snapshot is stored.
dataDir=${ZOOKEEPER_DATA_DIR}

# Other options
4lw.commands.whitelist=*
standaloneEnabled=false
reconfigEnabled=true
clientPort=12181
clientPortAddress=127.0.0.1

# TLS options
serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory
ssl.clientAuth=need
ssl.quorum.clientAuth=need
secureClientPort=2181
sslQuorum=true

ssl.trustStore.location=/tmp/zookeeper/cluster.truststore.p12
ssl.trustStore.password=${CERTS_STORE_PASSWORD}
ssl.trustStore.type=PKCS12
ssl.quorum.trustStore.location=/tmp/zookeeper/cluster.truststore.p12
ssl.quorum.trustStore.password=${CERTS_STORE_PASSWORD}
ssl.quorum.trustStore.type=PKCS12

ssl.keyStore.location=/tmp/zookeeper/cluster.keystore.p12
ssl.keyStore.password=${CERTS_STORE_PASSWORD}
ssl.keyStore.type=PKCS12
ssl.quorum.keyStore.location=/tmp/zookeeper/cluster.keystore.p12
ssl.quorum.keyStore.password=${CERTS_STORE_PASSWORD}
ssl.quorum.keyStore.type=PKCS12

# Provided configuration
${ZOOKEEPER_CONFIGURATION}

# Zookeeper nodes configuration
EOF

NODE=1
while [[ $NODE -le $ZOOKEEPER_NODE_COUNT ]]; do
    echo "server.${NODE}=${BASE_HOSTNAME}-$((NODE-1)).${BASE_FQDN}:2888:3888:participant;127.0.0.1:12181"
    (( NODE=NODE+1 ))
done
