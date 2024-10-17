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

# TLS options
serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory
ssl.clientAuth=need
ssl.quorum.clientAuth=need
secureClientPort=2181
sslQuorum=true

ssl.protocol=TLSv1.3
ssl.quorum.protocol=TLSv1.3
ssl.enabledProtocols=TLSv1.3,TLSv1.2
ssl.quorum.enabledProtocols=TLSv1.3,TLSv1.2
ssl.ciphersuites=TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,TLS_CHACHA20_POLY1305_SHA256
ssl.quorum.ciphersuites: TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,TLS_CHACHA20_POLY1305_SHA256

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

version() { echo "$@" | awk -F. '{ printf("%d%03d%03d\n", $1,$2,$3); }'; }

# Setting self IP as 0.0.0.0 to workaround the slow DNS issue.
# Also check that Kafka version is greater-equal 3.4.1, because binding to 0.0.0.0 doesn't seem to work with previous version.
# For single node case, we cannot set to 0.0.0.0 since ZooKeeper will fail when looking for next candidate in case of issue.
# See: https://issues.apache.org/jira/browse/ZOOKEEPER-4708
# For two nodes case (which is anyway a bad configuration), we cannot set to 0.0.0.0 because the two nodes seem to ping-pong
# on leading and never reach a quorum. Without 0.0.0.0, it works just fine.
NODE=1
while [[ $NODE -le $ZOOKEEPER_NODE_COUNT ]]; do
    if [[ $NODE -eq $ZOOKEEPER_ID ]] && [[ $ZOOKEEPER_NODE_COUNT -gt 2 ]] && [[ $(version "$KAFKA_VERSION") -ge $(version "3.4.1") ]]; then
      echo "server.${NODE}=0.0.0.0:2888:3888:participant;127.0.0.1:12181"
    else
      echo "server.${NODE}=${BASE_HOSTNAME}-$((NODE-1)).${BASE_FQDN}:2888:3888:participant;127.0.0.1:12181"
    fi
    (( NODE=NODE+1 ))
done
