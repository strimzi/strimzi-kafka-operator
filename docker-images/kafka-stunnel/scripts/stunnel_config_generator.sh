#!/bin/bash

# path were the Secret with broker certificates is mounted
KAFKA_CERTS_KEYS=/etc/tls-sidecar/kafka-brokers
# Combine all the certs in the cluster CA into one file
CA_CERTS=/tmp/cluster-ca.crt
cat /etc/tls-sidecar/cluster-ca-certs/*.crt > "$CA_CERTS"

CURRENT=${BASE_HOSTNAME}-${KAFKA_BROKER_ID}

echo "pid = /usr/local/var/run/stunnel.pid"
echo "foreground = yes"
echo "debug = $TLS_SIDECAR_LOG_LEVEL"

cat <<-EOF
[zookeeper-2181]
client = yes
CAfile = ${CA_CERTS}
cert = ${KAFKA_CERTS_KEYS}/${CURRENT}.crt
key = ${KAFKA_CERTS_KEYS}/${CURRENT}.key
accept = 127.0.0.1:2181
connect = ${KAFKA_ZOOKEEPER_CONNECT:-zookeeper-client:2181}
verify = 2

EOF