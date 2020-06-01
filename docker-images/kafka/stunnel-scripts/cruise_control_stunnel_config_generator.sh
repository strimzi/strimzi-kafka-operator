#!/usr/bin/env bash
set -e

# path were the Secret with Cruise Control certificates is mounted
CC_CERTS_KEYS=/etc/tls-sidecar/cc-certs
# Combine all the certs in the cluster CA into one file
CA_CERTS=/tmp/cluster-ca.crt
cat /etc/tls-sidecar/cluster-ca-certs/*.crt > "$CA_CERTS"

echo "pid = /usr/local/var/run/stunnel.pid"
echo "foreground = yes"
echo "debug = $TLS_SIDECAR_LOG_LEVEL"
echo "sslVersion = TLSv1.2"

cat <<-EOF
[zookeeper-2181]
client = yes
CAfile = ${CA_CERTS}
cert = ${CC_CERTS_KEYS}/cruise-control.crt
key = ${CC_CERTS_KEYS}/cruise-control.key
accept = 127.0.0.1:2181
connect = ${STRIMZI_ZOOKEEPER_CONNECT:-zookeeper-client:2181}
delay = yes
verify = 2

EOF
