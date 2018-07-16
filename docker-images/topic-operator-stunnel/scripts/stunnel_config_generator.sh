#!/bin/bash

# path were the Secret with certificates is mounted
CERTS=/etc/tls-sidecar/certs

echo "pid = /usr/local/var/run/stunnel.pid"
echo "foreground = yes"
echo "debug = info"

cat <<-EOF
[zookeeper-2181]
client = yes
CAfile = ${CERTS}/cluster-ca.crt
cert = ${CERTS}/topic-operator.crt
key = ${CERTS}/topic-operator.key
accept = 127.0.0.1:2181
connect = ${STRIMZI_ZOOKEEPER_CONNECT:-zookeeper-client:2181}
verify = 2

EOF
