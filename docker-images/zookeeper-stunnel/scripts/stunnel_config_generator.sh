#!/bin/bash

# path were the Secret with certificates is mounted
CERTS=/etc/tls-sidecar/certs

echo "pid = /usr/local/var/run/stunnel.pid"
echo "foreground = yes"
echo "debug = info"
 
declare -a PORTS=("2888" "3888")

CURRENT=${BASE_HOSTNAME}-$((ZOOKEEPER_ID-1))

for port in "${PORTS[@]}"
do
	NODE=1
	while [ $NODE -le $ZOOKEEPER_NODE_COUNT ]
	do
		# current node configuration is not needed
		if [ $NODE -ne $ZOOKEEPER_ID ]; then
			PEER=${BASE_HOSTNAME}-$((NODE-1))

			# configuration for outgoing connection to a peer
			cat <<-EOF
			[${PEER}-$port]
			client = yes
			CAfile = ${CERTS}/cluster-ca.crt
			cert = ${CERTS}/${CURRENT}.crt
			key = ${CERTS}/${CURRENT}.key
			accept = 127.0.0.1:$(expr $port \* 10 + $NODE - 1)
			connect = ${PEER}.${BASE_FQDN}:$port
			verify = 2

			EOF
		fi
		let NODE=NODE+1
	done

	# Zookeeper port where stunnel forwards received traffic
	CONNECTOR_PORT=$(expr $port \* 10 + $ZOOKEEPER_ID - 1)

	# listener configuration for incoming connection from peers
	cat <<-EOF
	[listener-$port]
	client = no
	CAfile = ${CERTS}/cluster-ca.crt
	cert = ${CERTS}/${CURRENT}.crt
	key = ${CERTS}/${CURRENT}.key
	accept = $port
	connect = 127.0.0.1:$CONNECTOR_PORT
	verify = 2

	EOF
done

# Zookeeper client port where stunnel forwards received traffic
CLIENT_PORT=$(expr 21810 + $ZOOKEEPER_ID - 1)

cat <<-EOF
[listener-2181]
client = no
CAfile = ${CERTS}/cluster-ca.crt
cert = ${CERTS}/${CURRENT}.crt
key = ${CERTS}/${CURRENT}.key
accept = 2181
connect = 127.0.0.1:$CLIENT_PORT
EOF