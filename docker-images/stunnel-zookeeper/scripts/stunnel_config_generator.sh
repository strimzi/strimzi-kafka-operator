#!/bin/bash

# path were the Secret with certificates is mounted
CERTS=/etc/stunnel/certs

echo "pid = /usr/local/var/run/stunnel.pid"
echo "foreground = yes"
echo "debug = info"
 
declare -a PORTS=("2888" "3888")

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
			[${PEER}]
			client=yes
			CAfile=${CERTS}/internal-ca.crt
			accept=127.0.0.1:$(expr $port \* 10 + $NODE - 1)
			connect=${PEER}.${BASE_FQDN}:$port

			EOF
		fi
		let NODE=NODE+1
	done

	# Zookeeper port where stunnel forwards received traffic
	CONNECTOR_PORT=$(expr $port \* 10 + $ZOOKEEPER_ID - 1)

	# private key and related cert for current Zookeeper instance into one PEM
	CURRENT=${BASE_HOSTNAME}-$((ZOOKEEPER_ID-1))
	KEY_AND_CERT=/tmp/${CURRENT}.pem

	cat ${CERTS}/${CURRENT}.crt ${CERTS}/${CURRENT}.key > ${KEY_AND_CERT}
	chmod 640 ${KEY_AND_CERT}

	# listener configuration for incoming connection from peers
	cat <<-EOF
	[listener-$port]
	client=no
	cert=${KEY_AND_CERT}
	accept=$port
	connect=127.0.0.1:$CONNECTOR_PORT

	EOF
done
