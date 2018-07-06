#!/bin/bash
	      
CERTS=/etc/stunnel/certs/

declare -a PORTS=("2888" "3888")
for port in "${PORTS[@]}"
do
NODE=1
  #echo "debug = 7"
  #echo "output = /tmp/stunnel.log"
  while [ $NODE -le $ZOOKEEPER_NODE_COUNT ]; do
	        
		PEER=${BASE_HOSTNAME}-$((NODE-1))
		KEY_AND_CERT=${CERTS}${PEER}	
		PEM=/tmp/${PEER}.pem

		# Not necessarily the way to create pem file, might have to use openssl
		cat ${KEY_AND_CERT}.crt ${KEY_AND_CERT}.key > ${PEM}
		
		# Stunnel client configuration
		cat <<-EOF
		[${PEER})]
		client=yes
		key=${KEY_AND_CERT}.key
		cert=${PEM}
		accept=127.0.0.1:$(expr $port \* 10 + $NODE)
		connect=${PEER}.${BASE_FQDN}:$port

		EOF
		let NODE=NODE+1
  done
	# Zookeeper port where stunnel forwards recieved traffic
	CONNECTOR_PORT=$(expr $port \* 10)

	# Stunnel listener configuration
	cat <<-EOF
	[listener-$port]
	client=no
	key=${KEY_AND_CERT}.key
	cert=${PEM}
	accept=127.0.0.1:$port
	connect=127.0.0.1:$CONNECTOR_PORT

	EOF
done
