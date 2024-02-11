#!/usr/bin/env bash
set -e

CRUISE_CONTROL_CONFIGURATION=$(</opt/cruise-control/custom-config/cruisecontrol.properties)

CC_ACCESS_LOG="/tmp/access.log"

# Write all webserver access logs to stdout
ln -sf /dev/stdout $CC_ACCESS_LOG

# Write the config file
cat <<EOF
bootstrap.servers=$STRIMZI_KAFKA_BOOTSTRAP_SERVERS
webserver.accesslog.path=$CC_ACCESS_LOG
webserver.http.address=0.0.0.0
webserver.http.cors.allowmethods=OPTIONS,GET
webserver.ssl.keystore.location=/tmp/cruise-control/cruise-control.keystore.p12
webserver.ssl.keystore.password=$CERTS_STORE_PASSWORD
webserver.ssl.keystore.type=PKCS12
webserver.ssl.key.password=$CERTS_STORE_PASSWORD
security.protocol=SSL
ssl.keystore.type=PKCS12
ssl.keystore.location=/tmp/cruise-control/cruise-control.keystore.p12
ssl.keystore.password=$CERTS_STORE_PASSWORD
ssl.truststore.type=PKCS12
ssl.truststore.location=/tmp/cruise-control/replication.truststore.p12
ssl.truststore.password=$CERTS_STORE_PASSWORD
kafka.broker.failure.detection.enable=true
capacity.config.file=/opt/cruise-control/custom-config/capacity.json
${CRUISE_CONTROL_CONFIGURATION}
EOF
