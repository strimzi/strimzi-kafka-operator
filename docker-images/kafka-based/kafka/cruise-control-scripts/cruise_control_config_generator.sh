#!/usr/bin/env bash
set -e

CRUISE_CONTROL_CONFIGURATION=$(</opt/cruise-control/custom-config/cruisecontrol.properties)

CC_ACCESS_LOG="/tmp/access.log"

# Write all webserver access logs to stdout
ln -sf /dev/stdout $CC_ACCESS_LOG

# Configure initial webserver options
cat <<EOF
webserver.accesslog.path=$CC_ACCESS_LOG
webserver.http.address=0.0.0.0
webserver.http.cors.allowmethods=OPTIONS,GET
EOF

# Configure webserver TLS encryption
if [ "$STRIMZI_CC_TLS_ENABLED" = true ]; then
    cat <<EOF
webserver.ssl.enable=true
webserver.ssl.keystore.location=/tmp/cruise-control/cruise-control.keystore.p12
webserver.ssl.keystore.password=$CERTS_STORE_PASSWORD
webserver.ssl.keystore.type=PKCS12
webserver.ssl.key.password=$CERTS_STORE_PASSWORD
EOF
fi

# Configure Kafka bootstrap server
cat <<EOF
bootstrap.servers=$STRIMZI_KAFKA_BOOTSTRAP_SERVERS
EOF

# Configure Kafka client TLS encryption
if [ "$STRIMZI_CC_TLS_ENABLED" = true ]; then
    cat <<EOF
ssl.truststore.type=PKCS12
ssl.truststore.location=/tmp/cruise-control/replication.truststore.p12
ssl.truststore.password=$CERTS_STORE_PASSWORD
EOF
fi

# Configure Kafka client mTLS authentication
if [ "$STRIMZI_CC_MTLS_ENABLED" = true ]; then
    cat <<EOF
ssl.keystore.type=PKCS12
ssl.keystore.location=/tmp/cruise-control/cruise-control.keystore.p12
ssl.keystore.password=$CERTS_STORE_PASSWORD
EOF
fi

# Configure Kafka client Service Account authentication
if [ "$STRIMZI_CC_SA_AUTH_ENABLED" = true ]; then
    cat <<EOF
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token.location="/var/run/secrets/strimzi.io/token";
sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler
EOF
fi

# Configure security protocol for Kafka client
if [ "$STRIMZI_CC_TLS_ENABLED" = true ] && [ "$STRIMZI_CC_SA_AUTH_ENABLED" = true ]; then
    cat <<EOF
security.protocol=SASL_SSL
EOF
elif [ "$STRIMZI_CC_TLS_ENABLED" = true ]; then
    cat <<EOF
security.protocol=SSL
EOF
elif [ "$STRIMZI_CC_SA_AUTH_ENABLED" = true ]; then
    cat <<EOF
security.protocol=SASL_PLAINTEXT
EOF
fi

# Remaining configuration options
cat <<EOF
kafka.broker.failure.detection.enable=true
capacity.config.file=/opt/cruise-control/custom-config/capacity.json
${CRUISE_CONTROL_CONFIGURATION}
EOF
