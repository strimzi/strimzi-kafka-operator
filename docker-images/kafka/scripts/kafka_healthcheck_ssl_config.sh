#!/bin/bash

cat <<EOF
security.protocol=SSL
ssl.truststore.location=/tmp/kafka/replication.truststore.jks
ssl.truststore.password=${ENC_PASSWORD}
EOF