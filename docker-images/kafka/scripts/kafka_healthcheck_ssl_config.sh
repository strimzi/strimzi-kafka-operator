#!/bin/bash

cat <<EOF
security.protocol=SSL
ssl.truststore.location=/var/lib/kafka/replication.truststore.jks
ssl.truststore.password=${ENC_PASSWORD}
EOF