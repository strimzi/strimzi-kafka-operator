#!/bin/bash

# Write the config file
cat <<EOF
# Provided configuration
${PRODUCER_CONFIGURATION}

# Fixed configuration
ssl.keystore.password=${CERTS_STORE_PASSWORD}
ssl.truststore.password=${CERTS_STORE_PASSWORD}
EOF