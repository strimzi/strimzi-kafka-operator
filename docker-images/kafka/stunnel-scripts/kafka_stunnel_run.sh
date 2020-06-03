#!/usr/bin/env bash
set -e

BASE_HOSTNAME=$(hostname | rev | cut -d "-" -f2- | rev)
export BASE_HOSTNAME
KAFKA_BROKER_ID=$(hostname | awk -F'-' '{print $NF}')
export KAFKA_BROKER_ID

# Generate and print the config file
echo "Starting Stunnel with configuration:"
"${STUNNEL_HOME}"/kafka_stunnel_config_generator.sh | tee /tmp/stunnel.conf
echo ""

# starting Stunnel with final configuration
exec /usr/bin/tini -w -e 143 -- /usr/bin/stunnel /tmp/stunnel.conf
