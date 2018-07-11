#!/bin/bash

export BASE_HOSTNAME=$(hostname | rev | cut -d "-" -f2- | rev)
export BASE_FQDN=$(hostname -f | cut -d "." -f2-)
export ZOOKEEPER_ID=$(hostname | awk -F'-' '{print $NF+1}')

# Generate and print the config file
echo "Starting Stunnel with configuration:"
./stunnel_config_generator.sh | tee /tmp/stunnel.conf
echo ""

# starting Stunnel with final configuration
exec /usr/bin/stunnel /tmp/stunnel.conf
