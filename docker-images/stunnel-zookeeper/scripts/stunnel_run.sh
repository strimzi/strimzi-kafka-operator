#!/bin/bash

CERT=/etc/stunnel/certs/internal-ca.crt
declare -a PORTS=("2888" "3888")
export BASE_HOSTNAME=$(hostname | rev | cut -d "-" -f2- | rev)
export BASE_FQDN=$(hostname -f | cut -d "." -f2-)

# Generate and print the config file
echo "Starting Stunnel with configuration:"
./stunnel_config_generator.sh | tee /tmp/stunnel.conf
echo ""

# starting Stunnel with final configuration
exec stunnel /tmp/stunnel.conf
