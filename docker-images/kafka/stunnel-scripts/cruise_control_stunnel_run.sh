#!/usr/bin/env bash


# Generate and print the config file
echo "Starting Stunnel with configuration:"
${STUNNEL_HOME}/cruise_control_stunnel_config_generator.sh | tee /tmp/stunnel.conf
echo ""

# starting Stunnel with final configuration
exec /usr/bin/stunnel /tmp/stunnel.conf
