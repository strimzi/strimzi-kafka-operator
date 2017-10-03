#!/bin/bash
set -e

# Start bash if requested. Otherwise start Kafka Connect
if [ "$1" = 'bash' ]; then
    exec "$@"
else
    exec ./kafka-connect-init-loader --namespace $NAMESPACE --config-map $CONFIG_MAP --plugin-path $PLUGIN_PATH
fi