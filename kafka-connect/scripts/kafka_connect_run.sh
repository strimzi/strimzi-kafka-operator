#!/bin/bash
set -e

# Start bash if requested. Otherwise start Kafka Connect
if [ "$1" = 'bash' ]; then
    exec "$@"
else
  if [ -z "$KAFKA_CONNECT_BOOTSTRAP_SERVERS" ]; then
    export KAFKA_CONNECT_BOOTSTRAP_SERVERS="kafka:9092"
  fi

  if [ -z "$KAFKA_CONNECT_GROUP_ID" ]; then
    export KAFKA_CONNECT_GROUP_ID="connect-cluster"
  fi

  if [ -z "$KAFKA_CONNECT_OFFSET_STORAGE_TOPIC" ]; then
    export KAFKA_CONNECT_OFFSET_STORAGE_TOPIC="${KAFKA_CONNECT_GROUP_ID}-offsets"
  fi

  if [ -z "$KAFKA_CONNECT_CONFIG_STORAGE_TOPIC" ]; then
    export KAFKA_CONNECT_CONFIG_STORAGE_TOPIC="${KAFKA_CONNECT_GROUP_ID}-configs"
  fi

  if [ -z "$KAFKA_CONNECT_STATUS_STORAGE_TOPIC" ]; then
    export KAFKA_CONNECT_STATUS_STORAGE_TOPIC="${KAFKA_CONNECT_GROUP_ID}-status"
  fi

  if [ -z "$KAFKA_CONNECT_KEY_CONVERTER" ]; then
    export KAFKA_CONNECT_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter"
  fi

  if [ -z "$KAFKA_CONNECT_VALUE_CONVERTER" ]; then
    export KAFKA_CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter"
  fi

  if [ -z "$KAFKA_CONNECT_PLUGIN_PATH" ]; then
    export KAFKA_CONNECT_PLUGIN_PATH="${KAFKA_HOME}/plugins"
  fi

  # Write the config file
  cat > /tmp/barnabas-connect.properties <<EOF
rest.port=8083
rest.advertised.host.name=$(hostname -I)
rest.advertised.port=8083
bootstrap.servers=${KAFKA_CONNECT_BOOTSTRAP_SERVERS}
group.id=${KAFKA_CONNECT_GROUP_ID}
offset.storage.topic=${KAFKA_CONNECT_OFFSET_STORAGE_TOPIC}
config.storage.topic=${KAFKA_CONNECT_CONFIG_STORAGE_TOPIC}
status.storage.topic=${KAFKA_CONNECT_STATUS_STORAGE_TOPIC}
key.converter=${KAFKA_CONNECT_KEY_CONVERTER}
value.converter=${KAFKA_CONNECT_VALUE_CONVERTER}
key.converter.schemas.enable=${KAFKA_CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE:-true}
value.converter.schemas.enable=${KAFKA_CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE:-true}
internal.key.converter=org.apache.kafka.connect.json.JsonConverter
internal.value.converter=org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable=${KAFKA_CONNECT_INTERNAL_KEY_CONVERTER_SCHEMAS_ENABLE:-false}
internal.value.converter.schemas.enable=${KAFKA_CONNECT_INTERNAL_VALUE_CONVERTER_SCHEMAS_ENABLE:-false}
plugin.path=${KAFKA_CONNECT_PLUGIN_PATH}
config.storage.replication.factor=${KAFKA_CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR:-3}
offset.storage.replication.factor=${KAFKA_CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR:-3}
status.storage.replication.factor=${KAFKA_CONNECT_STATUS_STORAGE_REPLICATION_FACTOR:-3}
EOF

  echo "Starting Kafka connect with configuration:"
  cat /tmp/barnabas-connect.properties
  echo ""

  # Disable Kafka's GC logging (which logs to a file)...
  export GC_LOG_ENABLED="false"
  # ... but enable equivalent GC logging to stdout
  export KAFKA_GC_LOG_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps"

  # starting Kafka server with final configuration
  exec $KAFKA_HOME/bin/connect-distributed.sh /tmp/barnabas-connect.properties
fi
