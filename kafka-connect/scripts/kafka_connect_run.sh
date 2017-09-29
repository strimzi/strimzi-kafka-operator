#!/bin/bash

if [ -z "$KAFKA_CONNECT_BOOTSTRAP_SERVERS" ]; then
  KAFKA_CONNECT_BOOTSTRAP_SERVERS="kafka:9092"
fi

if [ -z "$KAFKA_CONNECT_GROUP_ID" ]; then
  KAFKA_CONNECT_GROUP_ID="connect-cluster"
fi

if [ -z "$KAFKA_CONNECT_OFFSET_STORAGE_TOPIC" ]; then
  KAFKA_CONNECT_OFFSET_STORAGE_TOPIC="${KAFKA_CONNECT_GROUP_ID}-offsets"
fi

if [ -z "$KAFKA_CONNECT_CONFIG_STORAGE_TOPIC" ]; then
  KAFKA_CONNECT_CONFIG_STORAGE_TOPIC="${KAFKA_CONNECT_GROUP_ID}-configs"
fi

if [ -z "$KAFKA_CONNECT_STATUS_STORAGE_TOPIC" ]; then
  KAFKA_CONNECT_STATUS_STORAGE_TOPIC="${KAFKA_CONNECT_GROUP_ID}-status"
fi

if [ -z "$KAFKA_CONNECT_KEY_CONVERTER" ]; then
  KAFKA_CONNECT_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter"
fi

if [ -z "$KAFKA_CONNECT_VALUE_CONVERTER" ]; then
  KAFKA_CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter"
fi

if [ -z "$KAFKA_CONNECT_PLUGIN_PATH" ]; then
  KAFKA_CONNECT_PLUGIN_PATH="${KAFKA_HOME}/plugins"
fi

# Write the config file
cat > /tmp/barnabas-connect.properties <<EOF
bootstrap.servers=${KAFKA_CONNECT_BOOTSTRAP_SERVERS}
group.id=${KAFKA_CONNECT_GROUP_ID}
offset.storage.topic=${KAFKA_CONNECT_OFFSET_STORAGE_TOPIC}
config.storage.topic=${KAFKA_CONNECT_CONFIG_STORAGE_TOPIC}
status.storage.topic=${KAFKA_CONNECT_STATUS_STORAGE_TOPIC}
key.converter=${KAFKA_CONNECT_KEY_CONVERTER}
value.converter=${KAFKA_CONNECT_VALUE_CONVERTER}
key.converter.schemas.enable=false
value.converter.schemas.enable=false
internal.key.converter=org.apache.kafka.connect.json.JsonConverter
internal.value.converter=org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable=false
internal.value.converter.schemas.enable=false
plugin.path=${KAFKA_CONNECT_PLUGIN_PATH}
EOF

echo "Starting Kafka connect with configuration:"
cat /tmp/barnabas-connect.properties
echo ""

# starting Kafka server with final configuration
exec $KAFKA_HOME/bin/connect-distributed.sh /tmp/barnabas-connect.properties
