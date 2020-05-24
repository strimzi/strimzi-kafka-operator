#!/usr/bin/env bash
set +x

# Wait for the TLS sidecar to be ready and listen on port 2181
./kafka_pre_start.sh

export STRIMZI_BROKER_ID=$(hostname | awk -F'-' '{print $NF}')
echo "STRIMZI_BROKER_ID=${STRIMZI_BROKER_ID}"

# Disable Kafka's GC logging (which logs to a file)...
export GC_LOG_ENABLED="false"

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/custom-config/log4j.properties"
fi

rm /var/opt/kafka/kafka-ready /var/opt/kafka/zk-connected 2> /dev/null
export KAFKA_OPTS="$KAFKA_OPTS -javaagent:$(ls $KAFKA_HOME/libs/kafka-agent*.jar)=/var/opt/kafka/kafka-ready:/var/opt/kafka/zk-connected"

if [ "$KAFKA_JMX_ENABLED" = "true" ]; then
  KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.rmi.port=9999 -Dcom.sun.management.jmxremote=true -Djava.rmi.server.hostname=$(hostname -i) -Djava.net.preferIPv4Stack=true"

  if [ -n "$KAFKA_JMX_USERNAME" ]; then
    # Secure JMX port on 9999 with username and password
    JMX_ACCESS_FILE="/tmp/access.file"
    JMX_PASSWORD_FILE="/tmp/password.file"

    cat << EOF > "${JMX_ACCESS_FILE}"
${KAFKA_JMX_USERNAME} readonly
EOF

    cat << EOF > "${JMX_PASSWORD_FILE}"
$KAFKA_JMX_USERNAME $KAFKA_JMX_PASSWORD
EOF
    chmod 400 "${JMX_PASSWORD_FILE}"
    KAFKA_JMX_OPTS="${KAFKA_JMX_OPTS} -Dcom.sun.management.jmxremote.access.file=${JMX_ACCESS_FILE} -Dcom.sun.management.jmxremote.password.file=${JMX_PASSWORD_FILE}  -Dcom.sun.management.jmxremote.authenticate=true"
  else
    # expose the port insecurely
    KAFKA_JMX_OPTS="${KAFKA_JMX_OPTS} -Dcom.sun.management.jmxremote.authenticate=false"
  fi
fi

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export KAFKA_OPTS="${KAFKA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

KAFKA_OPTS="${KAFKA_OPTS} ${KAFKA_JMX_OPTS}"

# enabling Prometheus JMX exporter as Java agent
if [ "$KAFKA_METRICS_ENABLED" = "true" ]; then
  export KAFKA_OPTS="${KAFKA_OPTS} -javaagent:$(ls $KAFKA_HOME/libs/jmx_prometheus_javaagent*.jar)=9404:$KAFKA_HOME/custom-config/metrics-config.yml"
fi

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_HOME"

# Generate temporary keystore password
export CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)

mkdir -p /tmp/kafka

# Import certificates into keystore and truststore
./kafka_tls_prepare_certificates.sh

# Generate and print the config file
echo "Starting Kafka with configuration:"
./kafka_config_generator.sh | tee /tmp/strimzi.properties | sed -e 's/sasl.jaas.config=.*/sasl.jaas.config=[hidden]/g' -e 's/password=.*/password=[hidden]/g'
echo ""

if [ -z "$KAFKA_HEAP_OPTS" -a -n "${DYNAMIC_HEAP_FRACTION}" ]; then
    . ./dynamic_resources.sh
    # Calculate a max heap size based some DYNAMIC_HEAP_FRACTION of the heap
    # available to a jvm using 100% of the CGroup-aware memory
    # up to some optional DYNAMIC_HEAP_MAX
    CALC_MAX_HEAP=$(get_heap_size ${DYNAMIC_HEAP_FRACTION} ${DYNAMIC_HEAP_MAX})
    if [ -n "$CALC_MAX_HEAP" ]; then
      export KAFKA_HEAP_OPTS="-Xms${CALC_MAX_HEAP} -Xmx${CALC_MAX_HEAP}"
    fi
fi

. ./set_kafka_gc_options.sh

# starting Kafka server with final configuration
exec /usr/bin/tini -w -e 143 -- ${KAFKA_HOME}/bin/kafka-server-start.sh /tmp/strimzi.properties
