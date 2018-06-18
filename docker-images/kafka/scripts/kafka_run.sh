#!/bin/bash
set +x

# volume for saving Kafka server logs
export KAFKA_VOLUME="/var/lib/kafka/"
# base name for Kafka server data dir
export KAFKA_LOG_BASE_NAME="kafka-log"
export KAFKA_APP_LOGS_BASE_NAME="logs"

export KAFKA_BROKER_ID=$(hostname | awk -F'-' '{print $NF}')
echo "KAFKA_BROKER_ID=$KAFKA_BROKER_ID"

# create data dir
export KAFKA_LOG_DIRS=$KAFKA_VOLUME$KAFKA_LOG_BASE_NAME$KAFKA_BROKER_ID
echo "KAFKA_LOG_DIRS=$KAFKA_LOG_DIRS"

# Disable Kafka's GC logging (which logs to a file)...
export GC_LOG_ENABLED="false"
# ... but enable equivalent GC logging to stdout
export KAFKA_GC_LOG_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps"

if [ -z "$KAFKA_LOG_LEVEL" ]; then
  KAFKA_LOG_LEVEL="INFO"
fi
if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/config/log4j.properties -Dkafka.root.logger.level=$KAFKA_LOG_LEVEL,CONSOLE"
fi

# enabling Prometheus JMX exporter as Java agent
if [ "$KAFKA_METRICS_ENABLED" = "true" ]; then
  export KAFKA_OPTS="-javaagent:/opt/prometheus/jmx_prometheus_javaagent.jar=9404:/opt/prometheus/config/config.yml"
fi

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_HOME"

# get broker rack if it's enabled
if [ -e $KAFKA_HOME/rack/rack.id ]; then
  export KAFKA_RACK=$(cat $KAFKA_HOME/rack/rack.id)
fi


# set up for encryption support
if [ "$KAFKA_ENCRYPTION_ENABLED" = "true" ]; then
    export ENC_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c${1:-32})
    echo "ENC_PASSWORD" $ENC_PASSWORD

    # Import certificates into keystore and truststore
    echo "Importing certificates for internal communication ..."
    keytool -keystore /var/lib/kafka/replication.truststore.jks -storepass $ENC_PASSWORD -noprompt -alias internal-ca -import -file /var/lib/kafka/internal-certs/internal-ca.crt
    openssl pkcs12 -export -in /var/lib/kafka/internal-certs/$HOSTNAME.crt -inkey /var/lib/kafka/internal-certs/$HOSTNAME.key -chain -CAfile /var/lib/kafka/internal-certs/internal-ca.crt -name $HOSTNAME -password pass:$ENC_PASSWORD -out /var/lib/kafka/$HOSTNAME-internal.p12
    keytool -importkeystore -deststorepass $ENC_PASSWORD -destkeystore /var/lib/kafka/replication.keystore.jks -srcstorepass $ENC_PASSWORD -srckeystore /var/lib/kafka/$HOSTNAME-internal.p12 -srcstoretype PKCS12
    echo "... end importing certificates"

    echo "Importing certificates for clients communication ..."
    keytool -keystore /var/lib/kafka/clients.truststore.jks -storepass $ENC_PASSWORD -noprompt -alias clients-ca -import -file /var/lib/kafka/clients-certs/clients-ca.crt
    keytool -keystore /var/lib/kafka/clients.truststore.jks -storepass $ENC_PASSWORD -noprompt -alias internal-ca -import -file /var/lib/kafka/internal-certs/internal-ca.crt
    openssl pkcs12 -export -in /var/lib/kafka/clients-certs/$HOSTNAME.crt -inkey /var/lib/kafka/clients-certs/$HOSTNAME.key -chain -CAfile /var/lib/kafka/clients-certs/clients-ca.crt -name $HOSTNAME -password pass:$ENC_PASSWORD -out /var/lib/kafka/$HOSTNAME-clients.p12
    keytool -importkeystore -deststorepass $ENC_PASSWORD -destkeystore /var/lib/kafka/clients.keystore.jks -srcstorepass $ENC_PASSWORD -srckeystore /var/lib/kafka/$HOSTNAME-clients.p12 -srcstoretype PKCS12
    echo "... end importing certificates"

    # Generate the SSL healthcheck config file
    ./kafka_healthcheck_ssl_config.sh | tee /tmp/healthcheck.properties

    export KAFKA_SECURITY="listener.name.replication.ssl.keystore.location=/var/lib/kafka/replication.keystore.jks
listener.name.replication.ssl.keystore.password=${ENC_PASSWORD}
listener.name.replication.ssl.truststore.location=/var/lib/kafka/replication.truststore.jks
listener.name.replication.ssl.truststore.password=${ENC_PASSWORD}
listener.name.client.ssl.keystore.location=/var/lib/kafka/clients.keystore.jks
listener.name.client.ssl.keystore.password=${ENC_PASSWORD}
listener.name.client.ssl.truststore.location=/var/lib/kafka/clients.truststore.jks
listener.name.client.ssl.truststore.password=${ENC_PASSWORD}"

    export KAFKA_LISTENER_SECURITY_PROTOCOL="SSL"
fi

# Generate and print the config file
echo "Starting Kafka with configuration:"
./kafka_config_generator.sh | tee /tmp/strimzi.properties
echo ""

if [ -z "$KAFKA_HEAP_OPTS" -a -n "${DYNAMIC_HEAP_FRACTION}" ]; then
    . ./dynamic_resources.sh
    # Calculate a max heap size based some DYNAMIC_HEAP_FRACTION of the heap
    # available to a jvm using 100% of the GCroup-aware memory
    # up to some optional DYNAMIC_HEAP_MAX
    CALC_MAX_HEAP=$(get_heap_size ${DYNAMIC_HEAP_FRACTION} ${DYNAMIC_HEAP_MAX})
    if [ -n "$CALC_MAX_HEAP" ]; then
      export KAFKA_HEAP_OPTS="-Xms${CALC_MAX_HEAP} -Xmx${CALC_MAX_HEAP}"
    fi
fi

# starting Kafka server with final configuration
exec $KAFKA_HOME/bin/kafka-server-start.sh /tmp/strimzi.properties
