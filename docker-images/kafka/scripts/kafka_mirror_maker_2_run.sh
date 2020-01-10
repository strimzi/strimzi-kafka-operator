#!/usr/bin/env bash
set +x

# Generate temporary keystore password
export CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)

# Create dir where keystores and truststores will be stored
mkdir -p /tmp/kafka/clusters

# Import cluster certificates into keystores and truststores
echo "Preparing MirrorMaker 2.0 cluster truststores"
IFS=$'\n' read -rd '' -a CLUSTERS <<< "$KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS"
for cluster in "${CLUSTERS[@]}"
do
    IFS='=' read -ra TRUSTED_CERTS_CLUSTER <<< "${cluster}"
    export clusterAlias="${TRUSTED_CERTS_CLUSTER[0]}"
    export trustedCerts="${TRUSTED_CERTS_CLUSTER[1]}"

    echo "Preparing MirrorMaker 2.0 truststores for cluster ${clusterAlias}"
    echo "  with trusted certs ${trustedCerts}"
    # $1 = trusted certs, $2 = TLS auth cert, $3 = TLS auth key, $4 = truststore path, $5 = keystore path, $6 = certs and key path
    ./kafka_mirror_maker_tls_prepare_certificates.sh \
        "${trustedCerts}" \
        "" \
        "" \
        "/tmp/kafka/clusters/${clusterAlias}.truststore.p12" \
        "/tmp/kafka/clusters/${clusterAlias}.keystore.p12" \
        "/opt/kafka/connect-certs" \
        "/opt/kafka/oauth-certs" \
        "/tmp/kafka/clusters/${clusterAlias}-oauth.keystore.p12"
done
echo "Preparing MirrorMaker 2.0 cluster truststores is complete"

# Include the file config provider in the Kafka Connect config
export KAFKA_CONNECT_FILE_CONFIG_PROVIDER="true"

# Generate and print the connector config file
echo "Creating connector configuration:"
./kafka_mirror_maker_2_connector_config_generator.sh | tee /tmp/strimzi-mirrormaker2-connector.properties | sed -e 's/password=.*/password=[hidden]/g'
echo ""

./kafka_connect_run.sh
