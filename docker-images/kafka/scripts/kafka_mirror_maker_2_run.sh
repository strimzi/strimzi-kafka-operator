#!/usr/bin/env bash
set +x

# Use generated keystore password
export CERTS_STORE_PASSWORD=${STRIMZI_KAFKA_MIRRORMAKER_2_CLUSTER_TRUSTSTORE_PASSWORD}

# Create dir where keystores and truststores will be stored
mkdir -p /tmp/kafka/clusters

# Import cluster certificates into keystores and truststores
echo "Preparing MirrorMaker 2.0 cluster truststores"
IFS=$'\n' read -rd '' -a CLUSTERS <<< "$STRIMZI_KAFKA_MIRRORMAKER_2_CLUSTERS_TRUSTED_CERTS"
for cluster in "${CLUSTERS[@]}"
do
    IFS='=' read -ra CLUSTER_TRUSTED_CERTS <<< "${cluster}"
    export clusterAlias="${CLUSTER_TRUSTED_CERTS[0]}"
    export trustedCerts="${CLUSTER_TRUSTED_CERTS[1]}"

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

./kafka_connect_run.sh
