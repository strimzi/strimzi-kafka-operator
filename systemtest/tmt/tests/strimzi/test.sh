#!/bin/sh -eux

# Move to root folder of strimzi
cd ../../../../

# Build connect image
if [[ ${IP_FAMILY} == "ipv4" || ${IP_FAMILY} == "dual" ]]; then
	DOCKER_REGISTRY=$(hostname --ip-address | grep -oE '\b([0-9]{1,3}\.){3}[0-9]{1,3}\b' | awk '$1 != "127.0.0.1" { print $1 }' | head -1)
elif [[ ${IP_FAMILY} == "ipv6" ]]; then
	DOCKER_REGISTRY="myregistry.local"
fi

export DOCKER_REGISTRY="$DOCKER_REGISTRY:5001"

# Name of the image used within KafkaConnect build
export CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN="${DOCKER_REGISTRY}/${DOCKER_ORG}/connect-file-sink:latest"

# Set quay and release tag in case the pipeline is triggered by release
if [[ ${RELEASE:-False} == True ]]; then
	export DOCKER_REGISTRY="quay.io"
	export DOCKER_ORG="strimzi"
	export DOCKER_TAG="${PACKIT_TAG_NAME}"
fi

# Get latest Kafka version from kafka-versions.yaml
KAFKA_VERSION=$(cat kafka-versions.yaml | yq eval '.[] | select(.default) | .version' -)

# Configure the `KAFKA_TIERED_STORAGE_BASE_IMAGE` needed for the TieredStorageST
export KAFKA_TIERED_STORAGE_BASE_IMAGE="${DOCKER_REGISTRY}/${DOCKER_ORG}/kafka:${DOCKER_TAG}-kafka-${KAFKA_VERSION}"

echo "Using container registry '$DOCKER_REGISTRY'"
echo "Using container org '$DOCKER_ORG'"
echo "Using container tag '$DOCKER_TAG'"
echo "Using CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN=$CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN"

# Prepare upgrade files
./.azure/scripts/setup_upgrade.sh

mvn compile -pl config-model-generator -DskipTests -Dmaven.javadoc.skip=true --no-transfer-progress
mvn verify -pl systemtest -P ${TEST_PROFILE} \
    $([[ "${TEST_GROUPS}" != "" ]] && echo "-Dgroups=${TEST_GROUPS}" || echo "") \
    $([[ "${TESTS}" != "" ]] && echo "-Dit.test=${TESTS}" || echo "") \
    -DexcludedGroups="${EXCLUDED_TEST_GROUPS}" \
    -Dmaven.javadoc.skip=true \
    -Dfailsafe.rerunFailingTestsCount="${RERUN_FAILED_TEST_COUNT}" \
    -Djunit.jupiter.execution.parallel.enabled="${PARALLELISM_ENABLED}" \
    -Djunit.jupiter.execution.parallel.config.fixed.parallelism="${PARALLEL_TEST_COUNT}" \
    --no-transfer-progress
