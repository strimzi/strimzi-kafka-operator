#!/bin/sh -eux

# Move to root folder of strimzi
cd ../../../../

#run tests
if [[ ${IP_FAMILY} == "ipv4" || ${IP_FAMILY} == "dual" ]]; then
    DOCKER_REGISTRY=$(hostname --ip-address | grep -oE '\b([0-9]{1,3}\.){3}[0-9]{1,3}\b' | awk '$1 != "127.0.0.1" { print $1 }' | head -1)
elif [[ ${IP_FAMILY} == "ipv6" ]]; then
    DOCKER_REGISTRY="myregistry.local"
fi

export DOCKER_REGISTRY="$DOCKER_REGISTRY:5001"
echo "Using container registry:$DOCKER_REGISTRY"

mvn compile -pl config-model-generator -DskipTests -Dmaven.javadoc.skip=true --no-transfer-progress
mvn verify -pl systemtest -P ${TEST_PROFILE} \
    $([[ "${TEST_GROUPS}" != "" ]] && echo "-Dgroups=${TEST_GROUPS}" || echo "") \
    $([[ "${TESTS}" != "" ]] && echo "-Dit.test=${TESTS}" || echo "") \
    -DexcludedGroups="${EXCLUDED_TEST_GROUPS}" \
    -Dmaven.javadoc.skip=true \
    -Dfailsafe.rerunFailingTestsCount="${RERUN_FAILED_TEST_COUNT}" \
    -Djunit.jupiter.execution.parallel.enabled=true \
    -Djunit.jupiter.execution.parallel.config.fixed.parallelism="${PARALLEL_TEST_COUNT}" \
    --no-transfer-progress
