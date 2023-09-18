#!/bin/sh -eux

# Move to root folder of strimzi
cd ../../../../

#run tests
export DOCKER_REGISTRY="$(ip -4 addr | grep -oE 'inet [0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+' |  awk -F'/' '{print $1}' | sort | awk -F' ' '{print $2}' | head -1):5001"
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
