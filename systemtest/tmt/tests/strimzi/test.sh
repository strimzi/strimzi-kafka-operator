#!/bin/sh -eux

# Move to root folder of strimzi
cd ../../../../

#run tests
export DOCKER_REGISTRY="$(hostname -I | grep -oE '\b([0-9]{1,3}\.){3}[0-9]{1,3}\b' | sort | head -1):5001"
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
