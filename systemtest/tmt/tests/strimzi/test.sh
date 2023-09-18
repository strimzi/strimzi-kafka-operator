#!/bin/sh -eux

# Move to root folder of strimzi
cd ../../../../

#run tests
export DOCKER_REGISTRY="$(hostname -I | awk '/^10\./ && !/127\.0\.0\.1/ { print $1 }' | head -1):5001"
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
