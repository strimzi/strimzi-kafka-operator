#!/bin/sh -eux

# Move to root folder of strimzi
cd ../../../../

eval $(minikube docker-env)

#run tests
export DOCKER_REGISTRY="$(kubectl get service registry -n kube-system -o=jsonpath='{.spec.clusterIP}'):80"
mvn compile -pl config-model-generator -DskipTests -Dmaven.javadoc.skip=true --no-transfer-progress
mvn verify -pl systemtest -P ${TEST_PROFILE} \
    $([[ "${TEST_GROUPS}" != "" ]] && echo "-Dgroups=${TEST_GROUPS}" || echo "") \
    $([[ "${TESTS}" != "" ]] && echo "-Dit.test=${TESTS}" || echo "") \
    -DexcludedGroups="${EXCLUDED_TEST_GROUPS}" \
    -Dmaven.javadoc.skip=true \
    -Dfailsafe.rerunFailingTestsCount=1 \
    -Djunit.jupiter.execution.parallel.enabled=true \
    -Djunit.jupiter.execution.parallel.config.fixed.parallelism=3 \
    --no-transfer-progress
