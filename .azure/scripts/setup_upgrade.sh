#!/usr/bin/env bash

sed -i "s#strimzi/test-client:${DOCKER_TAG}#${DOCKER_ORG}/test-client:${DOCKER_TAG}#g" systemtest/src/test/resources/upgrade/StrimziUpgradeST.json

sed -i "s#:latest#:${DOCKER_TAG}#g" install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#/opt/${DOCKER_REGISTRY}#/opt#g" install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#strimzi/kafka#${DOCKER_ORG}/kafka#g" install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#strimzi/operator#${DOCKER_ORG}/operator#g" install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#strimzi/jmxtrans#${DOCKER_ORG}/jmxtrans#g" install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml

sed -i "s#strimzi/kafka:latest#${DOCKER_ORG}/kafka:${DOCKER_TAG}#g" systemtest/src/test/resources/upgrade/StrimziUpgradeST.json
sed -i "s#strimzi/operator:latest#${DOCKER_ORG}/operator:${DOCKER_TAG}#g" systemtest/src/test/resources/upgrade/StrimziUpgradeST.json

cat install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
cat systemtest/src/test/resources/upgrade/StrimziUpgradeST.json
cat systemtest/src/test/resources/upgrade/StrimziDowngradeST.json

