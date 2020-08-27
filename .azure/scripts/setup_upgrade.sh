#!/usr/bin/env bash

sed -i "s#:latest#:${DOCKER_TAG}#g" install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#strimzi/test-client:${DOCKER_TAG}#${DOCKER_REGISTRY}/strimzi/test-client:${DOCKER_TAG}#g" systemtest/src/main/resources/StrimziUpgradeST.json
sed -i "s#strimzi/#${DOCKER_REGISTRY}/strimzi/#g" install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#/opt/${DOCKER_REGISTRY}#/opt#g" install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#strimzi/#${DOCKER_ORG}/#g" install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#strimzi/kafka:latest#${DOCKER_ORG}/kafka:latest#g" systemtest/src/main/resources/StrimziUpgradeST.json
sed -i "s#strimzi/operator:latest#${DOCKER_ORG}/operator:latest#g" systemtest/src/main/resources/StrimziUpgradeST.json

cat install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
cat systemtest/src/main/resources/StrimziUpgradeST.json
