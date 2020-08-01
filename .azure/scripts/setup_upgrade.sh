#!/usr/bin/env bash

sed -i "s#:latest#:${DOCKER_TAG}#g" systemtest/src/main/resources/StrimziUpgradeST.json install/cluster-operator/050-Deployment-strimzi-cluster-operator.yaml
sed -i "s#strimzi/test-client:${DOCKER_TAG}#${DOCKER_REGISTRY}/strimzi/test-client:${DOCKER_TAG}#g" systemtest/src/main/resources/StrimziUpgradeST.json
sed -i "s#strimzi/#${DOCKER_REGISTRY}/strimzi/#g" install/cluster-operator/050-Deployment-strimzi-cluster-operator.yaml
sed -i "s#/opt/${DOCKER_REGISTRY}#/opt#g" install/cluster-operator/050-Deployment-strimzi-cluster-operator.yaml

cat install/cluster-operator/050-Deployment-strimzi-cluster-operator.yaml
cat systemtest/src/main/resources/StrimziUpgradeST.json
