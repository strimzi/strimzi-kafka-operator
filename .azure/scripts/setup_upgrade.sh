#!/usr/bin/env bash

sed -i "s#quay.io/strimzi/test-client:latest#${DOCKER_REGISTRY}/${DOCKER_ORG}/test-client:${DOCKER_TAG}#g" systemtest/src/test/resources/upgrade/StrimziUpgradeST.json

sed -i "s#:latest#:${DOCKER_TAG}#g" packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#/opt/${DOCKER_REGISTRY}#/opt#g" packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#quay.io/strimzi/kafka#${DOCKER_REGISTRY}/${DOCKER_ORG}/kafka#g" packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#quay.io/strimzi/operator#${DOCKER_REGISTRY}/${DOCKER_ORG}/operator#g" packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
sed -i "s#quay.io/strimzi/jmxtrans#${DOCKER_REGISTRY}/${DOCKER_ORG}/jmxtrans#g" packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml

sed -i "s#strimzi/kafka:latest#${DOCKER_ORG}/kafka:${DOCKER_TAG}#g" systemtest/src/test/resources/upgrade/StrimziUpgradeST.json
sed -i "s#strimzi/operator:latest#${DOCKER_ORG}/operator:${DOCKER_TAG}#g" systemtest/src/test/resources/upgrade/StrimziUpgradeST.json

cat packaging/install/cluster-operator/*-Deployment-strimzi-cluster-operator.yaml
cat systemtest/src/test/resources/upgrade/StrimziUpgradeST.json
cat systemtest/src/test/resources/upgrade/StrimziDowngradeST.json

