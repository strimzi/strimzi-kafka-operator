#!/bin/sh
set -e

export PULL_REQUEST=${PULL_REQUEST:-true}
export BRANCH=${BRANCH:-master}
export TAG=${TAG:-latest}
export DOCKER_ORG=${DOCKER_ORG:-$USER}
export DOCKER_TAG=$TAG
export DOCKER_VERSION_ARG=${COMMIT:-latest}
ORIG_DOCKER_REGISTRY=${DOCKER_REGISTRY:-docker.io}
export DOCKER_REGISTRY=localhost:5000

make docker_build

echo "Push to $DOCKER_REGISTRY"
make docker_push

echo "Running system tests"
./systemtest/scripts/run_tests.sh ${SYSTEMTEST_ARGS}
if [ ! $? ] ; then
  PASSED_TESTS="true"
fi

# If we're building master and the tests passed then publish the image to dockerhub
if [ "$TAG" = "latest" ] && [ "$BRANCH" = "master" ] && [ "$PASSED_TESTS" = "true" ] ; then
   export DOCKER_REGISTRY=${ORIG_DOCKER_REGISTRY}
   make docker_push
   make docu_pushtowebsite
fi