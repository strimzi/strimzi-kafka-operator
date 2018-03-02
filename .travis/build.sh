#!/bin/sh
set -e

export PULL_REQUEST=${PULL_REQUEST:-true}
export BRANCH=${BRANCH:-master}
export TAG=${TAG:-latest}
export DOCKER_ORG=${DOCKER_ORG:-$USER}
export DOCKER_REGISTRY=${DOCKER_REGISTRY:-docker.io}
export DOCKER_TAG=$TAG
export DOCKER_VERSION_ARG=${COMMIT:-latest}

make docker_build

if [ "$PULL_REQUEST" != "false" ] ; then
    echo "Building Pull Request - nothing to push"

elif [ "$TAG" = "latest" ] && [ "$BRANCH" != "master" ]; then
    echo "Not in master branch and not in release tag - nothing to push"
else
    echo "Login into Docker Hub ..."
    docker login -u $DOCKER_USER -p $DOCKER_PASS
    
    make docker_push
    make docu_pushtowebsite
fi

echo "Running systemtests"
./systemtest/scripts/run_tests.sh ${SYSTEMTEST_ARGS}