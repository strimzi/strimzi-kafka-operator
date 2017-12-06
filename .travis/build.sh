#!/bin/sh
set -e

export PULL_REQUEST=${PULL_REQUEST:-true}
echo $PULL_REQUEST
export BRANCH=${BRANCH:-master}
echo $BRANCH
export TAG=${TAG:-latest}
echo $TAG
export DOCKER_ORG=${DOCKER_ORG:-$USER}
echo $DOCKER_ORG
export DOCKER_REGISTRY=${DOCKER_REGISTRY:-docker.io}
echo $DOCKER_REGISTRY
export DOCKER_TAG=$TAG
echo $DOCKER_TAG
export DOCKER_VERSION_ARG=${COMMIT:-latest}
echo $DOCKER_VERSION_ARG

make docker_build

if [ "$PULL_REQUEST" != "false" ]; then
    echo "Building Pull Request - nothing to push"
elif [ "$TAG" = "latest" ] && [ "$BRANCH" != "master" ]; then
    echo "Not in master branch and not in release tag - nothing to push"
else
    echo "Login into Docker Hub ..."
    docker login -u $DOCKER_USER -p $DOCKER_PASS
    
    make docker_push
fi
