#!/bin/sh
set -e

export VERSION=${COMMIT:-latest}
export PULL_REQUEST=${PULL_REQUEST:-true}
export TAG=${TAG:-latest}
export BRANCH=${TRAVIS_BRANCH:-master}
export DOCKER_ORG=${DOCKER_ORG:-$USER}
export DOCKER_REGISTRY=${DOCKER_REGISTRY:-docker.io}

make docker_build

if [ "$PULL_REQUEST" != "false" ] || [ "$BRANCH" != "master" ] ; then
    echo "Building Pull Request or side branch - nothing to push"
else
    echo "Login into Docker Hub ..."
    docker login -u $DOCKER_USER -p $DOCKER_PASS
    
    make docker_push
fi