#!/usr/bin/env bash
set -e

image="test-client"
PROJECT_NAME=$image
RELEASE_VERSION=$(cat ../release.version)
DOCKERFILE_DIR="./"
DOCKER_REGISTRY=${DOCKER_REGISTRY:-docker.io}

# Kafka versions
function load_checksums {
    declare -Ag checksums
    while read line; do
        checksums[$(echo "$line" | cut -d ' ' -f 1)]=$(echo "${line,,}" | cut -d ' ' -f 2)
    done < <(sed -E -e '/^(#.*|[[:space:]]*)$/d' -e 's/^([0-9.]+)[[:space:]]+.*[[:space:]]+([[:alnum:]]+)$/\1 \2/g' ../kafka-versions)
}

function build {
    local targets="$@"
    local tag="${DOCKER_TAG:-latest}"
    local java_version=${JAVA_VERSION:-1.8.0}

    # Images depending on Kafka version (possibly indirectly thru FROM)
    for kafka_version in ${!checksums[@]}; do
        sha=${checksums[$kafka_version]}
        DOCKER_BUILD_ARGS="$DOCKER_BUILD_ARGS --build-arg JAVA_VERSION=${java_version} --build-arg KAFKA_VERSION=${kafka_version} --build-arg KAFKA_SHA512=${sha}" \
        DOCKER_TAG="${tag}-kafka-${kafka_version}" \
        BUILD_TAG="build-kafka-${kafka_version}" \

        if [[ $targets == *"docker_build"* ]]; then
            mvn $MVN_ARGS install
            echo "docker build -q $DOCKER_BUILD_ARGS --build-arg strimzi_version=$RELEASE_VERSION -t strimzi/$PROJECT_NAME:latest $DOCKERFILE_DIR"
            docker build -q $DOCKER_BUILD_ARGS --build-arg strimzi_version=$RELEASE_VERSION -t strimzi/$PROJECT_NAME:latest $DOCKERFILE_DIR

            echo "docker tag strimzi/$PROJECT_NAME:latest strimzi/$PROJECT_NAME:$BUILD_TAG"
            docker tag strimzi/$PROJECT_NAME:latest strimzi/$PROJECT_NAME:$BUILD_TAG
        fi

        if [[ $targets == *"docker_tag"* ]]; then
            echo "docker tag strimzi/$PROJECT_NAME:$BUILD_TAG $DOCKER_REGISTRY/$DOCKER_ORG/$PROJECT_NAME:$DOCKER_TAG"
            docker tag strimzi/$PROJECT_NAME:$BUILD_TAG $DOCKER_REGISTRY/$DOCKER_ORG/$PROJECT_NAME:$DOCKER_TAG
        fi

        if [[ $targets == *"docker_push"* ]]; then
            echo "docker tag strimzi/$PROJECT_NAME:$BUILD_TAG $DOCKER_REGISTRY/$DOCKER_ORG/$PROJECT_NAME:$DOCKER_TAG"
            docker tag strimzi/$PROJECT_NAME:$BUILD_TAG $DOCKER_REGISTRY/$DOCKER_ORG/$PROJECT_NAME:$DOCKER_TAG

            echo "docker push $DOCKER_REGISTRY/$DOCKER_ORG/$PROJECT_NAME:$DOCKER_TAG"
            docker push $DOCKER_REGISTRY/$DOCKER_ORG/$PROJECT_NAME:$DOCKER_TAG
        fi
    done
}

load_checksums
build $@

