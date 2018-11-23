#!/bin/bash
set -e

if [ -n "$RELEASE_VERSION" ]; then
    strimzi_version="$RELEASE_VERSION"
else
    strimzi_version="latest"
fi

java_images="java-base"
# Note dependency order of the following images
stunnel_images="stunnel-base zookeeper-stunnel kafka-stunnel entity-operator-stunnel"
kafka_images="kafka-base kafka kafka-connect kafka-mirror-maker zookeeper test-client kafka-connect kafka-connect/s2i"

# Kafka versions
function load_checksums {
    declare -Ag checksums
    while read line; do
        checksums[$(echo "$line" | cut -d ' ' -f 1)]=$(echo "$line" | cut -d ' ' -f 2)
    done < <(sed -E -e '/^(#.*|[[:space:]]*)$/d' -e 's/^([0-9.]+)[[:space:]]+.*[[:space:]]+([[:alnum:]]+)$/\1 \2/g' ../kafka-versions)
}

function build {
    targets="$@"
    build_args="$DOCKER_BUILD_ARGS"
    # Images not depending on Kafka version
    for image in $(echo "$java_images $stunnel_images"); do
        DOCKER_TAG="${strimzi_version}" BUILD_TAG="latest-${kafka_version}" make -C "$image" "$targets"
    done
    if [ "$targets" != "docker_build" ]; then
        # Images depending on Kafka version (possibly indirectly thru FROM)
        for kafka_version in ${!checksums[@]}; do
            sha=${checksums[$kafka_version]}
            for image in $(echo "$kafka_images"); do
                #export DOCKER_BUILD_ARGS="--build-arg KAFKA_VERSION=${kafka_version} --build-arg KAFKA_SHA512=${sha} ${build_args}"
                if [ "$targets" == "docker_tag" ]; then
                    DOCKER_BUILD_ARGS="--build-arg KAFKA_VERSION=${kafka_version} --build-arg KAFKA_SHA512=${sha} ${build_args}" \
                    DOCKER_TAG="${strimzi_version}-kafka-${kafka_version}" make -C "$image" "docker_build"
                fi
                DOCKER_BUILD_ARGS="--build-arg KAFKA_VERSION=${kafka_version} --build-arg KAFKA_SHA512=${sha} ${build_args}" \
                DOCKER_TAG="${strimzi_version}-kafka-${kafka_version}" \
                make -C "$image" "$targets"
            done
        done
    fi
}

load_checksums
build $@

