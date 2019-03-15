#!/usr/bin/env bash
set -e

java_images="operator"
kafka_images="kafka test-client"

# Kafka versions
function load_checksums {
    declare -Ag checksums
    while read line; do
        checksums[$(echo "$line" | cut -d ' ' -f 1)]=$(echo "${line,,}" | cut -d ' ' -f 2)
    done < <(sed -E -e '/^(#.*|[[:space:]]*)$/d' -e 's/^([0-9.]+)[[:space:]]+.*[[:space:]]+([[:alnum:]]+)$/\1 \2/g' ../kafka-versions)
}

#
# Support for alternate base images
# if ALTERNATE_BASE is defined, and there is a Dockerfile in the directory, use that Dockerfile
# $1 the component directory
#
function alternate_base {
  if [ -n "$ALTERNATE_BASE" -a -f "$1/$ALTERNATE_BASE/Dockerfile" ]; then
    echo "-f $ALTERNATE_BASE/Dockerfile"
  fi
}

function build {
    local targets="$@"
    local tag="${DOCKER_TAG:-latest}"
    local java_version=${JAVA_VERSION:-1.8.0}

    # Images not depending on Kafka version
    for image in $java_images; do
        DOCKER_BUILD_ARGS="$DOCKER_BUILD_ARGS --build-arg JAVA_VERSION=${java_version} $(alternate_base $image)" make -C "$image" "$targets"
    done

    # Images depending on Kafka version (possibly indirectly thru FROM)
    for kafka_version in ${!checksums[@]}; do
        sha=${checksums[$kafka_version]}
        for image in $kafka_images; do
            DOCKER_BUILD_ARGS="$DOCKER_BUILD_ARGS --build-arg JAVA_VERSION=${java_version} --build-arg KAFKA_VERSION=${kafka_version} --build-arg KAFKA_SHA512=${sha} $(alternate_base $image)" \
            DOCKER_TAG="${tag}-kafka-${kafka_version}" \
            BUILD_TAG="build-kafka-${kafka_version}" \
            make -C "$image" "$targets"
        done
    done
}

load_checksums
build $@

