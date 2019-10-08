#!/usr/bin/env bash
set -e

source $(dirname $(realpath $0))/../tools/kafka-versions-tools.sh

# Image directories
base_images="base"
java_images="operator"
kafka_images="kafka test-client"

function dependency_check { 

    # Check for bash >= 4
    if [ -z ${BASH_VERSINFO+x} ]
    then
        echo -e "No bash version information available. Aborting."
        exit 1
    fi

    if [ "$BASH_VERSINFO" -lt 4 ]
    then 
        echo -e "You need bash version >= 4 to build Strimzi.\nRefer to HACKING.md for more information"
        exit 1
    fi

    # Check that yq is installed 
    command -v yq >/dev/null 2>&1 || { printf "You need yq installed to build Strimzi.\nRefer to HACKING.md for more information"; exit 1; }

}

# Support for alternate base images
# if ALTERNATE_BASE is defined, and there is a Dockerfile in the directory, 
# use that Dockerfile $1 the component directory
function alternate_base {
    if [ -n "$ALTERNATE_BASE" ] && [ -f "$1/$ALTERNATE_BASE/Dockerfile" ]; then
      echo "-f $ALTERNATE_BASE/Dockerfile"
    fi
}

function build {
   
    # This function comes from the tools/kafka-versions-tools.sh script and provides two associative arrays
    # versions_checksums and versions_libs which map from version string to sha512 checksum and third party
    # library version respectively
    get_checksum_lib_maps
    
    local targets=$*
    local tag="${DOCKER_TAG:-latest}"
    local java_version="${JAVA_VERSION:-1.8.0}"

    # Base images
    for image in $base_images
    do
        DOCKER_BUILD_ARGS="$DOCKER_BUILD_ARGS --build-arg JAVA_VERSION=${java_version} $(alternate_base $image)" make -C "$image" "$targets"
    done

    # Images not depending on Kafka version
    for image in $java_images
    do
        DOCKER_BUILD_ARGS="$DOCKER_BUILD_ARGS --build-arg JAVA_VERSION=${java_version} $(alternate_base $image)" make -C "$image" "$targets"
    done

    # Images depending on Kafka version (possibly indirectly thru FROM)
    for kafka_version in "${!version_checksums[@]}"
    do
        sha=${version_checksums[$kafka_version]}
        lib_directory=${version_libs[$kafka_version]}
        for image in $kafka_images
        do
            DOCKER_BUILD_ARGS="$DOCKER_BUILD_ARGS --build-arg JAVA_VERSION=${java_version} --build-arg KAFKA_VERSION=${kafka_version} --build-arg KAFKA_SHA512=${sha} --build-arg THIRD_PARTY_LIBS=${lib_directory} $(alternate_base "$image")" \
            DOCKER_TAG="${tag}-kafka-${kafka_version}" \
            BUILD_TAG="build-kafka-${kafka_version}" \
            KAFKA_VERSION="${kafka_version}" \
            THIRD_PARTY_LIBS="${lib_directory}" \
            make -C "$image" "$targets"
        done
    done
}

dependency_check
build "$@"

