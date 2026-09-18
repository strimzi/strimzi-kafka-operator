#!/usr/bin/env bash
set -e

source $(dirname $(realpath $0))/../../tools/kafka-versions-tools.sh
source $(dirname $(realpath $0))/../../tools/multi-platform-support.sh

# Image directories
kafka_images="kafka"

function dependency_check {
    # Check for bash >= 4
    if [ -z ${BASH_VERSINFO+x} ]
    then
        >&2 echo "No bash version information available. Aborting."
        exit 1
    fi

    if [ "${BASH_VERSINFO[0]}" -lt 4 ]
    then 
        >&2 echo "You need bash version >= 4 to build Strimzi. Refer to DEV_GUIDE.md for more information"
        exit 1
    fi

    # Check that yq is installed
    command -v yq >/dev/null 2>&1 || { >&2 echo "You need yq installed to build Strimzi. Refer to DEV_GUIDE.md for more information"; exit 1; }
}

# Support for alternate base images and image variants.
# DOCKER_FILE, if set, names the Dockerfile to build and takes precedence. Otherwise, if
# ALTERNATE_BASE is defined and there is a Dockerfile in that directory, use that Dockerfile.
# $1 is the component directory.
function docker_file {
    if [ -n "$DOCKER_FILE" ]; then
      echo "$DOCKER_FILE"
    elif [ -n "$ALTERNATE_BASE" ] && [ -f "$1/$ALTERNATE_BASE/Dockerfile" ]; then
      echo "$ALTERNATE_BASE/Dockerfile"
    else
      echo "Dockerfile"
    fi
}


function build {
    # This function comes from the tools/kafka-versions-tools.sh script and provides several associative arrays
    # version_binary_urls, version_checksums and version_libs which map from version string 
    # to source tar url (or file if specified), sha512 checksum for those tar files and third party library 
    # version respectively.
    get_version_maps
    
    local targets=$*
    local tag="${DOCKER_TAG:-latest}"

    for kafka_version in "${!version_checksums[@]}"
    do
        lib_directory=${version_libs[$kafka_version]}

        if [[ $targets == *"docker_build"* ]]
        then
            relative_dist_dir="./tmp/$kafka_version"
        fi

        for image in $kafka_images
        do
            make -C "$image" "$targets" \
                DOCKER_BUILD_ARGS="$DOCKER_BUILD_ARGS --build-arg KAFKA_VERSION=${kafka_version} --build-arg KAFKA_DIST_DIR=${relative_dist_dir} --build-arg THIRD_PARTY_LIBS=${lib_directory}" \
                DOCKER_FILE="$(docker_file "$image")" \
                DOCKER_TAG="${tag}-kafka-${kafka_version}" \
                BUILD_TAG="latest-kafka-${kafka_version}" \
                KAFKA_VERSION="${kafka_version}" \
                THIRD_PARTY_LIBS="${lib_directory}"
        done
    done
}

dependency_check
build "$@"
