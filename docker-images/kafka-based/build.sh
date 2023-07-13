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

# Support for alternate base images
# if ALTERNATE_BASE is defined, and there is a Dockerfile in the directory, 
# use that Dockerfile $1 the component directory
function alternate_base {
    if [ -n "$ALTERNATE_BASE" ] && [ -f "$1/$ALTERNATE_BASE/Dockerfile" ]; then
      echo "-f $ALTERNATE_BASE/Dockerfile"
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
                DOCKER_BUILD_ARGS="$DOCKER_BUILD_ARGS --build-arg KAFKA_VERSION=${kafka_version} --build-arg KAFKA_DIST_DIR=${relative_dist_dir} --build-arg THIRD_PARTY_LIBS=${lib_directory} $(alternate_base "$image")" \
                DOCKER_TAG="${tag}-kafka-${kafka_version}" \
                BUILD_TAG="build-kafka-${kafka_version}" \
                KAFKA_VERSION="${kafka_version}" \
                THIRD_PARTY_LIBS="${lib_directory}"
        done
    done
}

dependency_check
build "$@"
