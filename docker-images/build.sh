#!/usr/bin/env bash
set -e

# Image directories
base_images="base"
java_images="operator"
kafka_images="kafka test-client"

function dependency_check { 

    # Check for bash >= 4
    if [ -z ${BASH_VERSION+x} ]
    then
        # For some reason the bash version is not set so we set is ourselves
        BASH_VERSION=$(/usr/bin/env bash --version |\
                       grep -o '[0-9]\+\(\.[0-9]\+\)*' |\
                       head -n 1 | cut -c 1)
    else
        # Bash version is set but we may need to clean it up depending on the 
        # installed version
        BASH_VERSION=$(echo "$BASH_VERSION" |\
                       grep -o '[0-9]\+\(\.[0-9]\+\)*' |\
                       head -n 1 | cut -c 1)
    fi

    if [ "$BASH_VERSION" -lt 4 ]
    then 
        echo -e "You need bash version >= 4 to build Strimzi.\nRefer to HACKING.md for more information"
        exit 1
    fi

    # Check that yq is installed 
    command -v yq >/dev/null 2>&1 || { printf "You need yq installed to build Strimzi.\nRefer to HACKING.md for more information"; exit 1; }

}

# Parse the Kafka versions yaml file and create maps from version string to 
# the checksum and third party library version strings
function parse_versions_file {

    declare -Ag checksums
    declare -Ag thirdpartylibs
    
    finished=0
    counter=0
    while [ $finished -lt 1 ] 
    do
        version=$(yq read ../kafka-versions.yaml "[${counter}].version")

        if [ "$version" = "null" ]
        then
            finished=1
        else
            checksum=$(yq read ../kafka-versions.yaml "[${counter}].checksum")

            thirdpartylib=$(yq read ../kafka-versions.yaml "[${counter}].third-party-libs")
 
            checksums[$version]=$checksum
            thirdpartylibs[$version]=$thirdpartylib
            
            counter=$((counter + 1))
        fi
        
    done
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
    for kafka_version in "${!checksums[@]}"
    do
        sha=${checksums[$kafka_version]}
        lib_directory=${thirdpartylibs[$kafka_version]}
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
parse_versions_file
build "$@"

