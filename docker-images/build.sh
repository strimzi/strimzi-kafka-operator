#!/usr/bin/env bash
set -e

source $(dirname $(realpath $0))/../multi-platform-support.sh
source $(dirname $(realpath $0))/../tools/kafka-versions-tools.sh

# Image directories
base_images="base"
java_images="operator"
kafka_image="kafka"
kafka_images="kafka test-client"

function dependency_check { 

    # Check for bash >= 4
    if [ -z ${BASH_VERSINFO+x} ]
    then
        >&2 echo "No bash version information available. Aborting."
        exit 1
    fi

    if [ "$BASH_VERSINFO" -lt 4 ]
    then 
        >&2 echo "You need bash version >= 4 to build Strimzi. Refer to HACKING.md for more information"
        exit 1
    fi

    # Check that yq is installed 
    command -v yq >/dev/null 2>&1 || { >&2 echo "You need yq installed to build Strimzi. Refer to HACKING.md for more information"; exit 1; }

}

# Support for alternate base images
# if ALTERNATE_BASE is defined, and there is a Dockerfile in the directory, 
# use that Dockerfile $1 the component directory
function alternate_base {
    if [ -n "$ALTERNATE_BASE" ] && [ -f "$1/$ALTERNATE_BASE/Dockerfile" ]; then
      echo "-f $ALTERNATE_BASE/Dockerfile"
    fi
}

function build_kafka_images {

    local tag=$1
    local java_version=$2
    local targets=${@:3}
       
    # Set the cache folder to store the binary files
    binary_file_dir="$kafka_image/tmp"
    test -d "$binary_file_dir" || mkdir -p "$binary_file_dir"

    for kafka_version in "${!version_checksums[@]}"
    do
        echo "Creating container image for Kafka $kafka_version"

        expected_sha=${version_checksums[$kafka_version]}
        lib_directory=${version_libs[$kafka_version]}

        if [ ${version_binary_urls[$kafka_version]} ]
        then

            binary_file_url=${version_binary_urls[$kafka_version]}
            binary_file_name=$(basename "$binary_file_url")
            binary_file_path="$binary_file_dir/$binary_file_name"
            expected_kafka_checksum="$expected_sha  $binary_file_path"
        else
            >&2 echo "No URL for Kafka version $kafka_version"
        fi

        # Check if there is an existing binary file and checksum it against the checksum listed in the kafka-versions file.
        local get_file=0
        local do_checksum=1

        if [ -f "$binary_file_path" ]
        then
            echo "A file named $binary_file_name is already present in the build directory"

            # We need case insensitive matching as sha512sum output is lowercase and version file checksums are uppercase.
            shopt -s nocasematch
            if [[ "$expected_kafka_checksum" == "$(sha512sum "$binary_file_path")" ]]
            then
                do_checksum=0
            else
                echo "The checksum of the existing $binary_file_name did not match the expected checksum. This file will be replaced."
                get_file=1
            fi
            shopt -u nocasematch

        else
            get_file=1
        fi

        # If there is not an existing file, or there is one and it failed the checksum, then download/copy the binary
        if [ $get_file -gt 0 ]
        then
            echo "Fetching Kafka $kafka_version binaries from: $binary_file_url"
            curl --output "$binary_file_path" "$binary_file_url"
        fi

        # If we haven't already checksum'd the file do it now before the build.
        if [ $do_checksum -gt 0 ]
        then
            # Do the checksum on the Kafka binaries
            kafka_checksum_filepath="$binary_file_path.sha512"
            echo "$expected_kafka_checksum" > "$kafka_checksum_filepath"
            echo "Checking binary file: $binary_file_path"
            sha512sum --check "$kafka_checksum_filepath"
        fi

        # We now have a verified tar archive for this version of Kafka. Unpack it into the temp dir
        dist_dir="$binary_file_dir/$kafka_version"
        test -d "$dist_dir" || mkdir -p "$dist_dir"
        tar xvfz "$binary_file_path" -C "$dist_dir" --strip-components=1

        relative_dist_dir="./tmp/$kafka_version"

        for image in $kafka_images
        do

            DOCKER_BUILD_ARGS="$DOCKER_BUILD_ARGS --build-arg JAVA_VERSION=${java_version} --build-arg KAFKA_VERSION=${kafka_version} --build-arg KAFKA_DIST_DIR=${relative_dist_dir} --build-arg THIRD_PARTY_LIBS=${lib_directory} $(alternate_base "$image")" \
            DOCKER_TAG="${tag}-kafka-${kafka_version}" r="build-kafka-${kafka_version}" \
            KAFKA_VERSION="${kafka_version}" \
            THIRD_PARTY_LIBS="${lib_directory}" \
            make -C "$image" "$targets"

        done

        # Delete the unpacked tar file
        rm -r "$dist_dir"
    done

}

function clean_kafka_images {

    for image in $kafka_images
    do
        make -C "$image" "clean"
    done

}

function build {
   
    # This function comes from the tools/kafka-versions-tools.sh script and provides several associative arrays
    # version_binary_urls, version_checksums and version_libs which map from version string 
    # to source tar url (or file if specified), sha512 checksum for those tar files and third party library 
    # version respectively.
    get_version_maps
    
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

    if [[ $targets == "clean" ]]
    then 
        clean_kafka_images
    elif [[ $targets == *"clean"* ]]
    then
        # If the targets include clean plus other targets then do the clean first and then the build
        clean_kafka_images
        build_kafka_images "$tag" "$java_version" "$targets"
    else
        build_kafka_images "$tag" "$java_version" "$targets"
    fi
}

dependency_check
build "$@"
