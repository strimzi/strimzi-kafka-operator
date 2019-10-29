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

function build {
   
    # This function comes from the tools/kafka-versions-tools.sh script and provides several associative arrays
    # version_binary_urls, version_binary_file, version_checksums and version_libs which map from version string 
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

    # Images depending on Kafka version (possibly indirectly thru FROM)
    for kafka_version in "${!version_checksums[@]}"
    do
        echo "Creating container image for Kafka $kafka_version"

        expected_sha=${version_checksums[$kafka_version]}
        lib_directory=${version_libs[$kafka_version]}

        binary_file_dir="$kafka_image/tmp"
        
        # If there is a file specified for this version of Kafka use that instead of the specified URL
        if [ ${version_binary_files[$kafka_version]} ]
        then 

            copy_or_dowload="copy"

            custom_binary_file_path=${version_binary_files[$kafka_version]}
            binary_file_name=$(basename "$custom_binary_file_path")
            binary_file_path="$binary_file_dir/$binary_file_name"
            expected_kafka_checksum="$expected_sha  $binary_file_path"
            echo "Kafka binary file field was specified, using $custom_binary_file_path instead of URL"
            
        elif [ ${version_binary_urls[$kafka_version]} ]
        then

            copy_or_dowload="download"
            
            binary_file_url=${version_binary_urls[$kafka_version]}
            binary_file_name=$(basename "$binary_file_url")
            binary_file_path="$binary_file_dir/$binary_file_name"
            expected_kafka_checksum="$expected_sha  $binary_file_path"

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
            if [ $copy_or_dowload == "copy" ]
            then
                echo "Copying $binary_file_name to build directory"
                $CP "$custom_binary_file_path" "$binary_file_path" 
            else
                echo "Downloading Kafka $kafka_version binaries from: $binary_file_url"
                curl --output "$binary_file_path" "$binary_file_url"
            fi

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

        relative_binary_file_path="tmp/$binary_file_name"

        for image in $kafka_images
        do

            DOCKER_BUILD_ARGS="$DOCKER_BUILD_ARGS --build-arg JAVA_VERSION=${java_version} --build-arg KAFKA_VERSION=${kafka_version} --build-arg KAFKA_DIST_FILENAME=${relative_binary_file_path} --build-arg KAFKA_SHA512=${sha} --build-arg THIRD_PARTY_LIBS=${lib_directory} $(alternate_base "$image")" \
            DOCKER_TAG="${tag}-kafka-${kafka_version}" r="build-kafka-${kafka_version}" \
            KAFKA_VERSION="${kafka_version}" \
            THIRD_PARTY_LIBS="${lib_directory}" \
            make -C "$image" "$targets"
            
        done
    done
}

dependency_check
build "$@"
