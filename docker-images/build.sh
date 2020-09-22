#!/usr/bin/env bash
set -e

source $(dirname $(realpath $0))/../tools/kafka-versions-tools.sh
source $(dirname $(realpath $0))/../multi-platform-support.sh

# Image directories
base_images="base"
java_images="operator jmxtrans"
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


function fetch_and_unpack_kafka_binaries {

    local tag=$1
    local java_version=$2

    # Create map from version to checksumed and unpacked dist directory
    declare -Ag version_dist_dirs

    # Set the cache folder to store the binary files
    binary_file_dir="$kafka_image/tmp"
    test -d "$binary_file_dir" || mkdir -p "$binary_file_dir"

    for kafka_version in "${!version_checksums[@]}"
    do

        echo "Fetching and unpacking binary for Kafka $kafka_version"

        expected_sha=${version_checksums[$kafka_version]}

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
            echo "Checking binary archive file: $binary_file_path"
            sha512sum --check "$kafka_checksum_filepath"
        fi

        # We now have a verified tar archive for this version of Kafka. Unpack it into the temp dir
        dist_dir="$binary_file_dir/$kafka_version"
        # If an unpacked directory with the same name exists then delete it
        test -d "$dist_dir" && rm -r "$dist_dir"
        mkdir -p "$dist_dir"
        echo "Unpacking binary archive"
        tar xvfz "$binary_file_path" -C "$dist_dir" --strip-components=1

        # Store the folder address for use in the image build 
        version_dist_dirs["$kafka_version"]="./tmp/$kafka_version"

        # create a file listing all the jars with colliding class files in the Kafka dist
        # (on the assumption that this is OK). This file will be used after building the images to detect any collisions
        # added by the third-party jars mechanism.
        whilelist_file="$binary_file_dir/$kafka_version.whitelist"
        if [ ! -e $whilelist_file ]
        then
            unzipped_dir=`mktemp -d`
            ./extract-jars.sh "$dist_dir/libs" "$unzipped_dir"
            ./find-colliding-classes.sh "$unzipped_dir" | awk '{print $1}' | $SORT | $UNIQ > "$whilelist_file" || true
            rm -rf $unzipped_dir
        fi
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
    local java_version="${JAVA_VERSION:-11}"

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

    if [[ $targets == *"docker_build"* ]]
    then
        fetch_and_unpack_kafka_binaries "$tag" "$java_version"
    fi

    for kafka_version in "${!version_checksums[@]}"
    do
        lib_directory=${version_libs[$kafka_version]}

        if [[ $targets == *"docker_build"* ]]
        then
            relative_dist_dir=${version_dist_dirs[$kafka_version]}
        fi

        for image in $kafka_images
        do
            make -C "$image" "$targets" \
                DOCKER_BUILD_ARGS="$DOCKER_BUILD_ARGS --build-arg JAVA_VERSION=${java_version} --build-arg KAFKA_VERSION=${kafka_version} --build-arg KAFKA_DIST_DIR=${relative_dist_dir} --build-arg THIRD_PARTY_LIBS=${lib_directory} $(alternate_base "$image")" \
                DOCKER_TAG="${tag}-kafka-${kafka_version}" \
                BUILD_TAG="build-kafka-${kafka_version}" \
                KAFKA_VERSION="${kafka_version}" \
                THIRD_PARTY_LIBS="${lib_directory}"
        done
    done
}

dependency_check
build "$@"
