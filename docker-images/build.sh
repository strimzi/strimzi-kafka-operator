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
        sha=${version_checksums[$kafka_version]}
        lib_directory=${version_libs[$kafka_version]}
        
        # If there is a file specified for this version of Kafka use that instead of the specified URL
        if [ ${version_binary_files[$kafka_version]} ]
        then 

            filepath=${version_binary_files[$kafka_version]}
            echo "Kafka binary file field was specified, using $filepath instead of URL"
            
            # Extract the filename from the filepath
            OIFS=$IFS
            IFS="/"
            read -a fparr <<< $filepath
            filename=${fparr[-1]}
            IFS=$OIFS
            
        elif [ ${version_binary_urls[$kafka_version]} ]
        then

            url=${version_binary_urls[$kafka_version]}
            echo "Downloading Kafka $kafka_version binaries from: $url"

            # Extract the filename from the URL
            OIFS=$IFS
            IFS="/"
            read -ra urlarr <<< "$url"
            filename=${urlarr[-1]}
            IFS=$OIFS
            
            filepath="$(mktemp -d)/$filename"

            curl --output "$filepath" "$url"

        fi
            
        # Do the checksum on the Kafka binaries
        kafka_checksum="$sha $filepath"
        kafka_checksum_filepath="$filepath.sha512"
        echo "$kafka_checksum" > "$kafka_checksum_filepath"
        sha512sum --check "$kafka_checksum_filepath"

        for image in $kafka_images
        do

            # Copy the Kafka binaries into the docker build context
            cp "$filepath" "./$image/$filename"

            DOCKER_BUILD_ARGS="$DOCKER_BUILD_ARGS --build-arg JAVA_VERSION=${java_version} --build-arg KAFKA_VERSION=${kafka_version} --build-arg KAFKA_SOURCE_FILENAME=${filename} --build-arg KAFKA_SHA512=${sha} --build-arg THIRD_PARTY_LIBS=${lib_directory} $(alternate_base "$image")" \
            DOCKER_TAG="${tag}-kafka-${kafka_version}" r="build-kafka-${kafka_version}" \
            KAFKA_VERSION="${kafka_version}" \
            THIRD_PARTY_LIBS="${lib_directory}" \
            make -C "$image" "$targets"
            
            # clean up the Kafka binaries
            rm "./$image/$filename"

        done
    done
}

dependency_check
build "$@"

