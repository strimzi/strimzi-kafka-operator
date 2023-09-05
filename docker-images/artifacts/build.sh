#!/usr/bin/env bash
set -e

source $(dirname $(realpath $0))/../../tools/kafka-versions-tools.sh
source $(dirname $(realpath $0))/../../tools/multi-platform-support.sh

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

function third_party_libs {
    # This function comes from the tools/kafka-versions-tools.sh script and provides a list of third-party-libs-directories.
    get_unique_kafka_third_party_libs

    # Copy JARs for the Kafka versions
    echo "Downloading Third Party libraries JARs"

    for version_lib in "${libs[@]}"
    do
        mvn dependency:copy-dependencies ${MVN_ARGS} -f kafka-thirdparty-libs/${version_lib}/pom.xml
        mkdir -p ./binaries/kafka-thirdparty-libs
        rm -f ./binaries/kafka-thirdparty-libs/${version_lib}.zip
        zip -j ./binaries/kafka-thirdparty-libs/${version_lib}.zip kafka-thirdparty-libs/${version_lib}/target/dependency/*
#        mkdir -p ./binaries/kafka-thirdparty-libs/${version_lib}
#        cp -v kafka-thirdparty-libs/${version_lib}/target/dependency/* ./binaries/kafka-thirdparty-libs/${version_lib}/
    done
}

function cruise_control {
    # Copy the Cruise Control JARs
    echo "Downloading Cruise Control JARs"

    mvn dependency:copy-dependencies ${MVN_ARGS} -f kafka-thirdparty-libs/cc/pom.xml
    mkdir -p ./binaries/kafka-thirdparty-libs
    rm -f ./binaries/kafka-thirdparty-libs/cc.zip
    zip -j ./binaries/kafka-thirdparty-libs/cc.zip kafka-thirdparty-libs/cc/target/dependency/*
#    mkdir -p ./binaries/kafka-thirdparty-libs/cc
#    cp -v kafka-thirdparty-libs/cc/target/dependency/* ./binaries/kafka-thirdparty-libs/cc/
}

function fetch_and_unpack_kafka_binaries {
    # This function comes from the tools/kafka-versions-tools.sh script and provides several associative arrays
    # version_binary_urls, version_checksums and version_libs which map from version string
    # to source tar url (or file if specified), sha512 checksum for those tar files and third party library
    # version respectively.
    get_version_maps

    # Set the cache folder to store the binary files
    binary_file_dir="binaries/kafka/archives"
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
            download_kafka_binaries_from_cdn "$binary_file_url" "$binary_file_path"
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
        dist_dir="binaries/kafka/$kafka_version"
        # If an unpacked directory with the same name exists then delete it
        test -d "$dist_dir" && rm -r "$dist_dir"
        mkdir -p "$dist_dir"
        echo "Unpacking binary archive"
        tar xvfz "$binary_file_path" -C "$dist_dir" --strip-components=1

        # create a file listing all the jars with colliding class files in the Kafka dist
        # (on the assumption that this is OK). This file will be used after building the images to detect any collisions
        # added by the third-party jars mechanism.
        ignorelist_file="$dist_dir.ignorelist"
        if [ ! -e $ignorelist_file ]
        then
            unzipped_dir=`mktemp -d`
            ./extract-jars.sh "$dist_dir/libs" "$unzipped_dir"
            ./find-colliding-classes.sh "$unzipped_dir" | awk '{print $1}' | $SORT | $UNIQ > "$ignorelist_file" || true
            rm -rf $unzipped_dir
            # Add ignored 3rd party libraries
            cat kafka-thirdparty-libs/${version_libs[$kafka_version]}/ignorelist >> $ignorelist_file
        fi

        # We extracted the files, so we just keep the archive and the ignorelist
        rm -rf $dist_dir
    done
}

function download_kafka_binaries_from_cdn {
    # This function extracts the remote path from the archive URL and tries to download from the CDN.
    # If it fails for any reason (e.g. 404), then it reverts back to the archive URL.
    local archive_url=$1
    local local_path=$2
    
    local remote_path="${archive_url/https:\/\/archive.apache.org\/dist\//}"
    local cdn_url="https://dlcdn.apache.org/${remote_path}"

    echo "Downloading from CDN: ${cdn_url}"
    local cdn_code=$(curl -L ${CURL_ARGS} -o "${local_path}" -w %{http_code} "${cdn_url}")
    echo "CDN HTTP code: ${cdn_code}"

    if [[ "${cdn_code}" != "200" ]]; then
        echo "Download from CDN failed. Retrying with archive: ${archive_url}"
        local archive_code=$(curl -L ${CURL_ARGS} -o "${local_path}" -w %{http_code} "${archive_url}")
        echo "Archive HTTP code: ${archive_code}"
    fi
}

dependency_check
third_party_libs
cruise_control
fetch_and_unpack_kafka_binaries
