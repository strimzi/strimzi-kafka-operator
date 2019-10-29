#!/usr/bin/env bash
set -e

VERSIONS_FILE="$(dirname $(realpath $0))/../kafka-versions.yaml"

# Gets the default Kafka version and sets "default_kafka_version" variable
# to the corresponding version string.
function get_default_kafka_version {

    finished=0
    counter=0
    while [ $finished -lt 1 ] 
    do
        version="$(yq read $VERSIONS_FILE [${counter}].version)"

        if [ "$version" = "null" ]
        then
            finished=1
        else
            if [ "$(yq read $VERSIONS_FILE [${counter}].default)" = "true" ]
            then
                default_kafka_version=$version
                finished=1
            fi
            counter=$((counter+1))
        fi
    done

    unset finished
    unset counter
    unset version

}

function get_kafka_versions {
    eval versions="$(yq read $VERSIONS_FILE '*.version' -j | tr '[],' '() ')"
}

function get_kafka_source_urls {
    eval kafka_source_urls="$(yq read $VERSIONS_FILE '*.url' -j | tr '[],' '() ')"
}

function get_kafka_source_files {
    eval kafka_source_files="$(yq read $VERSIONS_FILE '*.file' -j | tr '[],' '() ')"
}

function get_url_file_map {
    # This function loops over the yaml objects for each version, rather the just using a single line 
    # wild card, because the url and file entries are not guaranteed to always be present and 
    # therefore the length of their wild card arrays may not match the lengths of versions array.

    declare -Ag version_binary_urls
    declare -Ag version_binary_files

    finished=0
    counter=0
    while [ $finished -lt 1 ] 
    do
        version="$(yq read $VERSIONS_FILE [${counter}].version)"

        if [ "$version" = "null" ]
        then
            finished=1
        else

            url="$(yq read $VERSIONS_FILE [${counter}].url)"
            file="$(yq read $VERSIONS_FILE [${counter}].file)"
            
            # If the file field is present this takes precedence over any url entry.            
            if [ "$file" != null ]
            then
                version_binary_files["${version}"]="$file"
                source_count=$((source_count+1))
            elif [ "$url" != null ]
            then
                version_binary_urls["${version}"]="$url"
                source_count=$((source_count+1))
            else
                >&2 echo "No binary source specified for Kafka $version. Either 'url' or 'file' fields must be specified in the kafka-versions.yaml file"
                exit 1
            fi

            counter=$((counter+1))

        fi
    done

    unset finished
    unset counter
    unset version

}

function get_kafka_checksums {
    eval checksums="$(yq read $VERSIONS_FILE '*.checksum' -j | tr '[],' '() ')"
}

function get_kafka_third_party_libs {
    eval libs="$(yq read "$VERSIONS_FILE" '*.third-party-libs' -j | tr '[],' '() ')"
}

function get_kafka_protocols {
    eval protocols="$(yq read $VERSIONS_FILE '*.protocol' -j | tr '[],' '() ')"
}

function get_kafka_formats {
    eval formats="$(yq read $VERSIONS_FILE '*.format' -j | tr '[],' '() ')"
}

# Parses the Kafka versions file and creates three associative arrays:
# "version_source_urls": Maps from version string to url from which the kafka source 
# tar will be downloaded.
# "version_checksums": Maps from version string to sha512 checksum.
# "version_libs": Maps from version string to third party library version string.
function get_version_maps {

    get_kafka_versions
    get_kafka_checksums
    get_kafka_third_party_libs

    declare -Ag version_checksums
    declare -Ag version_libs

    for i in "${!versions[@]}"
    do 
        version_checksums[${versions[$i]}]=${checksums[$i]}
        version_libs[${versions[$i]}]=${libs[$i]}
    done
    
    get_url_file_map
    
}
