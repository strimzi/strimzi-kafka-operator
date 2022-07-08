#!/usr/bin/env bash
# shellcheck disable=SC2154,SC2034
# ^^^ Disables false-positives which should be ignored
set -e

VERSIONS_FILE="$(dirname "$(realpath "${BASH_SOURCE[0]}")")/../kafka-versions.yaml"

# Gets the default Kafka version and sets "default_kafka_version" variable
# to the corresponding version string.
function get_default_kafka_version {

    finished=0
    counter=0
    default_kafka_version="null"
    while [ $finished -lt 1 ] 
    do
        version="$(yq eval ".[${counter}].version" "$VERSIONS_FILE" )"

        if [ "$version" = "null" ]
        then
            finished=1
        else
            if [ "$(yq eval ".[${counter}].default" "$VERSIONS_FILE")" = "true" ]
            then
                if [ "$default_kafka_version" = "null" ]
                then
                    default_kafka_version=$version
                    finished=1
                else
                    # We have multiple defaults so there is an error in the versions file
                    >&2 echo "ERROR: There are multiple Kafka versions set as default"
                    unset default_kafka_version
                    exit 1
                fi
            fi
            counter=$((counter+1))
        fi
    done

    unset finished
    unset counter
    unset version

}

function get_kafka_versions {
    eval versions="($(yq eval '.[] | select(.supported == true) | .version' "$VERSIONS_FILE"))"
}

function get_kafka_urls {
    eval binary_urls="($(yq eval '.[] | select(.supported == true) | .url' "$VERSIONS_FILE"))"
}

function get_zookeeper_versions {
    eval zk_versions="($(yq eval '.[] | select(.supported == true) | .zookeeper' "$VERSIONS_FILE"))"
}

function get_kafka_checksums {
    eval checksums="($(yq eval '.[] | select(.supported == true) | .checksum' "$VERSIONS_FILE"))"
}

function get_kafka_third_party_libs {
    eval libs="($(yq eval '.[] | select(.supported == true) | .third-party-libs' "$VERSIONS_FILE"))"
}

function get_unique_kafka_third_party_libs {
    eval libs="($(yq eval '.[] | select(.supported == true) | .third-party-libs' "$VERSIONS_FILE" | sort -u))"
}

function get_kafka_protocols {
    eval protocols="($(yq eval '.[] | select(.supported == true) | .protocol' "$VERSIONS_FILE"))"
}

function get_kafka_formats {
    eval formats="($(yq eval '.[] | select(.supported == true) | .format' "$VERSIONS_FILE"))"
}

function get_kafka_does_not_support {
    eval does_not_support="($(yq eval '.[] | select(.supported == true) | .unsupported-features' "$VERSIONS_FILE"))"

    get_kafka_versions
    
    declare -Ag version_does_not_support
    for i in "${!versions[@]}"
    do 
        version_does_not_support[${versions[$i]}]=${does_not_support[$i]}
    done
}

# Parses the Kafka versions file and creates three associative arrays:
# "version_binary_urls": Maps from version string to url from which the kafka source 
# tar will be downloaded.
# "version_checksums": Maps from version string to sha512 checksum.
# "version_libs": Maps from version string to third party library version string.
function get_version_maps {
    get_kafka_versions
    get_kafka_urls
    get_kafka_checksums
    get_kafka_third_party_libs
    
    declare -Ag version_binary_urls
    declare -Ag version_checksums
    declare -Ag version_libs
    
    for i in "${!versions[@]}"
    do 
        version_binary_urls[${versions[$i]}]=${binary_urls[$i]}
        version_checksums[${versions[$i]}]=${checksums[$i]}
        version_libs[${versions[$i]}]=${libs[$i]}
    done
}
