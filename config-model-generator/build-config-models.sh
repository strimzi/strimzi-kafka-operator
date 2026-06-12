#!/usr/bin/env bash
set -e

source $(dirname $(realpath $0))/../tools/kafka-versions-tools.sh

# Parse the Kafka versions file and get a list of version strings in an array
# called "versions"
get_kafka_versions
get_version_maps

# Always delete existing files so when kafka-versions changes we remove
# models for unsupported versions
rm -f ../cluster-operator/src/main/resources/kafka-*-config-model.json || true

if [ "$1" = "build" ]
then
    for version in "${versions[@]}"
    do
        echo "Generating config model for Kafka $version (Maven version ${version_maven[$version]})"
        mvn ${MVN_ARGS} verify exec:java \
        "-Pgenerate-model" \
        "-Dkafka-metadata-version=${version_maven[$version]}" \
        "-Dconfig-model-file=../cluster-operator/src/main/resources/kafka-${version}-config-model.json"
    done
else
    # Clean up the last version in the file?
    mvn ${MVN_ARGS} "-Pgenerate-model" clean "-Dkafka-metadata-version=${versions[-1]}"
fi
