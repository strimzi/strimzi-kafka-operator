#!/usr/bin/env bash
set -e

source $(dirname $(realpath $0))/../tools/kafka-versions-tools.sh

# Parse the Kafka versions file and get a list of version strings in an array 
# called "versions"
get_kafka_versions

if [ "$1" = "build" ]
then
    # Always delete existing files so when kafka-versions changes we remove
    # models for unsupported versions
    rm ../cluster-operator/src/main/resources/kafka-*-config-model.json || true

    for version in "${versions[@]}"
    do
        mvn ${MVN_ARGS} verify exec:java \
        "-Pgenerate-model" \
        "-Dkafka-metadata-version=$version" \
        "-Dconfig-model-file=../cluster-operator/src/main/resources/kafka-${version}-config-model.json"
    done
else
    # Clean up the last version in the file?
    mvn ${MVN_ARGS} "-Pgenerate-model" clean "-Dkafka-metadata-version=${versions[-1]}"
fi
