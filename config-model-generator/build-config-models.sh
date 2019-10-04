#!/bin/bash

set -e

# Read the kafka versions file and create an array of version strings
declare -a versions
finished=0
counter=0
while [ $finished -lt 1 ] 
do
    version=$(yq read ../kafka-versions.yaml "[${counter}].version")

    if [ "$version" = "null" ]
    then
        finished=1
    else
        versions+=("$version")
        counter=$((counter + 1))
    fi 
done

if [ "$1" = "build" ]
then
    # Always delete existing files so when kafka-versions changes we remove
    # models for unsupported versions
    rm ../cluster-operator/src/main/resources/kafka-*-config-model.json || true

    for version in "${versions[@]}"
    do
        mvn ${MVN_ARGS} verify exec:java \
        "-Dkafka-metadata-version=$version" \
        "-Dconfig-model-file=../cluster-operator/src/main/resources/kafka-${version}-config-model.json"
    done
else
    # Clean up the last version in the file?
    mvn ${MVN_ARGS} clean "-Dkafka-metadata-version=${versions[-1]}"
fi
