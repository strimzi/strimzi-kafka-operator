#!/bin/bash

set -e

if [ $1 = "build" ]
then
    # Always delete existing files
    rm ../cluster-operator/src/main/resources/kafka-*-config-model.json || true
    for kversion in $(sed -E -e '/^(#.*|[[:space:]]*)$/d' -e 's/^([0-9.]+)[[:space:]]+.*[[:space:]]+([[:alnum:]]+)[[:space:]]+.*$/\1/g' ../kafka-versions)
    do
      mvn ${MVN_ARGS} verify exec:java \
        "-Dkafka-metadata-version=$kversion" \
        "-Dconfig-model-file=../cluster-operator/src/main/resources/kafka-${kversion}-config-model.json"
    done
else
    mvn ${MVN_ARGS} clean "-Dkafka-metadata-version=$(sed -E -e '/^(#.*|[[:space:]]*)$/d' -e 's/^([0-9.]+)[[:space:]]+.*[[:space:]]+([[:alnum:]]+)[[:space:]]+.*$/\1/g' ../kafka-versions | tail -1)"
fi