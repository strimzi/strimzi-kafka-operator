#!/usr/bin/env bash

KAFKA_VERSION=$1
BASE_IMAGE=$2
CONNECT_IMAGE=${3:-"strimzi/connect-file-sink:latest"}

function getLatestKafkaVersionFromYAML() {
  local strimziRoot=$(git rev-parse --show-toplevel)
  KAFKA_VERSION=$(cat $strimziRoot/kafka-versions.yaml | yq eval '.[] | select(.default) | .version' -)
}

function buildDockerImage() {
  local dockerFileDir=$(dirname "$0")

  if [ -z "${BASE_IMAGE}" ]; then
    docker build $dockerFileDir --build-arg KAFKA_VERSION=$KAFKA_VERSION -t $CONNECT_IMAGE
  else
    docker build $dockerFileDir --build-arg KAFKA_VERSION=$KAFKA_VERSION --build-arg BASE_IMAGE=$BASE_IMAGE -t $CONNECT_IMAGE
  fi
}

# in case that Kafka version wasn't supplied or we specify "latest" as a version, we will take the default version from kafka-versions.yaml
if [ -z "${KAFKA_VERSION}" ] || [ "${KAFKA_VERSION}" == "latest" ]; then
  getLatestKafkaVersionFromYAML
fi

# replace the place holder with the correct version
BASE_IMAGE=$(echo $BASE_IMAGE | sed "s/KAFKA_VERSION/${KAFKA_VERSION}/g")

buildDockerImage

echo "export CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN=$CONNECT_IMAGE"