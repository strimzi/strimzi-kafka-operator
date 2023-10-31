#!/usr/bin/env bash

RANDOM=$(shuf -i 60000-99999 -n 1)

KAFKA_VERSION=$1
BASE_IMAGE=$2
CONNECT_IMAGE=${3:-"strimzi/connect:$RANDOM"}

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

if [ -z "${KAFKA_VERSION}" ]; then
  echo "[WARN] Kafka version was not supplied, going to get latest version from kafka-versions.yaml"
  getLatestKafkaVersionFromYAML
fi

echo $(dirname "$0")
buildDockerImage

echo export CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN=$CONNECT_IMAGE