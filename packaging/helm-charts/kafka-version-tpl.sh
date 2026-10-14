#!/usr/bin/env bash

source $(dirname $(realpath $0))/../../tools/kafka-versions-tools.sh
VERSIONS_FILE="$(dirname $(realpath $0))/../../kafka-versions.yaml"

out="$1"

# Read the Kafka versions file and create an array of version strings
get_kafka_versions

# Get the default_kafka_version
get_default_kafka_version

# Get the features that are not supported by the kafka versions
get_kafka_does_not_support

# Set the default images
kafka_exporter_version="{{ template \"strimzi.image\" (merge . (dict \"key\" \"kafkaExporter\" \"tagSuffix\" \"-kafka-${default_kafka_version}\")) }}"

# Default multi-platform image digests for quay.io/strimzi/kafka:latest-kafka-<version>.
# Refresh when publishing new images or changing supported Kafka versions.
declare -A kafka_image_digests=(
    ["4.2.0"]="sha256:379ad6201a9a3d6f72d4a69a2d134544f4dbb3c10feb2a6a60e3c52b41ed16cb"
    ["4.2.1"]="sha256:56faef470161bb1b856108c6dfa4f11f53f4ea71104736c9066e1df78f953fad"
    ["4.2.2"]="sha256:b87c118d2a895e362f23da2e2aa55a632941c140f62aa01b25b5ece70ae3bf9d"
    ["4.3.0"]="sha256:5ee2b5814103b6f450ad37c7198e7adb1b9ca820213fdadf930549bcb0c0d961"
    ["4.3.1"]="sha256:7d097c26074d1a3a01f479a8d0bae4835ee433bf55e6cbc5487a68e6bff0667e"
)

for version in "${versions[@]}"
do
    digest="${kafka_image_digests[$version]:?Missing default Kafka image digest for $version}"
    kafka_exporter_version="{{ template \"strimzi.image\" (merge . (dict \"key\" \"kafkaExporter\" \"tagSuffix\" \"-kafka-${version}\")) }}"
    cruise_control_version="{{ template \"strimzi.image\" (merge . (dict \"key\" \"cruiseControl\" \"tagSuffix\" \"-kafka-${version}\")) }}"
    kafka_versions="${kafka_versions}
${version}={{ template \"strimzi.image\" (merge . (dict \"key\" \"kafka\" \"defaultDigest\" \"${digest}\" \"tagSuffix\" \"-kafka-${version}\")) }}"
    kafka_connect_versions="${kafka_connect_versions}
${version}={{ template \"strimzi.image\" (merge . (dict \"key\" \"kafkaConnect\" \"defaultDigest\" \"${digest}\" \"tagSuffix\" \"-kafka-${version}\")) }}"
    kafka_exporter_versions="${kafka_exporter_versions}
${version}={{ template \"strimzi.image\" (merge . (dict \"key\" \"kafkaExporter\" \"tagSuffix\" \"-kafka-${version}\")) }}"
    kafka_mirror_maker_2_versions="${kafka_mirror_maker_2_versions}
${version}={{ template \"strimzi.image\" (merge . (dict \"key\" \"kafkaMirrorMaker2\" \"defaultDigest\" \"${digest}\" \"tagSuffix\" \"-kafka-${version}\")) }}"
done

kafka_versions=$(echo "$kafka_versions" | sed 's/^/                /g')
kafka_connect_versions=$(echo "$kafka_connect_versions" | sed 's/^/                /g')
kafka_exporter_versions=$(echo "$kafka_exporter_versions" | sed 's/^/                /g')
kafka_mirror_maker_2_versions=$(echo "$kafka_mirror_maker_2_versions" | sed 's/^/                /g')

cat >"$out" <<EOF
{{/* vim: set filetype=mustache: */}}

{{/* This file is generated in helm-charts/Makefile */}}
{{/* DO NOT EDIT BY HAND                            */}}

{{/* Generate the kafka image map */}}
{{- define "strimzi.kafka.image.map" }}
            - name: STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE
              value: ${kafka_exporter_version}
            - name: STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE
              value: ${cruise_control_version}
            - name: STRIMZI_KAFKA_IMAGES
              value: | ${kafka_versions}
            - name: STRIMZI_KAFKA_CONNECT_IMAGES
              value: | ${kafka_connect_versions}
            - name: STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES
              value: | ${kafka_mirror_maker_2_versions}
{{- end -}}
EOF

