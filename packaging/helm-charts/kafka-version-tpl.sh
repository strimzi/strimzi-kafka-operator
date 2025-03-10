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
kafka_exporter_version="{{ template \"strimzi.image\" (merge . (dict \"key\" \"kafkaExporter\" \"version\" \"${default_kafka_version}\" \"defaultTagSuffix\" \"-kafka-${default_kafka_version}\")) }}"

for version in "${versions[@]}"
do
    kafka_exporter_version="{{ template \"strimzi.image\" (merge . (dict \"key\" \"kafkaExporter\" \"version\" \"${version}\" \"defaultTagSuffix\" \"-kafka-${version}\")) }}"
    cruise_control_version="{{ template \"strimzi.image\" (merge . (dict \"key\" \"cruiseControl\" \"version\" \"${version}\" \"defaultTagSuffix\" \"-kafka-${version}\")) }}"
    kafka_versions="${kafka_versions}
${version}={{ template \"strimzi.image\" (merge . (dict \"key\" \"kafka\" \"version\" \"${version}\" \"defaultTagSuffix\" \"-kafka-${version}\")) }}"
    kafka_connect_versions="${kafka_connect_versions}
${version}={{ template \"strimzi.image\" (merge . (dict \"key\" \"kafkaConnect\" \"version\" \"${version}\" \"defaultTagSuffix\" \"-kafka-${version}\")) }}"
    kafka_exporter_versions="${kafka_exporter_versions}
${version}={{ template \"strimzi.image\" (merge . (dict \"key\" \"kafkaExporter\" \"version\" \"${version}\" \"defaultTagSuffix\" \"-kafka-${version}\")) }}"
    kafka_mirror_maker_2_versions="${kafka_mirror_maker_2_versions}
${version}={{ template \"strimzi.image\" (merge . (dict \"key\" \"kafkaMirrorMaker2\" \"version\" \"${version}\" \"defaultTagSuffix\" \"-kafka-${version}\")) }}"
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

