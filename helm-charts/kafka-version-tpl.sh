#!/usr/bin/env bash

source $(dirname $(realpath $0))/../tools/kafka-versions-tools.sh

out="$1"

# Read the Kafka versions file and create an array of version strings
get_kafka_versions

# Get the default_kafka_version
get_default_kafka_version

# Get the features that are not supported by the kafka versions
get_kafka_does_not_support

# Set the default images
entity_operator_tls_sidecar_version="{{ default .Values.tlsSidecarEntityOperator.image.repository .Values.imageRepositoryOverride }}/{{ .Values.tlsSidecarEntityOperator.image.name }}:{{ default .Values.tlsSidecarEntityOperator.image.tagPrefix .Values.imageTagOverride }}-kafka-${default_kafka_version}"
kafka_exporter_version="{{ default .Values.kafkaExporter.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaExporter.image.name }}:{{ default .Values.kafkaExporter.image.tagPrefix .Values.imageTagOverride }}-kafka-${default_kafka_version}"

for version in "${versions[@]}"
do
    zookeeper_version="{{ default .Values.zookeeper.image.repository .Values.imageRepositoryOverride }}/{{ .Values.zookeeper.image.name }}:{{ default .Values.zookeeper.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    entity_operator_tls_sidecar_version="{{ default .Values.tlsSidecarEntityOperator.image.repository .Values.imageRepositoryOverride }}/{{ .Values.tlsSidecarEntityOperator.image.name }}:{{ default .Values.tlsSidecarEntityOperator.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    kafka_exporter_version="{{ default .Values.kafkaExporter.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaExporter.image.name }}:{{ default .Values.kafkaExporter.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    cruise_control_version="{{ default .Values.cruiseControl.image.repository .Values.imageRepositoryOverride }}/{{ .Values.cruiseControl.image.name }}:{{ default .Values.cruiseControl.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    cruise_control_tls_sidecar_version="{{ default .Values.tlsSidecarCruiseControl.image.repository .Values.imageRepositoryOverride }}/{{ .Values.tlsSidecarCruiseControl.image.name }}:{{ default .Values.tlsSidecarCruiseControl.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    kafka_versions="${kafka_versions}
${version}={{ default .Values.kafka.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafka.image.name }}:{{ default .Values.kafka.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    kafka_connect_versions="${kafka_connect_versions}
${version}={{ default .Values.kafkaConnect.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaConnect.image.name }}:{{ default .Values.kafkaConnect.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    kafka_connect_s2i_versions="${kafka_connect_s2i_versions}
${version}={{ default .Values.kafkaConnects2i.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaConnects2i.image.name }}:{{ default .Values.kafkaConnects2i.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    kafka_mirror_maker_versions="${kafka_mirror_maker_versions}
${version}={{ default .Values.kafkaMirrorMaker.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaMirrorMaker.image.name }}:{{ default .Values.kafkaMirrorMaker.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    kafka_exporter_versions="${kafka_exporter_versions}
${version}={{ default .Values.kafkaExporter.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaExporter.image.name }}:{{ default .Values.kafkaExporter.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    if [[ ${version_does_not_support[${version}]} != *"kafkaMirrorMaker2"* ]] ; then
      kafka_mirror_maker_2_versions="${kafka_mirror_maker_2_versions}
${version}={{ default .Values.kafkaMirrorMaker2.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaMirrorMaker2.image.name }}:{{ default .Values.kafkaMirrorMaker2.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    fi
done

kafka_versions=$(echo "$kafka_versions" | sed 's/^/                /g')
kafka_connect_versions=$(echo "$kafka_connect_versions" | sed 's/^/                /g')
kafka_connect_s2i_versions=$(echo "$kafka_connect_s2i_versions" | sed 's/^/                /g')
kafka_mirror_maker_versions=$(echo "$kafka_mirror_maker_versions" | sed 's/^/                /g')
kafka_exporter_versions=$(echo "$kafka_exporter_versions" | sed 's/^/                /g')
kafka_mirror_maker_2_versions=$(echo "$kafka_mirror_maker_2_versions" | sed 's/^/                /g')

cat >"$out" <<EOF
{{/* vim: set filetype=mustache: */}}

{{/* This file is generated in helm-charts/Makefile */}}
{{/* DO NOT EDIT BY HAND                            */}}

{{/* Generate the kafka image map */}}
{{- define "strimzi.kafka.image.map" }}
            - name: STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE
              value: ${entity_operator_tls_sidecar_version}
            - name: STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE
              value: ${kafka_exporter_version}
            - name: STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE
              value: ${cruise_control_version}
            - name: STRIMZI_DEFAULT_TLS_SIDECAR_CRUISE_CONTROL_IMAGE
              value: ${cruise_control_tls_sidecar_version}
            - name: STRIMZI_KAFKA_IMAGES
              value: | ${kafka_versions}
            - name: STRIMZI_KAFKA_CONNECT_IMAGES
              value: | ${kafka_connect_versions}
            - name: STRIMZI_KAFKA_CONNECT_S2I_IMAGES
              value: | ${kafka_connect_s2i_versions}
            - name: STRIMZI_KAFKA_MIRROR_MAKER_IMAGES
              value: | ${kafka_mirror_maker_versions}
            - name: STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES
              value: | ${kafka_mirror_maker_2_versions}
{{- end -}}
EOF

