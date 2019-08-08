#!/usr/bin/env bash

out="$1"
default_version=$(grep -E '^([0-9.]+)[[:space:]]+default' ../kafka-versions | cut -d ' ' -f 1)
for version in $(sed -E -e '/^(#.*|[[:space:]]*)$/d' -e 's/^([0-9.]+)[[:space:]]+.*$/\1/g' ../kafka-versions); do
    zookeeper_version="{{ default .Values.zookeeper.image.repository .Values.imageRepositoryOverride }}/{{ .Values.zookeeper.image.name }}:{{ default .Values.zookeeper.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    zookeeper_tls_sidecar_version="{{ default .Values.tlsSidecarZookeeper.image.repository .Values.imageRepositoryOverride }}/{{ .Values.tlsSidecarZookeeper.image.name }}:{{ default .Values.tlsSidecarZookeeper.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    kafka_tls_sidecar_version="{{ default .Values.tlsSidecarKafka.image.repository .Values.imageRepositoryOverride }}/{{ .Values.tlsSidecarKafka.image.name }}:{{ default .Values.tlsSidecarKafka.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    entity_operator_tls_sidecar_version="{{ default .Values.tlsSidecarEntityOperator.image.repository .Values.imageRepositoryOverride }}/{{ .Values.tlsSidecarEntityOperator.image.name }}:{{ default .Values.tlsSidecarEntityOperator.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    kafka_versions="${kafka_versions}
${version}={{ default .Values.kafka.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafka.image.name }}:{{ default .Values.kafka.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    kafka_connect_versions="${kafka_connect_versions}
${version}={{ default .Values.kafkaConnect.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaConnect.image.name }}:{{ default .Values.kafkaConnect.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    kafka_connect_s2i_versions="${kafka_connect_s2i_versions}
${version}={{ default .Values.kafkaConnects2i.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaConnects2i.image.name }}:{{ default .Values.kafkaConnects2i.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
    kafka_mirror_maker_versions="${kafka_mirror_maker_versions}
${version}={{ default .Values.kafkaMirrorMaker.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaMirrorMaker.image.name }}:{{ default .Values.kafkaMirrorMaker.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}"
done
kafka_versions=$(echo "$kafka_versions" | sed 's/^/                /g')
kafka_connect_versions=$(echo "$kafka_connect_versions" | sed 's/^/                /g')
kafka_connect_s2i_versions=$(echo "$kafka_connect_s2i_versions" | sed 's/^/                /g')
kafka_mirror_maker_versions=$(echo "$kafka_mirror_maker_versions" | sed 's/^/                /g')

cat >"$out" <<EOF
{{/* vim: set filetype=mustache: */}}

{{/* This file is generated in helm-charts/Makefile */}}
{{/* DO NOT EDIT BY HAND                            */}}

{{/* Generate the kafka image map */}}
{{- define "strimzi.kafka.image.map" }}
            - name: STRIMZI_DEFAULT_ZOOKEEPER_IMAGE
              value: ${zookeeper_version}
            - name: STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE
              value: ${entity_operator_tls_sidecar_version}
            - name: STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE
              value: ${kafka_tls_sidecar_version}
            - name: STRIMZI_DEFAULT_TLS_SIDECAR_ZOOKEEPER_IMAGE
              value: ${zookeeper_tls_sidecar_version}
            - name: STRIMZI_KAFKA_IMAGES
              value: | ${kafka_versions}
            - name: STRIMZI_KAFKA_CONNECT_IMAGES
              value: | ${kafka_connect_versions}
            - name: STRIMZI_KAFKA_CONNECT_S2I_IMAGES
              value: | ${kafka_connect_s2i_versions}
            - name: STRIMZI_KAFKA_MIRROR_MAKER_IMAGES
              value: | ${kafka_mirror_maker_versions}
{{- end -}}
EOF

