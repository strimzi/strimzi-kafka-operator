#!/usr/bin/env bash

out="$1"
default_version=$(grep -E '^([0-9.]+)[[:space:]]+default' ../kafka-versions | cut -d ' ' -f 1)

cat >"$out" <<EOF
{{/* vim: set filetype=mustache: */}}

{{/* This file is generated in helm-charts/Makefile */}}
{{/* DO NOT EDIT BY HAND                            */}}

{{/* Generate the kafka image map */}}
{{- define "strimzi.kafka.image.map" }}
            - name: STRIMZI_KAFKA_IMAGE_MAP
              value: |
EOF

for version in $(sed -E -e '/^(#.*|[[:space:]]*)$/d' -e 's/^([0-9.]+)[[:space:]]+.*$/\1/g' ../kafka-versions); do
    echo "                ${version}={{ default .Values.kafka.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafka.image.name }}:{{ default .Values.kafka.image.tagPrefix .Values.imageTagOverride }}-kafka-${version}" >>"$out"
done

echo "{{- end -}}" >>"$out"
