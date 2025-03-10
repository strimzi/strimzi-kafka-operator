{{/* vim: set filetype=mustache: */}}

{{/* This file is generated in helm-charts/Makefile */}}
{{/* DO NOT EDIT BY HAND                            */}}

{{/* Generate the kafka image map */}}
{{- define "strimzi.kafka.image.map" }}
            - name: STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE
              value: {{ template "strimzi.image" (merge . (dict "key" "kafkaExporter" "version" "4.0.0" "defaultTagSuffix" "-kafka-4.0.0")) }}
            - name: STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE
              value: {{ template "strimzi.image" (merge . (dict "key" "cruiseControl" "version" "4.0.0" "defaultTagSuffix" "-kafka-4.0.0")) }}
            - name: STRIMZI_KAFKA_IMAGES
              value: |                 
                3.9.0={{ template "strimzi.image" (merge . (dict "key" "kafka" "version" "3.9.0" "defaultTagSuffix" "-kafka-3.9.0")) }}
                4.0.0={{ template "strimzi.image" (merge . (dict "key" "kafka" "version" "4.0.0" "defaultTagSuffix" "-kafka-4.0.0")) }}
            - name: STRIMZI_KAFKA_CONNECT_IMAGES
              value: |                 
                3.9.0={{ template "strimzi.image" (merge . (dict "key" "kafkaConnect" "version" "3.9.0" "defaultTagSuffix" "-kafka-3.9.0")) }}
                4.0.0={{ template "strimzi.image" (merge . (dict "key" "kafkaConnect" "version" "4.0.0" "defaultTagSuffix" "-kafka-4.0.0")) }}
            - name: STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES
              value: |                 
                3.9.0={{ template "strimzi.image" (merge . (dict "key" "kafkaMirrorMaker2" "version" "3.9.0" "defaultTagSuffix" "-kafka-3.9.0")) }}
                4.0.0={{ template "strimzi.image" (merge . (dict "key" "kafkaMirrorMaker2" "version" "4.0.0" "defaultTagSuffix" "-kafka-4.0.0")) }}
{{- end -}}
