{{/* vim: set filetype=mustache: */}}

{{/* This file is generated in helm-charts/Makefile */}}
{{/* DO NOT EDIT BY HAND                            */}}

{{/* Generate the kafka image map */}}
{{- define "strimzi.kafka.image.map" }}
            - name: STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE
              value: {{ template "strimzi.image" (merge . (dict "key" "kafkaExporter" "tagSuffix" "-kafka-4.0.0")) }}
            - name: STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE
              value: {{ template "strimzi.image" (merge . (dict "key" "cruiseControl" "tagSuffix" "-kafka-4.0.0")) }}
            - name: STRIMZI_KAFKA_IMAGES
              value: |                 
                3.9.0={{ template "strimzi.image" (merge . (dict "key" "kafka" "tagSuffix" "-kafka-3.9.0")) }}
                3.9.1={{ template "strimzi.image" (merge . (dict "key" "kafka" "tagSuffix" "-kafka-3.9.1")) }}
                4.0.0={{ template "strimzi.image" (merge . (dict "key" "kafka" "tagSuffix" "-kafka-4.0.0")) }}
            - name: STRIMZI_KAFKA_CONNECT_IMAGES
              value: |                 
                3.9.0={{ template "strimzi.image" (merge . (dict "key" "kafkaConnect" "tagSuffix" "-kafka-3.9.0")) }}
                3.9.1={{ template "strimzi.image" (merge . (dict "key" "kafkaConnect" "tagSuffix" "-kafka-3.9.1")) }}
                4.0.0={{ template "strimzi.image" (merge . (dict "key" "kafkaConnect" "tagSuffix" "-kafka-4.0.0")) }}
            - name: STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES
              value: |                 
                3.9.0={{ template "strimzi.image" (merge . (dict "key" "kafkaMirrorMaker2" "tagSuffix" "-kafka-3.9.0")) }}
                3.9.1={{ template "strimzi.image" (merge . (dict "key" "kafkaMirrorMaker2" "tagSuffix" "-kafka-3.9.1")) }}
                4.0.0={{ template "strimzi.image" (merge . (dict "key" "kafkaMirrorMaker2" "tagSuffix" "-kafka-4.0.0")) }}
{{- end -}}
