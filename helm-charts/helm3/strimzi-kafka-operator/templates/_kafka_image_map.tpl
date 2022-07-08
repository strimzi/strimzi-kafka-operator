{{/* vim: set filetype=mustache: */}}

{{/* This file is generated in helm-charts/Makefile */}}
{{/* DO NOT EDIT BY HAND                            */}}

{{/* Generate the kafka image map */}}
{{- define "strimzi.kafka.image.map" }}
            - name: STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE
              value: {{ default .Values.defaultImageRegistry .Values.tlsSidecarEntityOperator.image.registry }}/{{ default .Values.defaultImageRepository .Values.tlsSidecarEntityOperator.image.repository }}/{{ .Values.tlsSidecarEntityOperator.image.name }}:{{ default .Values.defaultImageTag .Values.tlsSidecarEntityOperator.image.tagPrefix }}-kafka-3.2.0
            - name: STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE
              value: {{ default .Values.defaultImageRegistry .Values.kafkaExporter.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaExporter.image.repository }}/{{ .Values.kafkaExporter.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaExporter.image.tagPrefix }}-kafka-3.2.0
            - name: STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE
              value: {{ default .Values.defaultImageRegistry .Values.cruiseControl.image.registry }}/{{ default .Values.defaultImageRepository .Values.cruiseControl.image.repository }}/{{ .Values.cruiseControl.image.name }}:{{ default .Values.defaultImageTag .Values.cruiseControl.image.tagPrefix }}-kafka-3.2.0
            - name: STRIMZI_KAFKA_IMAGES
              value: |                 
                3.0.0={{ default .Values.defaultImageRegistry .Values.kafka.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafka.image.repository }}/{{ .Values.kafka.image.name }}:{{ default .Values.defaultImageTag .Values.kafka.image.tagPrefix }}-kafka-3.0.0
                3.0.1={{ default .Values.defaultImageRegistry .Values.kafka.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafka.image.repository }}/{{ .Values.kafka.image.name }}:{{ default .Values.defaultImageTag .Values.kafka.image.tagPrefix }}-kafka-3.0.1
                3.1.0={{ default .Values.defaultImageRegistry .Values.kafka.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafka.image.repository }}/{{ .Values.kafka.image.name }}:{{ default .Values.defaultImageTag .Values.kafka.image.tagPrefix }}-kafka-3.1.0
                3.1.1={{ default .Values.defaultImageRegistry .Values.kafka.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafka.image.repository }}/{{ .Values.kafka.image.name }}:{{ default .Values.defaultImageTag .Values.kafka.image.tagPrefix }}-kafka-3.1.1
                3.2.0={{ default .Values.defaultImageRegistry .Values.kafka.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafka.image.repository }}/{{ .Values.kafka.image.name }}:{{ default .Values.defaultImageTag .Values.kafka.image.tagPrefix }}-kafka-3.2.0
            - name: STRIMZI_KAFKA_CONNECT_IMAGES
              value: |                 
                3.0.0={{ default .Values.defaultImageRegistry .Values.kafkaConnect.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaConnect.image.repository }}/{{ .Values.kafkaConnect.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaConnect.image.tagPrefix }}-kafka-3.0.0
                3.0.1={{ default .Values.defaultImageRegistry .Values.kafkaConnect.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaConnect.image.repository }}/{{ .Values.kafkaConnect.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaConnect.image.tagPrefix }}-kafka-3.0.1
                3.1.0={{ default .Values.defaultImageRegistry .Values.kafkaConnect.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaConnect.image.repository }}/{{ .Values.kafkaConnect.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaConnect.image.tagPrefix }}-kafka-3.1.0
                3.1.1={{ default .Values.defaultImageRegistry .Values.kafkaConnect.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaConnect.image.repository }}/{{ .Values.kafkaConnect.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaConnect.image.tagPrefix }}-kafka-3.1.1
                3.2.0={{ default .Values.defaultImageRegistry .Values.kafkaConnect.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaConnect.image.repository }}/{{ .Values.kafkaConnect.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaConnect.image.tagPrefix }}-kafka-3.2.0
            - name: STRIMZI_KAFKA_MIRROR_MAKER_IMAGES
              value: |                 
                3.0.0={{ default .Values.defaultImageRegistry .Values.kafkaMirrorMaker.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaMirrorMaker.image.repository }}/{{ .Values.kafkaMirrorMaker.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaMirrorMaker.image.tagPrefix }}-kafka-3.0.0
                3.0.1={{ default .Values.defaultImageRegistry .Values.kafkaMirrorMaker.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaMirrorMaker.image.repository }}/{{ .Values.kafkaMirrorMaker.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaMirrorMaker.image.tagPrefix }}-kafka-3.0.1
                3.1.0={{ default .Values.defaultImageRegistry .Values.kafkaMirrorMaker.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaMirrorMaker.image.repository }}/{{ .Values.kafkaMirrorMaker.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaMirrorMaker.image.tagPrefix }}-kafka-3.1.0
                3.1.1={{ default .Values.defaultImageRegistry .Values.kafkaMirrorMaker.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaMirrorMaker.image.repository }}/{{ .Values.kafkaMirrorMaker.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaMirrorMaker.image.tagPrefix }}-kafka-3.1.1
                3.2.0={{ default .Values.defaultImageRegistry .Values.kafkaMirrorMaker.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaMirrorMaker.image.repository }}/{{ .Values.kafkaMirrorMaker.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaMirrorMaker.image.tagPrefix }}-kafka-3.2.0
            - name: STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES
              value: |                 
                3.0.0={{ default .Values.defaultImageRegistry .Values.kafkaMirrorMaker2.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaMirrorMaker2.image.repository }}/{{ .Values.kafkaMirrorMaker2.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaMirrorMaker2.image.tagPrefix }}-kafka-3.0.0
                3.0.1={{ default .Values.defaultImageRegistry .Values.kafkaMirrorMaker2.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaMirrorMaker2.image.repository }}/{{ .Values.kafkaMirrorMaker2.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaMirrorMaker2.image.tagPrefix }}-kafka-3.0.1
                3.1.0={{ default .Values.defaultImageRegistry .Values.kafkaMirrorMaker2.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaMirrorMaker2.image.repository }}/{{ .Values.kafkaMirrorMaker2.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaMirrorMaker2.image.tagPrefix }}-kafka-3.1.0
                3.1.1={{ default .Values.defaultImageRegistry .Values.kafkaMirrorMaker2.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaMirrorMaker2.image.repository }}/{{ .Values.kafkaMirrorMaker2.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaMirrorMaker2.image.tagPrefix }}-kafka-3.1.1
                3.2.0={{ default .Values.defaultImageRegistry .Values.kafkaMirrorMaker2.image.registry }}/{{ default .Values.defaultImageRepository .Values.kafkaMirrorMaker2.image.repository }}/{{ .Values.kafkaMirrorMaker2.image.name }}:{{ default .Values.defaultImageTag .Values.kafkaMirrorMaker2.image.tagPrefix }}-kafka-3.2.0
{{- end -}}
