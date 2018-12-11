{{/* vim: set filetype=mustache: */}}

{{/* This file is generated in helm-charts/Makefile */}}
{{/* DO NOT EDIT BY HAND                            */}}

{{/* Generate the kafka image map */}}
{{- define "strimzi.kafka.image.map" }}
            - name: STRIMZI_KAFKA_IMAGES
              value: |                 
                2.0.0={{ default .Values.kafka.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafka.image.name }}:{{ default .Values.kafka.image.tagPrefix .Values.imageTagOverride }}-kafka-2.0.0
                2.0.1={{ default .Values.kafka.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafka.image.name }}:{{ default .Values.kafka.image.tagPrefix .Values.imageTagOverride }}-kafka-2.0.1
                2.1.0={{ default .Values.kafka.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafka.image.name }}:{{ default .Values.kafka.image.tagPrefix .Values.imageTagOverride }}-kafka-2.1.0
            - name: STRIMZI_KAFKA_CONNECT_IMAGES
              value: |                 
                2.0.0={{ default .Values.kafkaConnect.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaConnect.image.name }}:{{ default .Values.kafkaConnect.image.tagPrefix .Values.imageTagOverride }}-kafka-2.0.0
                2.0.1={{ default .Values.kafkaConnect.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaConnect.image.name }}:{{ default .Values.kafkaConnect.image.tagPrefix .Values.imageTagOverride }}-kafka-2.0.1
                2.1.0={{ default .Values.kafkaConnect.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaConnect.image.name }}:{{ default .Values.kafkaConnect.image.tagPrefix .Values.imageTagOverride }}-kafka-2.1.0
            - name: STRIMZI_KAFKA_CONNECT_S2I_IMAGES
              value: |                 
                2.0.0={{ default .Values.kafkaConnects2i.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaConnects2i.image.name }}:{{ default .Values.kafkaConnects2i.image.tagPrefix .Values.imageTagOverride }}-kafka-2.0.0
                2.0.1={{ default .Values.kafkaConnects2i.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaConnects2i.image.name }}:{{ default .Values.kafkaConnects2i.image.tagPrefix .Values.imageTagOverride }}-kafka-2.0.1
                2.1.0={{ default .Values.kafkaConnects2i.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaConnects2i.image.name }}:{{ default .Values.kafkaConnects2i.image.tagPrefix .Values.imageTagOverride }}-kafka-2.1.0
            - name: STRIMZI_KAFKA_MIRROR_MAKER_IMAGES
              value: |                 
                2.0.0={{ default .Values.kafkaMirrorMaker.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaMirrorMaker.image.name }}:{{ default .Values.kafkaMirrorMaker.image.tagPrefix .Values.imageTagOverride }}-kafka-2.0.0
                2.0.1={{ default .Values.kafkaMirrorMaker.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaMirrorMaker.image.name }}:{{ default .Values.kafkaMirrorMaker.image.tagPrefix .Values.imageTagOverride }}-kafka-2.0.1
                2.1.0={{ default .Values.kafkaMirrorMaker.image.repository .Values.imageRepositoryOverride }}/{{ .Values.kafkaMirrorMaker.image.name }}:{{ default .Values.kafkaMirrorMaker.image.tagPrefix .Values.imageTagOverride }}-kafka-2.1.0
{{- end -}}
