{{/* vim: set filetype=mustache: */}}

{{/* This file is generated in helm-charts/Makefile */}}
{{/* DO NOT EDIT BY HAND                            */}}

{{/* Generate the kafka image map */}}
{{- define "strimzi.kafka.image.map" }}
            - name: STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE
              value: {{ template "strimzi.image" (merge . (dict "key" "kafkaExporter" "tagSuffix" "-kafka-4.3.1")) }}
            - name: STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE
              value: {{ template "strimzi.image" (merge . (dict "key" "cruiseControl" "tagSuffix" "-kafka-4.3.1")) }}
            - name: STRIMZI_KAFKA_IMAGES
              value: |                 
                4.2.0={{ template "strimzi.image" (merge . (dict "key" "kafka" "defaultDigest" "sha256:379ad6201a9a3d6f72d4a69a2d134544f4dbb3c10feb2a6a60e3c52b41ed16cb" "tagSuffix" "-kafka-4.2.0")) }}
                4.2.1={{ template "strimzi.image" (merge . (dict "key" "kafka" "defaultDigest" "sha256:56faef470161bb1b856108c6dfa4f11f53f4ea71104736c9066e1df78f953fad" "tagSuffix" "-kafka-4.2.1")) }}
                4.2.2={{ template "strimzi.image" (merge . (dict "key" "kafka" "defaultDigest" "sha256:b87c118d2a895e362f23da2e2aa55a632941c140f62aa01b25b5ece70ae3bf9d" "tagSuffix" "-kafka-4.2.2")) }}
                4.3.0={{ template "strimzi.image" (merge . (dict "key" "kafka" "defaultDigest" "sha256:5ee2b5814103b6f450ad37c7198e7adb1b9ca820213fdadf930549bcb0c0d961" "tagSuffix" "-kafka-4.3.0")) }}
                4.3.1={{ template "strimzi.image" (merge . (dict "key" "kafka" "defaultDigest" "sha256:7d097c26074d1a3a01f479a8d0bae4835ee433bf55e6cbc5487a68e6bff0667e" "tagSuffix" "-kafka-4.3.1")) }}
            - name: STRIMZI_KAFKA_CONNECT_IMAGES
              value: |                 
                4.2.0={{ template "strimzi.image" (merge . (dict "key" "kafkaConnect" "defaultDigest" "sha256:379ad6201a9a3d6f72d4a69a2d134544f4dbb3c10feb2a6a60e3c52b41ed16cb" "tagSuffix" "-kafka-4.2.0")) }}
                4.2.1={{ template "strimzi.image" (merge . (dict "key" "kafkaConnect" "defaultDigest" "sha256:56faef470161bb1b856108c6dfa4f11f53f4ea71104736c9066e1df78f953fad" "tagSuffix" "-kafka-4.2.1")) }}
                4.2.2={{ template "strimzi.image" (merge . (dict "key" "kafkaConnect" "defaultDigest" "sha256:b87c118d2a895e362f23da2e2aa55a632941c140f62aa01b25b5ece70ae3bf9d" "tagSuffix" "-kafka-4.2.2")) }}
                4.3.0={{ template "strimzi.image" (merge . (dict "key" "kafkaConnect" "defaultDigest" "sha256:5ee2b5814103b6f450ad37c7198e7adb1b9ca820213fdadf930549bcb0c0d961" "tagSuffix" "-kafka-4.3.0")) }}
                4.3.1={{ template "strimzi.image" (merge . (dict "key" "kafkaConnect" "defaultDigest" "sha256:7d097c26074d1a3a01f479a8d0bae4835ee433bf55e6cbc5487a68e6bff0667e" "tagSuffix" "-kafka-4.3.1")) }}
            - name: STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES
              value: |                 
                4.2.0={{ template "strimzi.image" (merge . (dict "key" "kafkaMirrorMaker2" "defaultDigest" "sha256:379ad6201a9a3d6f72d4a69a2d134544f4dbb3c10feb2a6a60e3c52b41ed16cb" "tagSuffix" "-kafka-4.2.0")) }}
                4.2.1={{ template "strimzi.image" (merge . (dict "key" "kafkaMirrorMaker2" "defaultDigest" "sha256:56faef470161bb1b856108c6dfa4f11f53f4ea71104736c9066e1df78f953fad" "tagSuffix" "-kafka-4.2.1")) }}
                4.2.2={{ template "strimzi.image" (merge . (dict "key" "kafkaMirrorMaker2" "defaultDigest" "sha256:b87c118d2a895e362f23da2e2aa55a632941c140f62aa01b25b5ece70ae3bf9d" "tagSuffix" "-kafka-4.2.2")) }}
                4.3.0={{ template "strimzi.image" (merge . (dict "key" "kafkaMirrorMaker2" "defaultDigest" "sha256:5ee2b5814103b6f450ad37c7198e7adb1b9ca820213fdadf930549bcb0c0d961" "tagSuffix" "-kafka-4.3.0")) }}
                4.3.1={{ template "strimzi.image" (merge . (dict "key" "kafkaMirrorMaker2" "defaultDigest" "sha256:7d097c26074d1a3a01f479a8d0bae4835ee433bf55e6cbc5487a68e6bff0667e" "tagSuffix" "-kafka-4.3.1")) }}
{{- end -}}
