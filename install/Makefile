PROJECT_NAME=install

include ../Makefile.os

RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../strimzi-$(RELEASE_VERSION)/$(PROJECT_NAME)

crd_install:
	$(CP) ./cluster-operator/043-Crd-kafkatopic.yaml ./topic-operator/04-Crd-kafkatopic.yaml
	$(CP) ./cluster-operator/044-Crd-kafkauser.yaml ./user-operator/04-Crd-kafkauser.yaml
	$(CP) ./cluster-operator/040-Crd-kafka.yaml ../olm/kafkas.crd.yaml
	$(CP) ./cluster-operator/041-Crd-kafkaconnect.yaml ../olm/kafkaconnects.crd.yaml
	$(CP) ./cluster-operator/042-Crd-kafkaconnects2i.yaml ../olm/kafkaconnects2is.crd.yaml
	$(CP) ./cluster-operator/043-Crd-kafkatopic.yaml ../olm/kafkatopics.crd.yaml
	$(CP) ./cluster-operator/044-Crd-kafkauser.yaml ../olm/kafkausers.crd.yaml
	$(CP) ./cluster-operator/045-Crd-kafkamirrormaker.yaml ../olm/kafkamirrormakers.crd.yaml
	$(CP) ./cluster-operator/046-Crd-kafkabridge.yaml ../olm/kafkabridges.crd.yaml
	$(CP) ./cluster-operator/040-Crd-kafka.yaml ../helm-charts/strimzi-kafka-operator/templates/040-Crd-kafka.yaml
	$(CP) ./cluster-operator/041-Crd-kafkaconnect.yaml ../helm-charts/strimzi-kafka-operator/templates/041-Crd-kafkaconnect.yaml
	$(CP) ./cluster-operator/042-Crd-kafkaconnects2i.yaml ../helm-charts/strimzi-kafka-operator/templates/042-Crd-kafkaconnects2i.yaml
	$(CP) ./cluster-operator/043-Crd-kafkatopic.yaml ../helm-charts/strimzi-kafka-operator/templates/043-Crd-kafkatopic.yaml
	$(CP) ./cluster-operator/044-Crd-kafkauser.yaml ../helm-charts/strimzi-kafka-operator/templates/044-Crd-kafkauser.yaml
	$(CP) ./cluster-operator/045-Crd-kafkamirrormaker.yaml ../helm-charts/strimzi-kafka-operator/templates/045-Crd-kafkamirrormaker.yaml
	$(CP) ./cluster-operator/046-Crd-kafkabridge.yaml ../helm-charts/strimzi-kafka-operator/templates/046-Crd-kafkabridge.yaml
	oldpath=`pwd` && cd ../helm-charts/strimzi-kafka-operator/templates/ &&  $(FIND) . -type f -name '04*-Crd-*.yaml' -exec yq write -i {} metadata.labels.app "{{ template \"strimzi.name\" . }}" \; && cd $$oldpath
	oldpath=`pwd` && cd ../helm-charts/strimzi-kafka-operator/templates/ &&  $(FIND) . -type f -name '04*-Crd-*.yaml' -exec yq write -i {} metadata.labels.chart "{{ template \"strimzi.chart\" . }}" \; && cd $$oldpath
	yq write -i ../helm-charts/strimzi-kafka-operator/templates/040-Crd-kafka.yaml metadata.labels.component "kafkas.kafka.strimzi.io-crd"
	yq write -i ../helm-charts/strimzi-kafka-operator/templates/041-Crd-kafkaconnect.yaml metadata.labels.component "kafkaconnects.kafka.strimzi.io-crd"
	yq write -i ../helm-charts/strimzi-kafka-operator/templates/042-Crd-kafkaconnects2i.yaml metadata.labels.component "kafkaconnects2is.kafka.strimzi.io-crd"
	yq write -i ../helm-charts/strimzi-kafka-operator/templates/043-Crd-kafkatopic.yaml metadata.labels.component "kafkatopics.kafka.strimzi.io-crd"
	yq write -i ../helm-charts/strimzi-kafka-operator/templates/044-Crd-kafkauser.yaml metadata.labels.component "kafkausers.kafka.strimzi.io-crd"
	yq write -i ../helm-charts/strimzi-kafka-operator/templates/045-Crd-kafkamirrormaker.yaml metadata.labels.component "kafkamirrormakers.kafka.strimzi.io-crd"
	yq write -i ../helm-charts/strimzi-kafka-operator/templates/046-Crd-kafkabridge.yaml metadata.labels.component "kafkabridges.kafka.strimzi.io-crd"

	oldpath=`pwd` && cd ../helm-charts/strimzi-kafka-operator/templates/ &&  $(FIND) . -type f -name '04*-Crd-*.yaml' -exec yq write -i {} metadata.labels.release "{{ .Release.Name }}" \; && cd $$oldpath
	oldpath=`pwd` && cd ../helm-charts/strimzi-kafka-operator/templates/ &&  $(FIND) . -type f -name '04*-Crd-*.yaml' -exec yq write -i {} metadata.labels.heritage "{{ .Release.Service }}" \; && cd $$oldpath
	oldpath=`pwd` && cd ../helm-charts/strimzi-kafka-operator/templates/ &&  $(FIND) . -type f -name '04*-Crd-*.yaml' -exec $(SED) -i -e '1s/^/{{- if .Values.createGlobalResources -}}\n/' -e '$$s/$$/\n{{- end -}}/' {} \; && cd $$oldpath

release:
	mkdir -p $(RELEASE_PATH)
	$(CP) -r ./cluster-operator $(RELEASE_PATH)/
	$(CP) -r ./user-operator $(RELEASE_PATH)/
	$(CP) -r ./topic-operator $(RELEASE_PATH)/
	$(CP) -r ./strimzi-admin $(RELEASE_PATH)/

.PHONY: all build clean docker_build docker_push docker_tag findbugs
