PROJECT_NAME=install

include ../../Makefile.os

RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../../strimzi-$(RELEASE_VERSION)/$(PROJECT_NAME)

crd_install_helm3:
	$(CP) ./cluster-operator/043-Crd-kafkatopic.yaml ./topic-operator/04-Crd-kafkatopic.yaml
	$(CP) ./cluster-operator/044-Crd-kafkauser.yaml ./user-operator/04-Crd-kafkauser.yaml
	$(CP) ./cluster-operator/040-Crd-kafka.yaml ../helm-charts/helm3/strimzi-kafka-operator/crds/040-Crd-kafka.yaml
	$(CP) ./cluster-operator/041-Crd-kafkaconnect.yaml ../helm-charts/helm3/strimzi-kafka-operator/crds/041-Crd-kafkaconnect.yaml
	$(CP) ./cluster-operator/042-Crd-strimzipodset.yaml ../helm-charts/helm3/strimzi-kafka-operator/crds/042-Crd-strimzipodset.yaml
	$(CP) ./cluster-operator/043-Crd-kafkatopic.yaml ../helm-charts/helm3/strimzi-kafka-operator/crds/043-Crd-kafkatopic.yaml
	$(CP) ./cluster-operator/044-Crd-kafkauser.yaml ../helm-charts/helm3/strimzi-kafka-operator/crds/044-Crd-kafkauser.yaml
	$(CP) ./cluster-operator/045-Crd-kafkamirrormaker.yaml ../helm-charts/helm3/strimzi-kafka-operator/crds/045-Crd-kafkamirrormaker.yaml
	$(CP) ./cluster-operator/046-Crd-kafkabridge.yaml ../helm-charts/helm3/strimzi-kafka-operator/crds/046-Crd-kafkabridge.yaml
	$(CP) ./cluster-operator/047-Crd-kafkaconnector.yaml ../helm-charts/helm3/strimzi-kafka-operator/crds/047-Crd-kafkaconnector.yaml
	$(CP) ./cluster-operator/048-Crd-kafkamirrormaker2.yaml ../helm-charts/helm3/strimzi-kafka-operator/crds/048-Crd-kafkamirrormaker2.yaml
	$(CP) ./cluster-operator/049-Crd-kafkarebalance.yaml ../helm-charts/helm3/strimzi-kafka-operator/crds/049-Crd-kafkarebalance.yaml
	$(CP) ./cluster-operator/04A-Crd-kafkanodepool.yaml ../helm-charts/helm3/strimzi-kafka-operator/crds/04A-Crd-kafkanodepool.yaml
	yq eval -i '.metadata.labels.component="kafkas.kafka.strimzi.io-crd"' ../helm-charts/helm3/strimzi-kafka-operator/crds/040-Crd-kafka.yaml
	yq eval -i '.metadata.labels.component="kafkaconnects.kafka.strimzi.io-crd"' ../helm-charts/helm3/strimzi-kafka-operator/crds/041-Crd-kafkaconnect.yaml
	yq eval -i '.metadata.labels.component="stirmzipodsets.core.strimzi.io-crd"' ../helm-charts/helm3/strimzi-kafka-operator/crds/042-Crd-strimzipodset.yaml
	yq eval -i '.metadata.labels.component="kafkatopics.kafka.strimzi.io-crd"' ../helm-charts/helm3/strimzi-kafka-operator/crds/043-Crd-kafkatopic.yaml
	yq eval -i '.metadata.labels.component="kafkausers.kafka.strimzi.io-crd"' ../helm-charts/helm3/strimzi-kafka-operator/crds/044-Crd-kafkauser.yaml 
	yq eval -i '.metadata.labels.component="kafkamirrormakers.kafka.strimzi.io-crd"' ../helm-charts/helm3/strimzi-kafka-operator/crds/045-Crd-kafkamirrormaker.yaml 
	yq eval -i '.metadata.labels.component="kafkabridges.kafka.strimzi.io-crd"' ../helm-charts/helm3/strimzi-kafka-operator/crds/046-Crd-kafkabridge.yaml 
	yq eval -i '.metadata.labels.component="kafkaconnectors.kafka.strimzi.io-crd"' ../helm-charts/helm3/strimzi-kafka-operator/crds/047-Crd-kafkaconnector.yaml 
	yq eval -i '.metadata.labels.component="kafkamirrormaker2.kafka.strimzi.io-crd"' ../helm-charts/helm3/strimzi-kafka-operator/crds/048-Crd-kafkamirrormaker2.yaml 
	yq eval -i '.metadata.labels.component="kafkarebalances.kafka.strimzi.io-crd"' ../helm-charts/helm3/strimzi-kafka-operator/crds/049-Crd-kafkarebalance.yaml 

crd_install: crd_install_helm3

release:
	mkdir -p $(RELEASE_PATH)
	$(CP) -r ./cluster-operator $(RELEASE_PATH)/
	$(CP) -r ./user-operator $(RELEASE_PATH)/
	$(CP) -r ./topic-operator $(RELEASE_PATH)/
	$(CP) -r ./strimzi-admin $(RELEASE_PATH)/
	$(CP) -r ./drain-cleaner $(RELEASE_PATH)/
	$(CP) -r ./canary $(RELEASE_PATH)/

java_build: crd_install
java_install: java_build
docker_build: crd_install
docker_tag:
docker_push:

.PHONY: all build clean docker_build docker_push docker_tag spotbugs
