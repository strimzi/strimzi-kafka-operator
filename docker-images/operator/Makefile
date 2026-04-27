PROJECT_NAME := operator
jar_version := $(shell cat ../../release.version)
topic_operator_dist := ../artifacts/binaries/topic-operator-$(jar_version)-dist.zip
user_operator_dist := ../artifacts/binaries/user-operator-$(jar_version)-dist.zip
cluster_operator_dist := ../artifacts/binaries/cluster-operator-$(jar_version)-dist.zip
kafka_init_dist := ../artifacts/binaries/kafka-init-$(jar_version)-dist.zip

clean: 
	rm -rf .topic-operator.tmp .user-operator.tmp .kafka-init.tmp .cluster-operator.tmp ./tmp

# We unzip each of the -dist.zip files into tmp which is then
# COPYed in the Dockerfile, so can delete tmp on exit
.INTERMEDIATE: tmp

.topic-operator.tmp: $(topic_operator_dist)
	test -d tmp || mkdir tmp
	unzip -qo $(topic_operator_dist) -d tmp
	touch .topic-operator.tmp

.user-operator.tmp: $(user_operator_dist)
	test -d tmp || mkdir tmp
	unzip -qo $(user_operator_dist) -d tmp
	touch .user-operator.tmp

.kafka-init.tmp: $(kafka_init_dist)
	test -d tmp || mkdir tmp
	unzip -qo $(kafka_init_dist) -d tmp
	touch .kafka-init.tmp

.cluster-operator.tmp: $(cluster_operator_dist)
	test -d tmp || mkdir tmp
	unzip -qo $(cluster_operator_dist) -d tmp
	touch .cluster-operator.tmp

docker_build: .topic-operator.tmp .user-operator.tmp .kafka-init.tmp .cluster-operator.tmp docker_build_default
	# Pull dependencies first

include ../../Makefile.docker

.PHONY: build clean release
