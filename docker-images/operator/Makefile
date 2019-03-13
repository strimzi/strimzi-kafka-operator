PROJECT_NAME=operator

clean:
	rm -rf lib
	rm -rf tmp
	rm .*.mk

.topic-operator.mk: ../../topic-operator/target/*-dist.zip
	test -d tmp || mkdir tmp
	unzip -qo ../../topic-operator/target/*-dist.zip -d tmp
	touch .topic-operator.mk

.user-operator.mk: ../../user-operator/target/*-dist.zip
	test -d tmp || mkdir tmp
	unzip -qo ../../user-operator/target/*-dist.zip -d tmp
	touch .user-operator.mk

.kafka-init.mk: ../../kafka-init/target/*-dist.zip
	test -d tmp || mkdir tmp
	unzip -qo ../../kafka-init/target/*-dist.zip -d tmp
	touch .kafka-init.mk

.cluster-operator.mk: ../../cluster-operator/target/*-dist.zip
	test -d tmp || mkdir tmp
	unzip -qo ../../cluster-operator/target/*-dist.zip -d tmp
	touch .cluster-operator.mk

docker_build: .topic-operator.mk .user-operator.mk .kafka-init.mk .cluster-operator.mk

include ../../Makefile.docker

.PHONY: build clean release
