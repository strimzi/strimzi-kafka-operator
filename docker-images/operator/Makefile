PROJECT_NAME=operator

clean:
	rm -rf lib
	rm -rf tmp
	rm .*.tmp

.topic-operator.tmp: ../../topic-operator/target/*-dist.zip
	test -d tmp || mkdir tmp
	unzip -qo ../../topic-operator/target/*-dist.zip -d tmp
	touch .topic-operator.tmp

.user-operator.tmp: ../../user-operator/target/*-dist.zip
	test -d tmp || mkdir tmp
	unzip -qo ../../user-operator/target/*-dist.zip -d tmp
	touch .user-operator.tmp

.kafka-init.tmp: ../../kafka-init/target/*-dist.zip
	test -d tmp || mkdir tmp
	unzip -qo ../../kafka-init/target/*-dist.zip -d tmp
	touch .kafka-init.tmp

.cluster-operator.tmp: ../../cluster-operator/target/*-dist.zip
	test -d tmp || mkdir tmp
	unzip -qo ../../cluster-operator/target/*-dist.zip -d tmp
	touch .cluster-operator.tmp

docker_build: .topic-operator.tmp .user-operator.tmp .kafka-init.tmp .cluster-operator.tmp

include ../../Makefile.docker

.PHONY: build clean release
