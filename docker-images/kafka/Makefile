RELEASE_VERSION ?= $(shell cat ../../release.version)
PROJECT_NAME=kafka

clean:
	rm -rf tmp
	rm -f .*.tmp
	rm -rf kafka-thirdparty-libs/*/target
	rm -f tmp/*.whitelist

.kafka-agent.tmp: ../../kafka-agent/target/kafka-agent-$(RELEASE_VERSION).jar
	test -d tmp || mkdir tmp
	cp ../../kafka-agent/target/kafka-agent-$(RELEASE_VERSION).jar tmp/kafka-agent.jar
	touch .kafka-agent.tmp

.mirror-maker-agent.tmp: ../../mirror-maker-agent/target/mirror-maker-agent-$(RELEASE_VERSION).jar
	test -d tmp || mkdir tmp
	cp ../../mirror-maker-agent/target/mirror-maker-agent-$(RELEASE_VERSION).jar tmp/mirror-maker-agent.jar
	touch .mirror-maker-agent.tmp

.tracing-agent.tmp: ../../tracing-agent/target/tracing-agent-$(RELEASE_VERSION).jar
	test -d tmp || mkdir tmp
	cp ../../tracing-agent/target/tracing-agent-$(RELEASE_VERSION).jar tmp/tracing-agent.jar
	touch .tracing-agent.tmp

.thirdparty-libs-$(THIRD_PARTY_LIBS).tmp: kafka-thirdparty-libs/$(THIRD_PARTY_LIBS)/pom.xml
	test -d tmp || mkdir tmp
	mvn dependency:copy-dependencies $(MVN_ARGS) -f kafka-thirdparty-libs/$(THIRD_PARTY_LIBS)/pom.xml
	touch .thirdparty-libs-$(THIRD_PARTY_LIBS).tmp

.thirdparty-libs-cc.tmp: kafka-thirdparty-libs/cc/pom.xml
	test -d tmp || mkdir tmp
	mvn dependency:copy-dependencies $(MVN_ARGS) -f kafka-thirdparty-libs/cc/pom.xml
	touch .thirdparty-libs-cc.tmp

docker_build: .kafka-agent.tmp .tracing-agent.tmp .mirror-maker-agent.tmp .thirdparty-libs-$(THIRD_PARTY_LIBS).tmp .thirdparty-libs-cc.tmp docker_build_default
	# Pull dependencies first

include ../../Makefile.docker

docker_build:
	../find-classpath-collision.sh strimzi/$(PROJECT_NAME):$(BUILD_TAG) /opt/kafka/libs "tmp/$(KAFKA_VERSION).whitelist"

.PHONY: build clean release
