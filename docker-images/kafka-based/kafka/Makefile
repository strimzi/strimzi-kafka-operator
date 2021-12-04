RELEASE_VERSION ?= $(shell cat ../../../release.version)
PROJECT_NAME=kafka

include ../../../Makefile.docker

kafka_agent_dist := ../../artifacts/binaries/kafka-agent-$(RELEASE_VERSION)-dist.zip
mirror_maker_agent_dist := ../../artifacts/binaries/mirror-maker-agent-$(RELEASE_VERSION)-dist.zip
tracing_agent_dist := ../../artifacts/binaries/tracing-agent-$(RELEASE_VERSION)-dist.zip

clean:
	rm -rf tmp
	rm -f .*.tmp

.kafka-agent.tmp: $(kafka_agent_dist)
	test -d tmp || mkdir tmp
	unzip -qoj $(kafka_agent_dist) lib/io.strimzi.kafka-agent-$(RELEASE_VERSION).jar -d tmp/
	mv tmp/io.strimzi.kafka-agent-$(RELEASE_VERSION).jar tmp/kafka-agent-$(RELEASE_VERSION).jar
	touch .kafka-agent.tmp

.mirror-maker-agent.tmp: $(mirror_maker_agent_dist)
	test -d tmp || mkdir tmp
	unzip -qoj $(mirror_maker_agent_dist) lib/io.strimzi.mirror-maker-agent-$(RELEASE_VERSION).jar -d tmp/
	mv tmp/io.strimzi.mirror-maker-agent-$(RELEASE_VERSION).jar tmp/mirror-maker-agent-$(RELEASE_VERSION).jar
	touch .mirror-maker-agent.tmp

.tracing-agent.tmp: $(tracing_agent_dist)
	test -d tmp || mkdir tmp
	unzip -qoj $(tracing_agent_dist) lib/io.strimzi.tracing-agent-$(RELEASE_VERSION).jar -d tmp/
	mv tmp/io.strimzi.tracing-agent-$(RELEASE_VERSION).jar tmp/tracing-agent-$(RELEASE_VERSION).jar
	touch .tracing-agent.tmp

.thirdparty-libs-$(THIRD_PARTY_LIBS).tmp: ../../artifacts/binaries/kafka-thirdparty-libs/$(THIRD_PARTY_LIBS).zip
	test -d tmp/thirdparty-libs-$(THIRD_PARTY_LIBS) || mkdir -p tmp/thirdparty-libs-$(THIRD_PARTY_LIBS)
	unzip -qoj ../../artifacts/binaries/kafka-thirdparty-libs/$(THIRD_PARTY_LIBS).zip -d tmp/thirdparty-libs-$(THIRD_PARTY_LIBS)
	touch .thirdparty-libs-$(THIRD_PARTY_LIBS).tmp

.thirdparty-libs-cc.tmp: ../../artifacts/binaries/kafka-thirdparty-libs/cc.zip
	test -d tmp/cc || mkdir -p tmp/cc
	unzip -qo ../../artifacts/binaries/kafka-thirdparty-libs/cc.zip -d tmp/cc
	touch .thirdparty-libs-cc.tmp

.kafka-$(KAFKA_VERSION).tmp: ../../artifacts/binaries/kafka/archives/kafka_2.13-$(KAFKA_VERSION).tgz
	test -d tmp/$(KAFKA_VERSION) || mkdir -p tmp/$(KAFKA_VERSION)
	tar xvfz ../../artifacts/binaries/kafka/archives/kafka_2.13-$(KAFKA_VERSION).tgz -C tmp/$(KAFKA_VERSION) --strip-components=1
	touch .kafka-$(KAFKA_VERSION).tmp

.kafka-$(KAFKA_VERSION)-ignorelist.tmp: ../../artifacts/binaries/kafka/$(KAFKA_VERSION).ignorelist
	test -d tmp/$(KAFKA_VERSION) || mkdir -p tmp/$(KAFKA_VERSION)
	cp ../../artifacts/binaries/kafka/$(KAFKA_VERSION).ignorelist tmp/$(KAFKA_VERSION).ignorelist
	touch .kafka-$(KAFKA_VERSION)-ignorelist.tmp

docker_build: .kafka-agent.tmp .tracing-agent.tmp .mirror-maker-agent.tmp .thirdparty-libs-$(THIRD_PARTY_LIBS).tmp .thirdparty-libs-cc.tmp .kafka-$(KAFKA_VERSION).tmp .kafka-$(KAFKA_VERSION)-ignorelist.tmp docker_build_default
	# Pull dependencies first

docker_build:
	../find-classpath-collision.sh strimzi/$(PROJECT_NAME):$(BUILD_TAG)$(DOCKER_PLATFORM_TAG_SUFFIX) /opt/kafka/libs "tmp/$(KAFKA_VERSION).ignorelist"

.PHONY: build clean release
