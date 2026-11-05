RELEASE_VERSION ?= $(shell cat ../../../release.version)
PROJECT_NAME=kafka
SOVA_VERSION ?= $(shell cat ../../../sova.version)

include ../../../Makefile.docker

kafka_agent_dist := ../../artifacts/binaries/kafka-agent-$(RELEASE_VERSION)-dist.zip
tracing_agent_dist := ../../artifacts/binaries/tracing-agent-$(RELEASE_VERSION)-dist.zip
sova_dist := ../../artifacts/binaries/sova-$(SOVA_VERSION)-dist.zip

clean:
	rm -rf tmp
	rm -f .*.tmp

.kafka-agent.tmp: $(kafka_agent_dist)
	test -d tmp || mkdir tmp
	unzip -qoj $(kafka_agent_dist) lib/io.strimzi.kafka-agent-$(RELEASE_VERSION).jar -d tmp/
	mv tmp/io.strimzi.kafka-agent-$(RELEASE_VERSION).jar tmp/kafka-agent-$(RELEASE_VERSION).jar
	touch .kafka-agent.tmp

.tracing-agent.tmp: $(tracing_agent_dist)
	test -d tmp || mkdir tmp
	unzip -qoj $(tracing_agent_dist) lib/io.strimzi.tracing-agent-$(RELEASE_VERSION).jar -d tmp/
	mv tmp/io.strimzi.tracing-agent-$(RELEASE_VERSION).jar tmp/tracing-agent-$(RELEASE_VERSION).jar
	touch .tracing-agent.tmp

.sova.tmp: $(sova_dist)
	test -d tmp || mkdir tmp
	unzip -qo $(sova_dist) -d tmp/sova
	mv tmp/sova/sova-*/* tmp/sova/
	rm -rf tmp/sova/sova-*
	touch .sova.tmp

.thirdparty-libs-$(THIRD_PARTY_LIBS).tmp: ../../artifacts/binaries/kafka-thirdparty-libs/$(THIRD_PARTY_LIBS).zip
	test -d tmp/thirdparty-libs-$(THIRD_PARTY_LIBS) || mkdir -p tmp/thirdparty-libs-$(THIRD_PARTY_LIBS)
	unzip -qoj ../../artifacts/binaries/kafka-thirdparty-libs/$(THIRD_PARTY_LIBS).zip -d tmp/thirdparty-libs-$(THIRD_PARTY_LIBS)
	touch .thirdparty-libs-$(THIRD_PARTY_LIBS).tmp

.thirdparty-libs-sova.tmp: ../../artifacts/binaries/kafka-thirdparty-libs/sova.zip
	test -d tmp/sova-libs || mkdir -p tmp/sova-libs
	unzip -qo ../../artifacts/binaries/kafka-thirdparty-libs/sova.zip -d tmp/sova-libs
	touch .thirdparty-libs-sova.tmp

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

docker_build: .kafka-agent.tmp .tracing-agent.tmp .sova.tmp .thirdparty-libs-sova.tmp .thirdparty-libs-$(THIRD_PARTY_LIBS).tmp .thirdparty-libs-cc.tmp .kafka-$(KAFKA_VERSION).tmp .kafka-$(KAFKA_VERSION)-ignorelist.tmp docker_build_default
	# Pull dependencies first

docker_build:
	../find-classpath-collision.sh strimzi/$(PROJECT_NAME):$(BUILD_TAG)$(DOCKER_PLATFORM_TAG_SUFFIX) /opt/kafka/libs "tmp/$(KAFKA_VERSION).ignorelist"
	../find-classpath-collision.sh strimzi/$(PROJECT_NAME):$(BUILD_TAG)$(DOCKER_PLATFORM_TAG_SUFFIX) /opt/sova/libs /dev/null

.PHONY: build clean release
