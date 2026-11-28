RELEASE_VERSION ?= $(shell cat ../../../release.version)
PROJECT_NAME=kafka
SOVA_VERSION ?= $(shell cat ../../../sova.version)

include ../../../Makefile.docker

# SOVA_DIST can point to a locally built Sova distribution zip (see DEV_GUIDE.md)
SOVA_DIST ?= ../../artifacts/binaries/sova-$(SOVA_VERSION)-dist.zip

clean:
	rm -rf tmp
	rm -f .*.tmp

.sova.tmp: $(SOVA_DIST)
	test -d tmp || mkdir tmp
	# Remove the previous distribution first, so that JARs of another Sova version do not stay on the classpath
	rm -rf tmp/sova
	unzip -qo $(SOVA_DIST) -d tmp/sova
	mv tmp/sova/sova-*/* tmp/sova/
	rm -rf tmp/sova/sova-*
	touch .sova.tmp

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

docker_build: .sova.tmp .thirdparty-libs-$(THIRD_PARTY_LIBS).tmp .thirdparty-libs-cc.tmp .kafka-$(KAFKA_VERSION).tmp .kafka-$(KAFKA_VERSION)-ignorelist.tmp docker_build_default
	# Pull dependencies first

docker_build:
	../find-classpath-collision.sh strimzi/$(PROJECT_NAME):$(BUILD_TAG)$(DOCKER_PLATFORM_TAG_SUFFIX) /opt/kafka/libs "tmp/$(KAFKA_VERSION).ignorelist"
	../find-classpath-collision.sh strimzi/$(PROJECT_NAME):$(BUILD_TAG)$(DOCKER_PLATFORM_TAG_SUFFIX) /opt/sova/libs /dev/null

.PHONY: build clean release
