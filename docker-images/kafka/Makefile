RELEASE_VERSION ?= $(shell cat ../../release.version)
PROJECT_NAME=kafka

clean:
	rm -rf tmp
	rm -f .*.tmp

.kafka-agent.tmp: ../../kafka-agent/target/kafka-agent-$(RELEASE_VERSION).jar
	test -d tmp || mkdir tmp
	cp ../../kafka-agent/target/kafka-agent-$(RELEASE_VERSION).jar tmp/
    sh "mvn dependency:copy-dependencies -f kafka-thirdparty-libs"
	touch .kafka-agent.tmp

docker_build: .kafka-agent.tmp

include ../../Makefile.docker

.PHONY: build clean release
