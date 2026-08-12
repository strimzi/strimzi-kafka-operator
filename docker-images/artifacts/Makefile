TOPDIR = $(dir $(lastword $(MAKEFILE_LIST)))
RELEASE_VERSION ?= $(shell cat $(TOPDIR)/release.version)
DOCKER_TARGETS = docker_build docker_push docker_tag
DOCKER_TAG ?= latest

.PHONY: build clean release all $(DOCKER_TARGETS)

clean:
	rm -rf binaries
	rm -rf kafka-thirdparty-libs/*/target
	mkdir -p binaries/kafka/archives
	cp kafka_2.13-4.1.0.tgz binaries/kafka/archives

java_build:
	./build.sh

java_install: java_build

all: java_install

.PHONY: build clean release all $(SUBDIRS) $(DOCKER_TARGETS) spotbugs

