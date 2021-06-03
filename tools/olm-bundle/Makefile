PROJECT_NAME=olm-bundle

BUNDLE_VERSION ?= $(shell cat $(dirname $(realpath $0))../../release.version | cut -d'-' -f1)
.DEFAULT_GOAL := default

all:
clean:
release:

include ../../Makefile.docker

olm_install:
	./generate-olm-bundle.sh BUNDLE_VERSION=$(BUNDLE_VERSION) DOCKER_REGISTRY=$(DOCKER_REGISTRY) DOCKER_ORG=$(DOCKER_ORG) DOCKER_TAG=$(DOCKER_TAG)

default: olm_install

.PHONY: build clean release spotbugs
