TOPDIR = $(dir $(lastword $(MAKEFILE_LIST)))
RELEASE_VERSION ?= $(shell cat $(TOPDIR)/release.version)
DOCKER_TARGETS = docker_build docker_push docker_tag
DOCKER_TAG ?= latest

.PHONY: build clean release all $(DOCKER_TARGETS)

clean:
	./build.sh clean

docker_build:
	./build.sh docker_build

docker_tag:
	./build.sh docker_tag

docker_push:
	./build.sh docker_push

all: docker_build docker_push

.PHONY: build clean release all $(SUBDIRS) $(DOCKER_TARGETS) spotbugs

