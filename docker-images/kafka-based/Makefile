TOPDIR = $(dir $(lastword $(MAKEFILE_LIST)))
RELEASE_VERSION ?= $(shell cat $(TOPDIR)/release.version)
DOCKER_TARGETS = docker_build docker_push docker_tag docker_load docker_save docker_amend_manifest docker_push_manifest docker_delete_manifest
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

docker_save:
	./build.sh docker_save

docker_load:
	./build.sh docker_load

docker_amend_manifest:
	./build.sh docker_amend_manifest

docker_push_manifest:
	./build.sh docker_push_manifest

docker_delete_manifest:
	./build.sh docker_delete_manifest

all: docker_build docker_push

.PHONY: build clean release all $(SUBDIRS) $(DOCKER_TARGETS) spotbugs

