PROJECT_NAME=olm-manifests

include ../../Makefile.os

RELEASE_VERSION ?= latest

manifests_clean:
	rm -rf manifests

generate_manifests:
	./generate-manifests.sh $(RELEASE_VERSION)

docker_build: generate_manifests
docker_tag:
docker_push:

all: docker_build
clean: manfests_clean

.PHONY: build clean release spotbugs
