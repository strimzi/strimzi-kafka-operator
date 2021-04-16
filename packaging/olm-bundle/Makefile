PROJECT_NAME=olm-bundle

include ../../Makefile.os

RELEASE_VERSION ?= latest

manifests_clean:
	rm -rf manifests

update_bundle:
	./update-bundle.sh

docker_build: update_bundle
docker_tag:
docker_push:

all: docker_build
clean: manfests_clean

.PHONY: build clean release spotbugs
