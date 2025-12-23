PROJECT_NAME=cluster-operator-metadata

docker_build: java_build
docker_push:
docker_tag:

all: docker_build docker_push

clean: java_clean

include ../Makefile.maven

.PHONY: build clean release