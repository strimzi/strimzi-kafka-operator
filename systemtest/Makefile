PROJECT_NAME=systemtest

docker_build: java_build
docker_push:
docker_tag:
all: docker_build docker_push
clean: java_clean

#include ../Makefile.docker

include ../Makefile.maven

.PHONY: build clean release
