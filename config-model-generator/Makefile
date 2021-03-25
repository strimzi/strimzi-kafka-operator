PROJECT_NAME=cluster-operator-metadata

docker_build: java_build

docker_tag:

docker_push:

java_build:
	./build-config-models.sh build

spotbugs:
	mvn $(MVN_ARGS) spotbugs:check

all: docker_build docker_push

clean:
	./build-config-models.sh clean

#include ../Makefile.maven

.PHONY: build clean release





