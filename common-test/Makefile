PROJECT_NAME=common-test

docker_build: java_build
docker_push:
docker_tag:
all: docker_build docker_push
clean: java_clean

#include ../Makefile.docker

java_build:
	echo "Building JAR file ..."
	mvn -DtrimStackTrace=false install

java_clean:
	echo "Cleaning Maven build ..."
	mvn clean

#include ../Makefile.maven

.PHONY: build clean release
