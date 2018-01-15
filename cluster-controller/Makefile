PROJECT_NAME=cluster-controller

all: java_build docker_build docker_push
clean: java_clean

include ../Makefile.docker

include ../Makefile.maven

.PHONY: build clean release
