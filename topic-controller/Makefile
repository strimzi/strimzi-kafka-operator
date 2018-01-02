PROJECT_NAME=topic-controller

all: java_build docker_build docker_push
clean: java_clean

include ../Makefile.docker

include ../Makefile.maven

include ../Makefile.release

.PHONY: build clean
