PROJECT_NAME=topic-operator
MAIN=io.strimzi.operator.topic.Main

docker_build: java_build

all: docker_build docker_push
clean: java_clean

include ../Makefile.operator

.PHONY: build clean release
