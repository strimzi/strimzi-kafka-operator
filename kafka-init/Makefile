PROJECT_NAME=kafka-init
MAIN=io.strimzi.kafka.init.Main

docker_build: java_build

all: docker_build docker_push
clean: java_clean

include ../Makefile.operator

.PHONY: build clean release
