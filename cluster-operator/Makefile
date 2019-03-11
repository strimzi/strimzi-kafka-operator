PROJECT_NAME=cluster-operator
MAIN=io.strimzi.operator.cluster.Main

docker_build: java_build
all: docker_build docker_push
clean: java_clean

include ../Makefile.operator

.PHONY: build clean release
