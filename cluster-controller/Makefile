PROJECT_NAME=barnabas-cluster-controller

all: java_build docker_build docker_push
clean: java_clean

include ../Makefile.docker

include ../Makefile.java

include ../Makefile.release

.PHONY: build clean
