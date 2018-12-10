PROJECT_NAME=install

include ../Makefile.os

RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../strimzi-$(RELEASE_VERSION)/$(PROJECT_NAME)

release:
	mkdir -p $(RELEASE_PATH)
	$(CP) -r ./cluster-operator $(RELEASE_PATH)/
	$(CP) -r ./user-operator $(RELEASE_PATH)/
	$(CP) -r ./topic-operator $(RELEASE_PATH)/

.PHONY: all build clean docker_build docker_push docker_tag findbugs
