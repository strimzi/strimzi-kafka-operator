PROJECT_NAME=install

RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../strimzi-$(RELEASE_VERSION)/$(PROJECT_NAME)

release:
	mkdir -p $(RELEASE_PATH)
	cp -r ./cluster-operator $(RELEASE_PATH)/
	cp -r ./user-operator $(RELEASE_PATH)/
	cp -r ./topic-operator $(RELEASE_PATH)/

.PHONY: all build clean docker_build docker_push docker_tag
