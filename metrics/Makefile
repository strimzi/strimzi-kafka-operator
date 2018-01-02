PROJECT_NAME=metrics
RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../strimzi-$(RELEASE_VERSION)/$(PROJECT_NAME)

release:
	mkdir -p $(RELEASE_PATH)
	cp -r ./* $(RELEASE_PATH)/

.PHONY: all build clean docker_build docker_push docker_tag