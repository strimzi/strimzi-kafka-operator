PROJECT_NAME=resources

RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../strimzi-$(RELEASE_VERSION)/$(PROJECT_NAME)

release:
	mkdir -p $(RELEASE_PATH)
	cp -r ./openshift $(RELEASE_PATH)/
	cp -r ./kubernetes $(RELEASE_PATH)/

.PHONY: all build clean docker_build docker_push docker_tag