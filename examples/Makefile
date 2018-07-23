PROJECT_NAME=examples

RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../strimzi-$(RELEASE_VERSION)/$(PROJECT_NAME)

release:
	mkdir -p $(RELEASE_PATH)
	cp -r ./install $(RELEASE_PATH)/
	cp -r ./templates $(RELEASE_PATH)/
	cp -r ./configmaps $(RELEASE_PATH)/
	cp -r ./kafka $(RELEASE_PATH)/
	cp -r ./kafka-connect $(RELEASE_PATH)/


.PHONY: all build clean docker_build docker_push docker_tag