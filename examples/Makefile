PROJECT_NAME=examples

include ../Makefile.os

RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../strimzi-$(RELEASE_VERSION)/$(PROJECT_NAME)

release:
	mkdir -p $(RELEASE_PATH)
	$(CP) -r ./templates $(RELEASE_PATH)/
	$(CP) -r ./kafka $(RELEASE_PATH)/
	$(CP) -r ./kafka-connect $(RELEASE_PATH)/
	$(CP) -r ./kafka-mirror-maker $(RELEASE_PATH)/
	$(CP) -r ./kafka-mirror-maker-2 $(RELEASE_PATH)/
	$(CP) -r ./kafka-bridge $(RELEASE_PATH)/
	$(CP) -r ./user $(RELEASE_PATH)/
	$(CP) -r ./topic $(RELEASE_PATH)/
	$(CP) -r ./metrics $(RELEASE_PATH)/

.PHONY: all build clean docker_build docker_push docker_tag spotbugs
