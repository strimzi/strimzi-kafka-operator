PROJECT_NAME=metrics

include ../Makefile.os

RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../strimzi-$(RELEASE_VERSION)/examples/$(PROJECT_NAME)

release:
	mkdir -p $(RELEASE_PATH)
	mkdir -p $(RELEASE_PATH)/grafana-dashboards
	mkdir -p $(RELEASE_PATH)/alerting-rules
	$(CP) -r ./examples/kafka/* $(RELEASE_PATH)/
	$(CP) -r ./examples/grafana/*.json $(RELEASE_PATH)/grafana-dashboards/

.PHONY: all build clean docker_build docker_push docker_tag findbugs