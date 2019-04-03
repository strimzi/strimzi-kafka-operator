PROJECT_NAME=metrics

include ../Makefile.os

RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../strimzi-$(RELEASE_VERSION)/examples/$(PROJECT_NAME)

release:
	mkdir -p $(RELEASE_PATH)
	mkdir -p $(RELEASE_PATH)/grafana-dashboards
	mkdir -p $(RELEASE_PATH)/prometheus-install
	mkdir -p $(RELEASE_PATH)/prometheus-install-additional-properties
	mkdir -p $(RELEASE_PATH)/prometheus-install-alertmanager-config
	$(CP) -r ./examples/kafka/* $(RELEASE_PATH)/
	$(CP) -r ./examples/grafana/*.json $(RELEASE_PATH)/grafana-dashboards/
	$(CP) -r ./examples/prometheus/install/*.yaml $(RELEASE_PATH)/prometheus-install/
	$(CP) -r ./examples/prometheus/additional-properties/*.yaml $(RELEASE_PATH)/prometheus-additional-properties/
	$(CP) -r ./examples/prometheus/alertmanager-config/*.yaml $(RELEASE_PATH)/prometheus-alertmanager-config/

.PHONY: all build clean docker_build docker_push docker_tag findbugs