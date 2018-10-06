PROJECT_NAME=metrics
RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../strimzi-$(RELEASE_VERSION)/examples/$(PROJECT_NAME)

release:
	mkdir -p $(RELEASE_PATH)
	mkdir -p $(RELEASE_PATH)/grafana-dashboards
	cp -r ./examples/kafka/* $(RELEASE_PATH)/
	cp -r ./examples/grafana/*.json $(RELEASE_PATH)/grafana-dashboards/

.PHONY: all build clean docker_build docker_push docker_tag