PROJECT_NAME=helm-charts

include ../../Makefile.os

RELEASE_VERSION ?= latest
CHART_SEMANTIC_RELEASE_VERSION ?= $(shell cat ../../release.version | tr A-Z a-z)
CHART_NAME = strimzi-kafka-operator
CHART_PATH ?= ../helm2/strimzi-kafka-operator/
CHART_RENDERED_TEMPLATES_TMP = ../../target/charts
HELM_CLI = helm2

helm_clean:
	rm -rf $(CHART_RENDERED_TEMPLATES_TMP)
	rm -rf strimzi-$(RELEASE_VERSION)/charts/
	rm -f $(CHART_NAME)-*.tgz

helm_lint:
	# Linting Helm Chart
	$(HELM_CLI) lint --debug $(CHART_PATH)

helm_pkg: helm_lint
	$(HELM_CLI) init --client-only
	# Copying unarchived Helm Chart to release directory
	mkdir -p strimzi-$(RELEASE_VERSION)/charts/
	$(CP) -r $(CHART_PATH) strimzi-$(RELEASE_VERSION)/charts/$(CHART_NAME)
	# Packaging helm chart with semantic version: $(CHART_SEMANTIC_RELEASE_VERSION)
	$(HELM_CLI) package --version $(CHART_SEMANTIC_RELEASE_VERSION) --app-version $(CHART_SEMANTIC_RELEASE_VERSION) --destination ./ $(CHART_PATH)
	rm -rf strimzi-$(RELEASE_VERSION)

docker_build: helm_pkg
docker_tag:
docker_push:

all: docker_build
clean: helm_clean

.PHONY: build clean release spotbugs
