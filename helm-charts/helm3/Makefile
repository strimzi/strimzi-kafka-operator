PROJECT_NAME=helm-charts

include ../../../Makefile.os

RELEASE_VERSION ?= latest
CHART_SEMANTIC_RELEASE_VERSION ?= $(shell cat ../../../release.version | tr A-Z a-z)
CHART_NAME=strimzi-kafka-operator
CHART_PATH ?= ../helm3/strimzi-kafka-operator/
CHART_RENDERED_TEMPLATES_TMP=../../../target/charts
CHART_RENDERED_TEMPLATES_CLUSTERROLES=../../../cluster-operator/src/main/resources/cluster-roles/
CHART_RENDERED_TEMPLATES_INSTALL=../../install/cluster-operator/
HELM_CLI=helm

helm_clean:
	rm -rf $(CHART_RENDERED_TEMPLATES_TMP)
	rm -rf strimzi-$(RELEASE_VERSION)/charts/
	rm -f $(CHART_NAME)-*.tgz

helm_lint:
	# Linting Helm Chart
	$(HELM_CLI) lint --debug $(CHART_PATH)

helm_template:
	../kafka-version-tpl.sh strimzi-kafka-operator/templates/_kafka_image_map.tpl
	# Generating rendered template files to: $(CHART_RENDERED_TEMPLATES_TMP)
	mkdir -p $(CHART_RENDERED_TEMPLATES_TMP)
	$(HELM_CLI) template --namespace myproject --output-dir $(CHART_RENDERED_TEMPLATES_TMP) $(CHART_PATH)

helm_install: helm_clean helm_template
	# Remove Helm-related labels
	$(FIND) $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -exec $(SED) -i '/^---/d' {} \;
	$(FIND) $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -exec $(SED) -i '/^# Source: /d' {} \;
	$(FIND) $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -exec yq eval -i 'del .metadata.labels.chart' {} \;
	$(FIND) $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -exec yq eval -i 'del .metadata.labels.component' {}  \;
	$(FIND) $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -exec yq eval -i 'del .metadata.labels.release' {} \;
	$(FIND) $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -exec yq eval -i 'del .metadata.labels.heritage' {} \;
	$(FIND) $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -exec yq eval -i 'del .metadata.namespace' {} \;
	# Copying rendered template files to: $(CHART_RENDERED_TEMPLATES_INSTALL)
	mkdir -p $(CHART_RENDERED_TEMPLATES_INSTALL)
	# Find rendered resources which are not CustomResourceDefinition and move them
	$(FIND) $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -printf "%p " -exec yq eval '.kind' {} \; \
	| grep -v 'CustomResourceDefinition$$' \
	| $(SED) -E 's/([^ ]*) ([a-zA-Z0-9]*)$$/\1/' \
	| xargs -IFILE $(CP) FILE $(CHART_RENDERED_TEMPLATES_INSTALL)
	# Copying rendered template files to: $(CHART_RENDERED_TEMPLATES_CLUSTERROLES)
	mkdir -p $(CHART_RENDERED_TEMPLATES_CLUSTERROLES)
	# Find rendered resources which are not CustomResourceDefinition and move them
	$(FIND) $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*ClusterRole-*.yaml' -printf "%p " -exec yq eval '.kind' {} \; \
	| $(SED) -E 's/([^ ]*) ([a-zA-Z0-9]*)$$/\1/' \
	| xargs -IFILE $(CP) FILE $(CHART_RENDERED_TEMPLATES_CLUSTERROLES)

helm_pkg: helm_lint helm_install
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
