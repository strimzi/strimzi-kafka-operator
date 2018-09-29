PROJECT_NAME=helm-charts

RELEASE_VERSION ?= latest
CHART_SEMANTIC_RELEASE_VERSION ?= $(shell cat ../release.version | tr A-Z a-z)
CHART_NAME=strimzi-kafka-operator
CHART_PATH=./$(CHART_NAME)/
CHART_RENDERED_TEMPLATES_TMP=../target/charts
CHART_RENDERED_TEMPLATES_EXAMPLES=../examples/install/cluster-operator/

helm_clean:
	rm -rf $(CHART_RENDERED_TEMPLATES_TMP)
	rm -rf strimzi-$(RELEASE_VERSION)/charts/
	rm -f $(CHART_NAME)-*.tgz

helm_lint:
	# Linting Helm Chart
	helm lint --debug $(CHART_PATH)

helm_template:
	# Generating rendered template files to: $(CHART_RENDERED_TEMPLATES_TMP)
	mkdir -p $(CHART_RENDERED_TEMPLATES_TMP)
	helm template --namespace myproject --output-dir $(CHART_RENDERED_TEMPLATES_TMP) $(CHART_PATH)

helm_examples: helm_clean helm_template
	# Remove Helm-related labels
	find $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -exec yq d -i {} metadata.labels.chart \;
	find $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -exec yq d -i {} metadata.labels.component \;
	find $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -exec yq d -i {} metadata.labels.release \;
	find $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -exec yq d -i {} metadata.labels.heritage \;
	find $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -exec yq d -i {} metadata.namespace \;
	# Copying rendered template files to: $(CHART_RENDERED_TEMPLATES_EXAMPLES)
	mkdir -p $(CHART_RENDERED_TEMPLATES_EXAMPLES)
	# Find rendered resources which are not CustomResourceDefinition and move them
	find $(CHART_RENDERED_TEMPLATES_TMP) -type f -name '*.yaml' -printf "%p " -exec yq r {} kind \; \
	| grep -v 'CustomResourceDefinition$$' \
	| sed -E 's/([^ ]*) ([a-zA-Z0-9]*)$$/\1/' \
	| xargs -IFILE cp FILE $(CHART_RENDERED_TEMPLATES_EXAMPLES)

helm_pkg: helm_lint helm_examples
	# Copying unarchived Helm Chart to release directory
	mkdir -p strimzi-$(RELEASE_VERSION)/charts/
	cp -r $(CHART_PATH) strimzi-$(RELEASE_VERSION)/charts/$(CHART_NAME)
	# Packaging helm chart with semantic version: $(CHART_SEMANTIC_RELEASE_VERSION)
	helm package --version $(CHART_SEMANTIC_RELEASE_VERSION) --app-version $(CHART_SEMANTIC_RELEASE_VERSION) --destination ./ $(CHART_PATH)
	rm -rf strimzi-$(RELEASE_VERSION)

docker_build: helm_pkg
docker_tag:
docker_push:

all: docker_build
clean: helm_clean

.PHONY: build clean release
