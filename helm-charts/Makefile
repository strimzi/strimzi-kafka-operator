PROJECT_NAME=helm-charts

RELEASE_VERSION ?= latest
CHART_SEMANTIC_RELEASE_VERSION ?= $(shell cat ../release.version | sed 's/\([0-9.]*\).*/\1/')
CHART_NAME=strimzi-kafka-operator
CHART_PATH=./$(CHART_NAME)/
CHART_RENDERED_TEMPLATES_TMP=../target/charts
CHART_RENDERED_TEMPLATES_EXAMPLES=../examples/install/cluster-operator/

helm_clean:
	rm -rf $(CHART_RENDERED_TEMPLATES_TMP)
	rm -rf strimzi-$(RELEASE_VERSION)/charts/
	#rm -rf $(CHART_RENDERED_TEMPLATES_EXAMPLES)
	rm -f $(CHART_NAME)-*.tgz

helm_lint:
	# Linting Helm Chart
	helm lint --debug $(CHART_PATH)

helm_template:
	# Generating rendered template files to: $(CHART_RENDERED_TEMPLATES_TMP)
	mkdir -p $(CHART_RENDERED_TEMPLATES_TMP)
	helm template --namespace myproject --output-dir $(CHART_RENDERED_TEMPLATES_TMP) --set imageTagOverride=$(RELEASE_VERSION) $(CHART_PATH)

helm_examples: helm_template
	# Copying rendered template files to: $(CHART_RENDERED_TEMPLATES_EXAMPLES)
	mkdir -p $(CHART_RENDERED_TEMPLATES_EXAMPLES)
	cp $(CHART_RENDERED_TEMPLATES_TMP)/$(CHART_NAME)/templates/* $(CHART_RENDERED_TEMPLATES_EXAMPLES)
	# Remove Heml-related labels
	find $(CHART_RENDERED_TEMPLATES_EXAMPLES) -type f -name '*.yaml' -exec yq d -i {} metadata.labels.chart \;
	find $(CHART_RENDERED_TEMPLATES_EXAMPLES) -type f -name '*.yaml' -exec yq d -i {} metadata.labels.component \;
	find $(CHART_RENDERED_TEMPLATES_EXAMPLES) -type f -name '*.yaml' -exec yq d -i {} metadata.labels.release \;
	find $(CHART_RENDERED_TEMPLATES_EXAMPLES) -type f -name '*.yaml' -exec yq d -i {} metadata.labels.heritage \;

helm_pkg: helm_lint helm_examples
	# Copying unarchived Helm Chart to release directory
	mkdir -p strimzi-$(RELEASE_VERSION)/charts/
	cp -r $(CHART_PATH) strimzi-$(RELEASE_VERSION)/charts/$(CHART_NAME)
	# Packaging helm chart with semantic version: $(CHART_SEMANTIC_RELEASE_VERSION)
	helm package --version $(CHART_SEMANTIC_RELEASE_VERSION) --app-version $(CHART_SEMANTIC_RELEASE_VERSION) --destination ./ $(CHART_PATH)

docker_build: helm_pkg
docker_tag:
docker_push:

all: docker_build
clean: helm_clean

.PHONY: build clean release


