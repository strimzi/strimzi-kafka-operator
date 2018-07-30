TOPDIR=$(dir $(lastword $(MAKEFILE_LIST)))
RELEASE_VERSION ?= latest
CHART_NAME=strimzi-kafka-operator
CHART_PATH=./helm-charts/$(CHART_NAME)/
CHART_RENDERED_TEMPLATES_TMP=./target/charts
CHART_RENDERED_TEMPLATES_EXAMPLES=./examples/install/cluster-operator/
CHART_SEMANTIC_RELEASE_VERSION ?= $(shell cat release.version | sed 's/\([0-9.]*\).*/\1/')

SUBDIRS=docker-images crd-generator api certificate-manager cluster-operator topic-operator kafka-init examples
DOCKER_TARGETS=docker_build docker_push docker_tag

all: $(SUBDIRS)
clean: $(SUBDIRS) docu_clean
$(DOCKER_TARGETS): $(SUBDIRS)
release: release_prepare release_version release_maven $(SUBDIRS) release_docu release_helm_pkg release_pkg docu_clean

next_version:
	echo $(shell echo $(NEXT_VERSION) | tr a-z A-Z) > release.version
	mvn versions:set -DnewVersion=$(shell echo $(NEXT_VERSION) | tr a-z A-Z)
	mvn versions:commit

release_prepare:
	echo $(shell echo $(RELEASE_VERSION) | tr a-z A-Z) > release.version
	rm -rf ./strimzi-$(RELEASE_VERSION)
	rm -f ./strimzi-$(RELEASE_VERSION).tar.gz
	mkdir ./strimzi-$(RELEASE_VERSION)

release_version:
	# TODO: This would be replaced ideally once Helm Chart templating is used for cluster and topic operator examples
	echo "Changing Docker image tags to :$(RELEASE_VERSION)"
	find ./examples -name '*.yaml' -type f -exec sed -i '/image: "\?strimzi\/[a-zA-Z0-9_-.]\+:[a-zA-Z0-9_-.]\+"\?/s/:[a-zA-Z0-9_-.]\+/:$(RELEASE_VERSION)/g' {} \;
	find ./examples -name '*.yaml' -type f -exec sed -i '/name: [a-zA-Z0-9_-]*IMAGE_TAG/{n;s/value: [a-zA-Z0-9_-.]\+/value: $(RELEASE_VERSION)/}' {} \;
	find ./examples -name '*.yaml' -type f -exec sed -i '/name: STRIMZI_DEFAULT_[a-zA-Z0-9_-]*IMAGE/{n;s/:[a-zA-Z0-9_-.]\+/:$(RELEASE_VERSION)/}' {} \;

release_maven:
	echo "Update pom versions to $(RELEASE_VERSION)"
	mvn versions:set -DnewVersion=$(shell echo $(RELEASE_VERSION) | tr a-z A-Z)
	mvn versions:commit

release_pkg:
	tar -z -cf ./strimzi-$(RELEASE_VERSION).tar.gz strimzi-$(RELEASE_VERSION)/
	zip -r ./strimzi-$(RELEASE_VERSION).zip strimzi-$(RELEASE_VERSION)/
	rm -rf ./strimzi-$(RELEASE_VERSION)

release_helm_version:
	echo "Updating default image tags in Helm Chart to $(RELEASE_VERSION)"
	# Update default image tag in chart values.yaml to RELEASE_VERSION
	sed -i 's/\(tag: \)latest/\1$(RELEASE_VERSION)/g' $(CHART_PATH)values.yaml
	# Update default image tag in chart README.md config grid with RELEASE_VERSION
	sed -i 's/\(image\.tag[^\n]*\| \)`latest`/\1`$(RELEASE_VERSION)`/g' $(CHART_PATH)README.md

release_helm_lint:
	echo "Linting Helm Chart"
	helm lint --debug $(CHART_PATH)

release_helm_template:
	echo "Generating rendered template files to: $(CHART_RENDERED_TEMPLATES_TMP)"
	rm -rf $(CHART_RENDERED_TEMPLATES_TMP)
	mkdir -p $(CHART_RENDERED_TEMPLATES_TMP)
	helm template --output-dir $(CHART_RENDERED_TEMPLATES_TMP) --set imageTagOverride=$(RELEASE_VERSION) $(CHART_PATH)
	echo "Copying rendered template files to: $(CHART_RENDERED_TEMPLATES_EXAMPLES)"
	rm -rf $(CHART_RENDERED_TEMPLATES_EXAMPLES)
	mkdir -p $(CHART_RENDERED_TEMPLATES_EXAMPLES)
	cp $(CHART_RENDERED_TEMPLATES_TMP)/$(CHART_NAME)/templates/* $(CHART_RENDERED_TEMPLATES_EXAMPLES)

release_helm_pkg: release_helm_version release_helm_lint release_helm_template
	echo "Copying unarchived Helm Chart to release directory"
	rm -rf strimzi-$(RELEASE_VERSION)/charts/
	mkdir -p strimzi-$(RELEASE_VERSION)/charts/
	cp -r $(CHART_PATH) strimzi-$(RELEASE_VERSION)/charts/$(CHART_NAME)
	echo "Packaging helm chart with semantic version: $(CHART_SEMANTIC_RELEASE_VERSION)"
	rm -f $(CHART_NAME)-*.tgz
	helm package --version $(CHART_SEMANTIC_RELEASE_VERSION) --app-version $(CHART_SEMANTIC_RELEASE_VERSION) --destination ./ $(CHART_PATH)

docu_html: docu_htmlclean
	mkdir -p documentation/html
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) documentation/book/master.adoc -o documentation/html/index.html
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) documentation/contributing/master.adoc -o documentation/html/contributing.html
	cp -vrL documentation/book/images documentation/html/images

docu_htmlnoheader: docu_htmlnoheaderclean
	mkdir -p documentation/htmlnoheader
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -s documentation/book/master.adoc -o documentation/htmlnoheader/master.html
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -s documentation/contributing/master.adoc -o documentation/htmlnoheader/contributing.html
	cp -vrL documentation/book/images documentation/htmlnoheader/images

docu_pushtowebsite: docu_htmlnoheader docu_html
	./.travis/docu-push-to-website.sh

release_docu: docu_html docu_htmlnoheader
	mkdir -p strimzi-$(RELEASE_VERSION)/docs
	cp -rv documentation/html/index.html strimzi-$(RELEASE_VERSION)/docs/
	cp -rv documentation/html/images/ strimzi-$(RELEASE_VERSION)/docs/images/

docu_clean: docu_htmlclean docu_htmlnoheaderclean

docu_htmlclean:
	rm -rf documentation/html

docu_htmlnoheaderclean:
	rm -rf documentation/htmlnoheader

systemtests:
	./systemtest/scripts/run_tests.sh $(SYSTEMTEST_ARGS)

$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: all $(SUBDIRS) $(DOCKER_TARGETS) systemtests
