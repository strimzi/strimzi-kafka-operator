TOPDIR=$(dir $(lastword $(MAKEFILE_LIST)))

include ./Makefile.os

SHELL = /usr/bin/env bash
GITHUB_VERSION ?= main
OAUTH_VERSION = $(shell source ./tools/strimzi-oauth-version.sh && get_strimzi_oauth_version)
RELEASE_VERSION ?= latest
CHART_SEMANTIC_RELEASE_VERSION ?= $(shell cat ./release.version | tr A-Z a-z)
BRIDGE_VERSION ?= $(shell cat ./bridge.version | tr A-Z a-z)
DOCKER_CMD ?= docker

ifneq ($(RELEASE_VERSION),latest)
  GITHUB_VERSION = $(RELEASE_VERSION)
endif

SUBDIRS=kafka-agent mirror-maker-agent tracing-agent crd-annotations test crd-generator api mockkube certificate-manager operator-common config-model config-model-generator cluster-operator topic-operator user-operator kafka-init systemtest docker-images/artifacts packaging/helm-charts/helm3 packaging/install packaging/examples
DOCKERDIRS=docker-images/base docker-images/operator docker-images/kafka-based docker-images/maven-builder docker-images/kaniko-executor
DOCKER_TARGETS=docker_build docker_push docker_tag docker_load docker_save docker_amend_manifest docker_push_manifest docker_sign_manifest docker_delete_manifest docker_delete_archive docker_sbom docker_push_sbom
JAVA_TARGETS=java_build java_install java_clean

all: prerequisites_check $(SUBDIRS) $(DOCKERDIRS) crd_install dashboard_install helm_install shellcheck docu_versions docu_check
clean: prerequisites_check $(SUBDIRS) $(DOCKERDIRS) docu_clean
$(DOCKER_TARGETS): prerequisites_check $(DOCKERDIRS)
$(JAVA_TARGETS): prerequisites_check $(SUBDIRS)
release: release_prepare release_version release_helm_version release_maven $(SUBDIRS) release_docu release_single_file release_pkg docu_clean

next_version:
	echo $(shell echo $(NEXT_VERSION) | tr a-z A-Z) > release.version
	mvn versions:set -DnewVersion=$(shell echo $(NEXT_VERSION) | tr a-z A-Z)
	mvn versions:commit

bridge_version:
	# Set Kafka Bridge version to its own version
	$(FIND) ./packaging/install -name '*.yaml' -type f -exec $(SED) -i '/value: "\?quay.io\/strimzi\/kafka-bridge:[a-zA-Z0-9_.-]\+"\?/s/quay.io\/strimzi\/kafka-bridge:[a-zA-Z0-9_.-]\+/quay.io\/strimzi\/kafka-bridge:$(BRIDGE_VERSION)/g' {} \;
	CHART_PATH=./packaging/helm-charts/helm3/strimzi-kafka-operator; \
	$(SED) -i '/name: kafka-bridge/{n;s/\(tag: \).*/\1$(BRIDGE_VERSION)/g}' $$CHART_PATH/values.yaml; \
	$(SED) -i 's/\(kafkaBridge.image\.tag[^\n]*| \)`.*`/\1`$(BRIDGE_VERSION)`/g' $$CHART_PATH/README.md

release_prepare:
	echo $(shell echo $(RELEASE_VERSION) | tr a-z A-Z) > release.version
	rm -rf ./strimzi-$(RELEASE_VERSION)
	rm -f ./strimzi-$(RELEASE_VERSION).tar.gz
	rm -f ./strimzi-$(RELEASE_VERSION).zip
	rm -f ./strimzi-kafka-operator-helm-3-chart-$(RELEASE_VERSION).tgz
	rm -f ./strimzi-topic-operator-$(RELEASE_VERSION).yaml
	rm -f ./strimzi-cluster-operator-$(RELEASE_VERSION).yaml
	rm -f ./strimzi-user-operator-$(RELEASE_VERSION).yaml
	mkdir ./strimzi-$(RELEASE_VERSION)
	$(CP) CHANGELOG.md ./strimzi-$(RELEASE_VERSION)

release_version:
	# TODO: This would be replaced ideally once Helm Chart templating is used for cluster and topic operator examples
	echo "Changing Docker image tags in install to :$(RELEASE_VERSION)"
	$(FIND) ./packaging/install -name '*.yaml' -type f -exec $(SED) -i '/image: "\?quay.io\/strimzi\/operator:[a-zA-Z0-9_.-]\+"\?/s/:[a-zA-Z0-9_.-]\+/:$(RELEASE_VERSION)/g' {} \;
	$(FIND) ./packaging/install -name '*.yaml' -type f -exec $(SED) -i '/value: "\?quay.io\/strimzi\/operator:[a-zA-Z0-9_.-]\+"\?/s/quay.io\/strimzi\/operator:[a-zA-Z0-9_.-]\+/quay.io\/strimzi\/operator:$(RELEASE_VERSION)/g' {} \;
	$(FIND) ./packaging/install -name '*.yaml' -type f -exec $(SED) -i '/value: "\?quay.io\/strimzi\/kafka-bridge:[a-zA-Z0-9_.-]\+"\?/s/quay.io\/strimzi\/kafka-bridge:[a-zA-Z0-9_.-]\+/quay.io\/strimzi\/kafka-bridge:$(BRIDGE_VERSION)/g' {} \;
	$(FIND) ./packaging/install -name '*.yaml' -type f -exec $(SED) -i '/value: "\?quay.io\/strimzi\/kafka:[a-zA-Z0-9_.-]\+"\?/s/quay.io\/strimzi\/kafka:[a-zA-Z0-9_.-]\+-kafka-\([0-9.]\+\)/quay.io\/strimzi\/kafka:$(RELEASE_VERSION)-kafka-\1/g' {} \;
	$(FIND) ./packaging/install -name '*.yaml' -type f -exec $(SED) -i '/[0-9.]\+=quay.io\/strimzi\/kafka[a-zA-Z0-9_.-]\?\+:[a-zA-Z0-9_.-]\+-kafka-[0-9.]\+"\?/s/:[a-zA-Z0-9_.-]\+-kafka-\([0-9.]\+\)/:$(RELEASE_VERSION)-kafka-\1/g' {} \;
	$(FIND) ./packaging/install -name '*.yaml' -type f -exec $(SED) -i '/value: "\?quay.io\/strimzi\/kaniko-executor:[a-zA-Z0-9_.-]\+"\?/s/quay.io\/strimzi\/kaniko-executor:[a-zA-Z0-9_.-]\+/quay.io\/strimzi\/kaniko-executor:$(RELEASE_VERSION)/g' {} \;
	$(FIND) ./packaging/install -name '*.yaml' -type f -exec $(SED) -i '/value: "\?quay.io\/strimzi\/maven-builder:[a-zA-Z0-9_.-]\+"\?/s/quay.io\/strimzi\/maven-builder:[a-zA-Z0-9_.-]\+/quay.io\/strimzi\/maven-builder:$(RELEASE_VERSION)/g' {} \;
	# Set Kafka Bridge version to its own version
	$(FIND) ./packaging/install -name '*.yaml' -type f -exec $(SED) -i '/value: "\?quay.io\/strimzi\/kafka-bridge:[a-zA-Z0-9_.-]\+"\?/s/quay.io\/strimzi\/kafka-bridge:[a-zA-Z0-9_.-]\+/quay.io\/strimzi\/kafka-bridge:$(BRIDGE_VERSION)/g' {} \;

release_maven:
	echo "Update pom versions to $(RELEASE_VERSION)"
	mvn versions:set -DnewVersion=$(shell echo $(RELEASE_VERSION) | tr a-z A-Z)
	mvn versions:commit

release_pkg: helm_pkg
	tar -z -cf ./strimzi-$(RELEASE_VERSION).tar.gz strimzi-$(RELEASE_VERSION)/
	zip -r ./strimzi-$(RELEASE_VERSION).zip strimzi-$(RELEASE_VERSION)/
	rm -rf ./strimzi-$(RELEASE_VERSION)
	$(FIND) ./examples/ -mindepth 1 -maxdepth 1 ! -name DO_NOT_EDIT.md -type f,d -exec rm -rvf {} +
	$(FIND) ./install/ -mindepth 1 -maxdepth 1 ! -name DO_NOT_EDIT.md -type f,d -exec rm -rvf {} +
	rm -rfv ./helm-charts/helm3/strimzi-kafka-operator
	$(FIND) ./packaging/examples/ -mindepth 1 -maxdepth 1 ! -name Makefile -type f,d -exec $(CP) -rv {} ./examples/ \;
	$(FIND) ./packaging/install/ -mindepth 1 -maxdepth 1 ! -name Makefile -type f,d -exec $(CP) -rv {} ./install/ \;
	$(CP) -rv ./packaging/helm-charts/helm3/strimzi-kafka-operator ./helm-charts/helm3/strimzi-kafka-operator

release_helm_version:
	echo "Updating default image tags in Helm Chart to $(RELEASE_VERSION)"
	CHART_PATH=./packaging/helm-charts/helm3/strimzi-kafka-operator; \
	$(SED) -i 's/\(defaultImageTag: \).*/\1$(RELEASE_VERSION)/g' $$CHART_PATH/values.yaml; \
	$(SED) -i 's/\(defaultImageTag[^\n]*| \)`.*`/\1`$(RELEASE_VERSION)`/g' $$CHART_PATH/README.md; \
	$(SED) -i '/name: kafka-bridge/{n;s/\(tag: \).*/\1$(BRIDGE_VERSION)/g}' $$CHART_PATH/values.yaml; \
	$(SED) -i 's/\(kafkaBridge.image\.tag[^\n]*| \)`.*`/\1`$(BRIDGE_VERSION)`/g' $$CHART_PATH/README.md

release_single_file:
	$(FIND) ./strimzi-$(RELEASE_VERSION)/install/cluster-operator/ -type f -exec cat {} \; -exec printf "\n---\n" \; > strimzi-cluster-operator-$(RELEASE_VERSION).yaml
	$(FIND) ./strimzi-$(RELEASE_VERSION)/install/topic-operator/ -type f -exec cat {} \; -exec printf "\n---\n" \; > strimzi-topic-operator-$(RELEASE_VERSION).yaml
	$(FIND) ./strimzi-$(RELEASE_VERSION)/install/user-operator/ -type f -exec cat {} \; -exec printf "\n---\n" \; > strimzi-user-operator-$(RELEASE_VERSION).yaml
	$(FIND) ./strimzi-$(RELEASE_VERSION)/install/cluster-operator/*-Crd-*.yaml -type f -exec cat {} \; -exec printf "\n---\n" \; > strimzi-crds-$(RELEASE_VERSION).yaml

helm_pkg: dashboard_install
	# Copying unarchived Helm Chart to release directory
	mkdir -p strimzi-$(RELEASE_VERSION)/helm3-charts/
	helm package --version $(CHART_SEMANTIC_RELEASE_VERSION) --app-version $(CHART_SEMANTIC_RELEASE_VERSION) --destination ./ ./packaging/helm-charts/helm3/strimzi-kafka-operator/
	$(CP) strimzi-kafka-operator-$(CHART_SEMANTIC_RELEASE_VERSION).tgz strimzi-kafka-operator-helm-3-chart-$(CHART_SEMANTIC_RELEASE_VERSION).tgz
	rm -rf strimzi-$(RELEASE_VERSION)/helm3-charts/
	rm strimzi-kafka-operator-$(CHART_SEMANTIC_RELEASE_VERSION).tgz

docu_versions:
	documentation/snip-kafka-versions.sh > documentation/modules/snip-kafka-versions.adoc
	documentation/version-dependent-attrs.sh > documentation/shared/version-dependent-attrs.adoc
	documentation/snip-images.sh > documentation/modules/snip-images.adoc

docu_html: docu_htmlclean docu_versions docu_check
	mkdir -p documentation/html
	$(CP) -vrL documentation/shared/images documentation/html/images
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a BridgeVersion=$(BRIDGE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -a OAuthVersion=$(OAUTH_VERSION) documentation/deploying/deploying.adoc -o documentation/html/deploying.html
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a BridgeVersion=$(BRIDGE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -a OAuthVersion=$(OAUTH_VERSION) documentation/configuring/configuring.adoc -o documentation/html/configuring.html
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a BridgeVersion=$(BRIDGE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -a OAuthVersion=$(OAUTH_VERSION) documentation/overview/overview.adoc -o documentation/html/overview.html
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a BridgeVersion=$(BRIDGE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -a OAuthVersion=$(OAUTH_VERSION) documentation/contributing/contributing.adoc -o documentation/html/contributing.html

docu_htmlnoheader: docu_htmlnoheaderclean docu_versions docu_check
	mkdir -p documentation/htmlnoheader
	$(CP) -vrL documentation/shared/images documentation/htmlnoheader/images
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a BridgeVersion=$(BRIDGE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -a OAuthVersion=$(OAUTH_VERSION) -s documentation/deploying/deploying.adoc -o documentation/htmlnoheader/deploying-book.html
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a BridgeVersion=$(BRIDGE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -a OAuthVersion=$(OAUTH_VERSION) -s documentation/configuring/configuring.adoc -o documentation/htmlnoheader/configuring-book.html
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a BridgeVersion=$(BRIDGE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -a OAuthVersion=$(OAUTH_VERSION) -s documentation/overview/overview.adoc -o documentation/htmlnoheader/overview-book.html
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a BridgeVersion=$(BRIDGE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -a OAuthVersion=$(OAUTH_VERSION) -s documentation/contributing/contributing.adoc -o documentation/htmlnoheader/contributing-book.html

docu_pdf: docu_pdfclean docu_versions docu_check
	mkdir -p documentation/pdf
	asciidoctor-pdf -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a BridgeVersion=$(BRIDGE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -a OAuthVersion=$(OAUTH_VERSION) documentation/deploying/deploying.adoc -o documentation/pdf/deploying.pdf
	asciidoctor-pdf -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a BridgeVersion=$(BRIDGE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -a OAuthVersion=$(OAUTH_VERSION) documentation/configuring/configuring.adoc -o documentation/pdf/configuring.pdf
	asciidoctor-pdf -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a BridgeVersion=$(BRIDGE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -a OAuthVersion=$(OAUTH_VERSION) documentation/overview/overview.adoc -o documentation/pdf/overview.pdf
	asciidoctor-pdf -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a BridgeVersion=$(BRIDGE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -a OAuthVersion=$(OAUTH_VERSION) documentation/contributing/contributing.adoc -o documentation/pdf/contributing.pdf

docu_check:
	./.azure/scripts/check_docs.sh

shellcheck:
	./.azure/scripts/shellcheck.sh

release_files_check:
	./.azure/scripts/release_files_check.sh

spotbugs: $(SUBDIRS)

docu_pushtowebsite:
	./.azure/scripts/docu-push-to-website.sh

pushtonexus:
	./.azure/scripts/push-to-nexus.sh

release_docu: docu_html docu_htmlnoheader docu_pdf
	mkdir -p strimzi-$(RELEASE_VERSION)/docs/html
	mkdir -p strimzi-$(RELEASE_VERSION)/docs/pdf
	$(CP) -rv documentation/pdf/overview.pdf strimzi-$(RELEASE_VERSION)/docs/pdf/
	$(CP) -rv documentation/pdf/deploying.pdf strimzi-$(RELEASE_VERSION)/docs/pdf/
	$(CP) -rv documentation/pdf/configuring.pdf strimzi-$(RELEASE_VERSION)/docs/pdf/
	$(CP) -rv documentation/html/overview.html strimzi-$(RELEASE_VERSION)/docs/html/
	$(CP) -rv documentation/html/deploying.html strimzi-$(RELEASE_VERSION)/docs/html/
	$(CP) -rv documentation/html/configuring.html strimzi-$(RELEASE_VERSION)/docs/html/
	$(CP) -rv documentation/html/images/ strimzi-$(RELEASE_VERSION)/docs/html/images/

docu_clean: docu_htmlclean docu_htmlnoheaderclean docu_pdfclean

docu_htmlclean:
	rm -rf documentation/html

docu_htmlnoheaderclean:
	rm -rf documentation/htmlnoheader

docu_pdfclean:
	rm -rf documentation/pdf

helm_install:
	$(MAKE) -C packaging/helm-charts/helm3 helm_install

crd_install:
	$(MAKE) -C packaging/install crd_install

dashboard_install:
	$(MAKE) -C packaging/examples dashboard_install

$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

$(DOCKERDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

prerequisites_check:
	SED=$(SED) ./tools/prerequisites-check.sh

checksum_examples:
	@$(FIND) ./examples/ -type f -print0 | LC_ALL=C $(SORT) -z | $(XARGS) -0 $(SHA1SUM) | $(SHA1SUM)

checksum_install:
	@$(FIND) ./install/ -type f -print0 | LC_ALL=C $(SORT) -z | $(XARGS) -0 $(SHA1SUM) | $(SHA1SUM)

checksum_helm:
	@$(FIND) ./helm-charts/ -type f -print0 | LC_ALL=C $(SORT) -z | $(XARGS) -0 $(SHA1SUM) | $(SHA1SUM)

.PHONY: all $(SUBDIRS) $(DOCKERDIRS) $(DOCKER_TARGETS) docu_versions spotbugs docu_check prerequisites_check
