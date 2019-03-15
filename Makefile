TOPDIR=$(dir $(lastword $(MAKEFILE_LIST)))

include ./Makefile.os

GITHUB_VERSION ?= master
RELEASE_VERSION ?= latest
CHART_PATH ?= ./helm-charts/strimzi-kafka-operator/
CHART_SEMANTIC_RELEASE_VERSION ?= $(shell cat ./release.version | tr A-Z a-z)

ifneq ($(RELEASE_VERSION),latest)
  GITHUB_VERSION = $(RELEASE_VERSION)
endif

SUBDIRS=mockkube test crd-generator api certificate-manager operator-common cluster-operator topic-operator user-operator kafka-init docker-images helm-charts install examples metrics
DOCKER_TARGETS=docker_build docker_push docker_tag

all: $(SUBDIRS)
clean: $(SUBDIRS) docu_clean
$(DOCKER_TARGETS): helm_install $(SUBDIRS)
release: release_prepare release_version release_helm_version release_maven $(SUBDIRS) release_docu release_single_file release_pkg release_helm_repo docu_clean

next_version:
	echo $(shell echo $(NEXT_VERSION) | tr a-z A-Z) > release.version
	mvn versions:set -DnewVersion=$(shell echo $(NEXT_VERSION) | tr a-z A-Z)
	mvn versions:commit
	# Update OLM
	$(SED) -i 's/currentCSV: strimzi-cluster-operator.v.*\+/currentCSV: strimzi-cluster-operator.v$(NEXT_VERSION)/g' ./olm/strimzi-kafka-operator.package.yaml
	$(SED) -i 's/name: strimzi-cluster-operator.v.*/name: strimzi-cluster-operator.v$(NEXT_VERSION)/g' ./olm/strimzi-cluster-operator.clusterserviceversion.yaml
	$(SED) -i 's/version: [0-9]\+\.[0-9]\+\.[0-9]\+[a-zA-Z0-9_-]*.*/version: $(NEXT_VERSION)/g' ./olm/strimzi-cluster-operator.clusterserviceversion.yaml

release_prepare:
	echo $(shell echo $(RELEASE_VERSION) | tr a-z A-Z) > release.version
	rm -rf ./strimzi-$(RELEASE_VERSION)
	rm -f ./strimzi-$(RELEASE_VERSION).tar.gz
	mkdir ./strimzi-$(RELEASE_VERSION)

release_version:
	# TODO: This would be replaced ideally once Helm Chart templating is used for cluster and topic operator examples
	echo "Changing Docker image tags in install to :$(RELEASE_VERSION)"
	$(FIND) ./install -name '*.yaml' -type f -exec $(SED) -i '/image: "\?strimzi\/[a-zA-Z0-9_.-]\+:[a-zA-Z0-9_.-]\+"\?/s/:[a-zA-Z0-9_.-]\+/:$(RELEASE_VERSION)/g' {} \;
	$(FIND) ./install -name '*.yaml' -type f -exec $(SED) -i '/name: [a-zA-Z0-9_-]*IMAGE_TAG/{n;s/value: [a-zA-Z0-9_.-]\+/value: $(RELEASE_VERSION)/}' {} \;
	# Zookeeper needs to have special handling, because it has strange tag but no image map
	# The firs line below handles the Zoo tag
	# The one below it handles all tags whcih are not Zookeeper
	$(FIND) ./install -name '*.yaml' -type f -exec $(SED) -i '/name: STRIMZI_DEFAULT_[a-zA-Z0-9_-]*IMAGE/{n;s/:[a-zA-Z0-9_.-]\+-kafka-\([0-9.]\+\)/:$(RELEASE_VERSION)-kafka-\1/}' {} \;
	$(FIND) ./install -name '*.yaml' -type f -exec $(SED) -i '/name: STRIMZI_DEFAULT_ZOOKEEPER_[a-zA-Z0-9_-]*IMAGE/b; /name: STRIMZI_DEFAULT_[a-zA-Z0-9_-]*IMAGE/{n;s/:[a-zA-Z0-9_.-]\+/:$(RELEASE_VERSION)/}' {} \;
	$(FIND) ./install -name '*.yaml' -type f -exec $(SED) -i '/[0-9.]\+=strimzi\/kafka[a-zA-Z0-9_.-]\?\+:[a-zA-Z0-9_.-]\+-kafka-[0-9.]\+"\?/s/:[a-zA-Z0-9_.-]\+-kafka-\([0-9.]\+\)/:$(RELEASE_VERSION)-kafka-\1/g' {} \;
	echo "Changing Docker image tags in olm to :$(RELEASE_VERSION)"
	$(FIND) ./olm -name '*.yaml' -type f -exec $(SED) -i '/image: "\?strimzi\/[a-zA-Z0-9_.-]\+:[a-zA-Z0-9_.-]\+"\?/s/:[a-zA-Z0-9_.-]\+/:$(RELEASE_VERSION)/g' {} \;
	$(FIND) ./olm -name '*.yaml' -type f -exec $(SED) -i '/name: [a-zA-Z0-9_-]*IMAGE_TAG/{n;s/value: [a-zA-Z0-9_.-]\+/value: $(RELEASE_VERSION)/}' {} \;
	# Zookeeper needs to have special handling, because it has strange tag but no image map
	# The firs line below handles the Zoo tag
	# The one below it handles all tags whcih are not Zookeeper
	$(FIND) ./olm -name '*.yaml' -type f -exec $(SED) -i '/name: STRIMZI_DEFAULT_[a-zA-Z0-9_-]*IMAGE/{n;s/:[a-zA-Z0-9_.-]\+-kafka-\([0-9.]\+\)/:$(RELEASE_VERSION)-kafka-\1/}' {} \;
	$(FIND) ./olm -name '*.yaml' -type f -exec $(SED) -i '/name: STRIMZI_DEFAULT_ZOOKEEPER_[a-zA-Z0-9_-]*IMAGE/b; /name: STRIMZI_DEFAULT_[a-zA-Z0-9_-]*IMAGE/{n;s/:[a-zA-Z0-9_.-]\+/:$(RELEASE_VERSION)/}' {} \;
	$(FIND) ./olm -name '*.yaml' -type f -exec $(SED) -i '/[0-9.]\+=strimzi\/kafka[a-zA-Z0-9_.-]\?\+:[a-zA-Z0-9_.-]\+-kafka-[0-9.]\+"\?/s/:[a-zA-Z0-9_.-]\+-kafka-\([0-9.]\+\)/:$(RELEASE_VERSION)-kafka-\1/g' {} \;
	$(SED) -i 's/currentCSV: strimzi-cluster-operator.v.*\+/currentCSV: strimzi-cluster-operator.v$(RELEASE_VERSION)/g' ./olm/strimzi-kafka-operator.package.yaml
	$(SED) -i 's/name: strimzi-cluster-operator.v.*/name: strimzi-cluster-operator.v$(RELEASE_VERSION)/g' ./olm/strimzi-cluster-operator.clusterserviceversion.yaml
	$(SED) -i 's/version: [0-9]\+\.[0-9]\+\.[0-9]\+[a-zA-Z0-9_-]*.*/version: $(RELEASE_VERSION)/g' ./olm/strimzi-cluster-operator.clusterserviceversion.yaml
	$(SED) -i 's/containerImage: docker.io\/strimzi\/cluster-operator:.*/containerImage: docker.io\/strimzi\/cluster-operator:$(RELEASE_VERSION)/g' ./olm/strimzi-cluster-operator.clusterserviceversion.yaml

release_maven:
	echo "Update pom versions to $(RELEASE_VERSION)"
	mvn versions:set -DnewVersion=$(shell echo $(RELEASE_VERSION) | tr a-z A-Z)
	mvn versions:commit

release_pkg: helm_pkg	
	tar -z -cf ./strimzi-$(RELEASE_VERSION).tar.gz strimzi-$(RELEASE_VERSION)/
	zip -r ./strimzi-$(RELEASE_VERSION).zip strimzi-$(RELEASE_VERSION)/
	rm -rf ./strimzi-$(RELEASE_VERSION)

release_helm_version:
	echo "Updating default image tags in Helm Chart to $(RELEASE_VERSION)"
	# Update default image tag in chart values.yaml to RELEASE_VERSION
	$(SED) -i 's/\(tag: \).*/\1$(RELEASE_VERSION)/g' $(CHART_PATH)values.yaml
	# Update default image tagPrefix in chart values.yaml to RELEASE_VERSION
	$(SED) -i 's/\(tagPrefix: \).*/\1$(RELEASE_VERSION)/g' $(CHART_PATH)values.yaml
	# Update default image tag in chart README.md config grid with RELEASE_VERSION
	$(SED) -i 's/\(image\.tag[^\n]*| \)`.*`/\1`$(RELEASE_VERSION)`/g' $(CHART_PATH)README.md
	# Update default image tag in chart README.md config grid with RELEASE_VERSION
	$(SED) -i 's/\(image\.tagPrefix[^\n]*| \)`.*`/\1`$(RELEASE_VERSION)`/g' $(CHART_PATH)README.md

release_helm_repo:
	echo "Updating Helm Repository index.yaml"
	helm repo index ./ --url https://github.com/strimzi/strimzi-kafka-operator/releases/download/$(RELEASE_VERSION)/ --merge ./helm-charts/index.yaml
	mv ./index.yaml ./helm-charts/index.yaml

release_single_file:
	$(FIND) ./strimzi-$(RELEASE_VERSION)/install/cluster-operator/ -type f -exec cat {} \; -exec printf "\n---\n" \; > strimzi-cluster-operator-$(RELEASE_VERSION).yaml
	$(FIND) ./strimzi-$(RELEASE_VERSION)/install/topic-operator/ -type f -exec cat {} \; -exec printf "\n---\n" \; > strimzi-topic-operator-$(RELEASE_VERSION).yaml
	$(FIND) ./strimzi-$(RELEASE_VERSION)/install/user-operator/ -type f -exec cat {} \; -exec printf "\n---\n" \; > strimzi-user-operator-$(RELEASE_VERSION).yaml

helm_pkg:
	# Copying unarchived Helm Chart to release directory
	mkdir -p strimzi-$(RELEASE_VERSION)/charts/
	$(CP) -r $(CHART_PATH) strimzi-$(RELEASE_VERSION)/charts/$(CHART_NAME)
	# Packaging helm chart with semantic version: $(CHART_SEMANTIC_RELEASE_VERSION)
	helm package --version $(CHART_SEMANTIC_RELEASE_VERSION) --app-version $(CHART_SEMANTIC_RELEASE_VERSION) --destination ./ $(CHART_PATH)
	mv strimzi-kafka-operator-$(CHART_SEMANTIC_RELEASE_VERSION).tgz strimzi-kafka-operator-helm-chart-$(CHART_SEMANTIC_RELEASE_VERSION).tgz
	rm -rf strimzi-$(RELEASE_VERSION)/charts/

docu_versions:
	documentation/snip-kafka-versions.sh kafka-versions > documentation/book/snip-kafka-versions.adoc
	documentation/version-dependent-attrs.sh kafka-versions > documentation/book/common/version-dependent-attrs.adoc

docu_html: docu_htmlclean docu_check docu_versions
	mkdir -p documentation/html
	$(CP) -vrL documentation/book/images documentation/html/images
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a GithubVersion=$(GITHUB_VERSION) documentation/book/master.adoc -o documentation/html/index.html
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a GithubVersion=$(GITHUB_VERSION) documentation/contributing/master.adoc -o documentation/html/contributing.html


docu_htmlnoheader: docu_htmlnoheaderclean docu_check docu_versions
	mkdir -p documentation/htmlnoheader
	$(CP) -vrL documentation/book/images documentation/htmlnoheader/images
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -s documentation/book/master.adoc -o documentation/htmlnoheader/master.html
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -a GithubVersion=$(GITHUB_VERSION) -s documentation/contributing/master.adoc -o documentation/htmlnoheader/contributing.html

docu_check:
	./.travis/check_docs.sh

findbugs: $(SUBDIRS)

docu_pushtowebsite: docu_htmlnoheader docu_html
	./.travis/docu-push-to-website.sh

pushtonexus:
	./.travis/push-to-nexus.sh

release_docu: docu_html docu_htmlnoheader
	mkdir -p strimzi-$(RELEASE_VERSION)/docs
	$(CP) -rv documentation/html/index.html strimzi-$(RELEASE_VERSION)/docs/
	$(CP) -rv documentation/html/images/ strimzi-$(RELEASE_VERSION)/docs/images/

docu_clean: docu_htmlclean docu_htmlnoheaderclean

docu_htmlclean:
	rm -rf documentation/html

docu_htmlnoheaderclean:
	rm -rf documentation/htmlnoheader

systemtests:
	./systemtest/scripts/run_tests.sh $(SYSTEMTEST_ARGS)

helm_install: helm-charts

$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: all $(SUBDIRS) $(DOCKER_TARGETS) systemtests docu_versions findbugs docu_check
