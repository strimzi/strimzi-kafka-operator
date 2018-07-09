TOPDIR=$(dir $(lastword $(MAKEFILE_LIST)))
RELEASE_VERSION ?= latest

SUBDIRS=docker-images certificate-manager api cluster-operator topic-operator init-kafka examples
DOCKER_TARGETS=docker_build docker_push docker_tag

all: $(SUBDIRS)
clean: $(SUBDIRS) docu_clean
$(DOCKER_TARGETS): $(SUBDIRS)
release: release_prepare release_version release_maven $(SUBDIRS) release_docu release_pkg

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

docu_html: docu_htmlclean
	mkdir -p documentation/html
	asciidoctor -t -dbook -a ProductVersion=$(RELEASE_VERSION) documentation/book/master.adoc -o documentation/html/index.html
	cp -vrL documentation/book/images documentation/html/images

docu_htmlnoheader: docu_htmlnoheaderclean
	mkdir -p documentation/htmlnoheader
	asciidoctor -t -dbook -a ProductVersion=$(RELEASE_VERSION) -s documentation/book/master.adoc -o documentation/htmlnoheader/master.html
	cp -vrL documentation/book/images documentation/htmlnoheader/images

docu_pushtowebsite: docu_htmlnoheader
	./.travis/docu-push-to-website.sh

release_docu: docu_html docu_htmlnoheader
	mkdir -p strimzi-$(RELEASE_VERSION)/docs
	cp -rv documentation/html/ strimzi-$(RELEASE_VERSION)/docs/

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
