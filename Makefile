RELEASE_VERSION ?= latest

SUBDIRS=kafka-base zookeeper kafka-statefulsets kafka-inmemory kafka-connect kafka-connect/s2i cluster-controller
DOCKER_TARGETS=docker_build docker_push docker_tag

all: $(SUBDIRS)
clean: $(SUBDIRS)
$(DOCKER_TARGETS): $(SUBDIRS)
release: release_prepare $(SUBDIRS) release_version release_pkg

release_prepare:
	rm -rf ./barnabas-$(RELEASE_VERSION)
	rm -f ./barnabas-$(RELEASE_VERSION).tar.gz
	mkdir ./barnabas-$(RELEASE_VERSION)
	cp README.md ./barnabas-$(RELEASE_VERSION)

release_version:
	echo "Changing Docker image tags from :latest to :$(RELEASE_VERSION)"
	find ./barnabas-$(RELEASE_VERSION)/ -name '*.yaml' -type f -exec sed -i '/image: "*enmasseproject\/[a-zA-Z0-9_-]*:latest"*/s/:latest/:$(RELEASE_VERSION)/g' {} \;
	find ./barnabas-$(RELEASE_VERSION)/ -name '*.yaml' -type f -exec sed -i '/name: [a-zA-Z0-9_-]*IMAGE_TAG/{n;s/latest/$(RELEASE_VERSION)/}' {} \;

release_pkg:
	tar -z -cf ./barnabas-$(RELEASE_VERSION).tar.gz barnabas-$(RELEASE_VERSION)/
	rm -rf ./barnabas-$(RELEASE_VERSION)

$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: all $(SUBDIRS) $(DOCKER_TARGETS)
