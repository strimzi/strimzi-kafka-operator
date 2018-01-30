RELEASE_VERSION ?= latest

SUBDIRS=docker-images cluster-controller topic-controller resources
#SUBDIRS=docker-images/kafka-base docker-images/zookeeper docker-images/kafka docker-images/kafka-connect docker-images/kafka-connect/s2i cluster-controller topic-controller resources
DOCKER_TARGETS=docker_build docker_push docker_tag

all: $(SUBDIRS)
clean: $(SUBDIRS)
$(DOCKER_TARGETS): $(SUBDIRS)
release: release_prepare $(SUBDIRS) release_version release_pkg

release_prepare:
	rm -rf ./strimzi-$(RELEASE_VERSION)
	rm -f ./strimzi-$(RELEASE_VERSION).tar.gz
	mkdir ./strimzi-$(RELEASE_VERSION)
	cp README.md ./strimzi-$(RELEASE_VERSION)

release_version:
	echo "Changing Docker image tags from :latest to :$(RELEASE_VERSION)"
	find ./strimzi-$(RELEASE_VERSION)/ -name '*.yaml' -type f -exec sed -i '/image: "*strimzi\/[a-zA-Z0-9_-]*:latest"*/s/:latest/:$(RELEASE_VERSION)/g' {} \;
	find ./strimzi-$(RELEASE_VERSION)/ -name '*.yaml' -type f -exec sed -i '/name: [a-zA-Z0-9_-]*IMAGE_TAG/{n;s/latest/$(RELEASE_VERSION)/}' {} \;

release_pkg:
	tar -z -cf ./strimzi-$(RELEASE_VERSION).tar.gz strimzi-$(RELEASE_VERSION)/
	zip -r ./strimzi-$(RELEASE_VERSION).zip strimzi-$(RELEASE_VERSION)/
	rm -rf ./strimzi-$(RELEASE_VERSION)

$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: all $(SUBDIRS) $(DOCKER_TARGETS)
