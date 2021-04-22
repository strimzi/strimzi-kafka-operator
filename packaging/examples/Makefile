PROJECT_NAME=examples

include ../../Makefile.os

RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../../strimzi-$(RELEASE_VERSION)/$(PROJECT_NAME)

ifneq ($(RELEASE_VERSION),latest)
  DOC_TAG = $(RELEASE_VERSION)
else
  DOC_TAG = in-development
endif

release:
	$(SED) -Ei -e "s#strimzi.io/docs/operators/[^/]+/using.html#strimzi.io/docs/operators/$(DOC_TAG)/using.html#" security/keycloak-authorization/README.md
	mkdir -p $(RELEASE_PATH)
	$(CP) -r $$(ls | $(GREP) -Ev '^Makefile$$') $(RELEASE_PATH)/

.PHONY: all build clean docker_build docker_push docker_tag spotbugs
