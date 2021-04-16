PROJECT_NAME=examples

include ../../Makefile.os

RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../../strimzi-$(RELEASE_VERSION)/$(PROJECT_NAME)

release:
	$(SED) -Ei -e "s#strimzi.io/docs/operators/latest/using.html#strimzi.io/docs/operators/$(RELEASE_VERSION)/using.html#" security/keycloak-authorization/README.md
	mkdir -p $(RELEASE_PATH)
	$(CP) -r $$(ls | $(GREP) -Ev '^Makefile$$') $(RELEASE_PATH)/

.PHONY: all build clean docker_build docker_push docker_tag spotbugs
