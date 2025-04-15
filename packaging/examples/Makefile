PROJECT_NAME=examples

include ../../Makefile.os

RELEASE_VERSION ?= latest
RELEASE_PATH ?= ../../strimzi-$(RELEASE_VERSION)/$(PROJECT_NAME)
EXAMPLE_DASHBOARDS ?= metrics/grafana-dashboards/
HELM_CHART_DASHBOARD_PATH ?= ../helm-charts/helm3/strimzi-kafka-operator/files/grafana-dashboards/

ifneq ($(RELEASE_VERSION),latest)
  DOC_TAG = $(RELEASE_VERSION)
else
  DOC_TAG = in-development
endif

release:
	$(SED) -Ei -e "s#strimzi.io/docs/operators/[^/]+/configuring.html#strimzi.io/docs/operators/$(DOC_TAG)/configuring.html#" security/keycloak-authorization/README.md
	mkdir -p $(RELEASE_PATH)
	$(CP) -r $$(ls | $(GREP) -Ev '^Makefile$$') $(RELEASE_PATH)/

dashboard_install:
	mkdir -p $(HELM_CHART_DASHBOARD_PATH)
	rm -f $(HELM_CHART_DASHBOARD_PATH)/*.json
	cp $(EXAMPLE_DASHBOARDS)/*.json $(HELM_CHART_DASHBOARD_PATH)


.PHONY: all build clean java_install java_build docker_build docker_push docker_tag spotbugs
