PROJECT_NAME := buildah
BUILDAH = quay.io/containers/buildah:v1.41.4

docker_build:
	# The Buildah image used for building new Kafka Connect images with additional connectors is not build from
	# scratch. We just pull the one released by Buildah, retag it and push it to our repository to have our versioning on
	# it and have it stored there in our other images.
	$(DOCKER_CMD) pull $(DOCKER_PLATFORM) $(BUILDAH)
	$(DOCKER_CMD) tag $(BUILDAH) strimzi/$(PROJECT_NAME):latest
	$(DOCKER_CMD) tag $(BUILDAH) strimzi/$(PROJECT_NAME):$(BUILD_TAG)$(DOCKER_PLATFORM_TAG_SUFFIX)

include ../../Makefile.docker

.PHONY: build clean release
