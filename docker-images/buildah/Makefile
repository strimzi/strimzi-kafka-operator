PROJECT_NAME := buildah
BUILDAH = quay.io/containers/buildah:v1.41.4

docker_build:
	# The Buildah image is used to build new Kafka Connect images with additional connectors.
    # We do not build it from scratch. Instead, we pull the official Buildah image,
    # retag it under our namespace for consistent versioning, and store it alongside
    # our other images.
	$(DOCKER_CMD) pull $(DOCKER_PLATFORM) $(BUILDAH)
	$(DOCKER_CMD) tag $(BUILDAH) strimzi/$(PROJECT_NAME):latest
	$(DOCKER_CMD) tag $(BUILDAH) strimzi/$(PROJECT_NAME):$(BUILD_TAG)$(DOCKER_PLATFORM_TAG_SUFFIX)

include ../../Makefile.docker

.PHONY: build clean release
