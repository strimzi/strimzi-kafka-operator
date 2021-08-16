PROJECT_NAME := maven-builder
MAVEN_IMAGE = registry.access.redhat.com/ubi8/openjdk-11:1.3-18

docker_build:
	# The Maven image used for building new Kafka Connect images from Maven coordinates.
	# We just pull the one released, retag it and push it to our repository to have our versioning on
	# it and have it stored there in our other images.
	$(DOCKER_CMD) pull $(MAVEN_IMAGE)
	$(DOCKER_CMD) tag $(MAVEN_IMAGE) strimzi/$(PROJECT_NAME):latest
	$(DOCKER_CMD) tag $(MAVEN_IMAGE) strimzi/$(PROJECT_NAME):$(BUILD_TAG)

include ../../Makefile.docker

.PHONY: build clean release
