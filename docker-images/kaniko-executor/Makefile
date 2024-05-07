PROJECT_NAME := kaniko-executor
KANIKO_EXECUTOR = gcr.io/kaniko-project/executor:v1.22.0

docker_build:
	# The Kaniko executor image used for building new Kafka Connect images with additional connectors is not build from
	# scratch. We just pull the one released by Kaniko, retag it and push it to our repository to have our versioning on
	# it and have it stored there in our other images.
	$(DOCKER_CMD) pull $(DOCKER_PLATFORM) $(KANIKO_EXECUTOR)
	$(DOCKER_CMD) tag $(KANIKO_EXECUTOR) strimzi/$(PROJECT_NAME):latest
	$(DOCKER_CMD) tag $(KANIKO_EXECUTOR) strimzi/$(PROJECT_NAME):$(BUILD_TAG)$(DOCKER_PLATFORM_TAG_SUFFIX)

include ../../Makefile.docker

.PHONY: build clean release
