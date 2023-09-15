TOPDIR=$(dir $(lastword $(MAKEFILE_LIST)))

DOCKERDIRS=base operator kafka-based maven-builder kaniko-executor
DOCKER_TARGETS=docker_build docker_push docker_tag docker_load docker_save docker_amend_manifest docker_push_manifest docker_delete_manifest docker_delete_archive docker_sbom docker_push_sbom

all: $(DOCKERDIRS)
$(DOCKER_TARGETS): $(DOCKERDIRS)

$(DOCKERDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: all $(DOCKERDIRS) $(DOCKER_TARGETS)