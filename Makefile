SUBDIRS=kafka-base zookeeper kafka-statefulsets kafka-connect kafka-connect/s2i
DOCKER_TARGETS=docker_build docker_push docker_tag

all: $(SUBDIRS)
$(DOCKER_TARGETS): $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: all $(SUBDIRS)
