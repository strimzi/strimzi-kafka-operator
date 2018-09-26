SUBDIRS=java-base kafka-base zookeeper kafka kafka-connect kafka-connect/s2i \
        stunnel-base zookeeper-stunnel kafka-stunnel entity-operator-stunnel \
        test-client kafka-mirror-maker
DOCKER_TARGETS=docker_build docker_push docker_tag

all: $(SUBDIRS)
clean: $(SUBDIRS)
$(DOCKER_TARGETS): $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: build clean release all $(SUBDIRS) $(DOCKER_TARGETS)
