SUBDIRS=kafka-base zookeeper kafka-statefulsets kafka-connect kafka-connect/s2i

all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ all

.PHONY: all $(SUBDIRS)
