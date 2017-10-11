SUBDIRS=kafka-base zookeeper kafka-inmemory kafka-statefulsets kafka-connect kafka-connect/s2i

all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ all

.PHONY: all $(SUBDIRS)
