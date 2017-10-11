SUBDIRS=kafka-base zookeeper kafka-statefulsets kafka-connect

all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ all

.PHONY: all $(SUBDIRS)
