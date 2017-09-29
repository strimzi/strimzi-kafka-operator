SUBDIRS=kafka-persisted kafka-inmemory kafka-statefulsets kafka-connect

all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ all

.PHONY: all $(SUBDIRS)
