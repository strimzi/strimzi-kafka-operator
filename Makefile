SUBDIRS=kafka-persisted kafka-inmemory kafka-statefulsets

all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ all

.PHONY: all $(SUBDIRS)
