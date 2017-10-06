SUBDIRS=kafka-inmemory kafka-statefulsets kafka-connect/init-loader kafka-connect

all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ all

.PHONY: all $(SUBDIRS)
