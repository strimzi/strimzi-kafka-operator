SUBDIRS=kafka-persisted kafka-inmemory kafka-statefulsets

all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ all
	curl -s https://raw.githubusercontent.com/EnMasseProject/travis-scripts/master/trigger-travis.sh | bash /dev/stdin

.PHONY: all $(SUBDIRS)
