PROJECT_NAME := crd-convert
jar_version := $(shell cat ../../release.version)
crd_convert_dist := ../../crd-convert/target/crd-convert-0.20.0-SNAPSHOT.jar

clean: 
	rm -rf .crd-convert.tmp ./tmp

# We unzip each of the -dist.zip files into tmp which is then
# COPYed in the Dockerfile, so can delete tmp on exit
.INTERMEDIATE: tmp

.crd-convert.tmp: $(crd_convert_dist)
	test -d tmp || mkdir tmp
	cp $(crd_convert_dist) ./tmp/
	touch .crd-convert.tmp

docker_build: .crd-convert.tmp docker_build_default
	# Pull dependencies first

include ../../Makefile.docker

.PHONY: build clean release
