PROJECT_NAME=common-test

docker_build: java_install
docker_push:
docker_tag:
all: docker_build docker_push
clean: java_clean

include ../Makefile.maven

#java_build:
#	echo "Building JAR file ..."
#	mvn -q -DtrimStackTrace=false install

#java_clean:
#	echo "Cleaning Maven build ..."
#	mvn clean

.PHONY: build clean release
