PROJECT_NAME=test-client

clean:
	rm -rf lib
	rm -rf tmp
	rm .*.tmp

.test-client.tmp: ../../test-client/target/test-client*.jar
	test -d tmp || mkdir tmp
	cp -f ../../test-client/target/test-client*.jar -d tmp
	touch .test-client.tmp

docker_build: .test-client.tmp

include ../../Makefile.docker

.PHONY: build clean release
