TOPDIR = $(dir $(lastword $(MAKEFILE_LIST)))
JAR_VERSION = $(shell cat ../release.version)

generate-docs:
	mvn clean install -DskipTests -Pcopy-deps -f ../systemtest/pom.xml
	java -classpath ../systemtest/target/lib/\*:../systemtest/target/test-classes:../systemtest/target/systemtest-$(JAR_VERSION).jar io.strimzi.systemtestdoc.DocGenerator --filePath ../systemtest/src/test/java/io/strimzi/systemtest --generatePath ./documentation/

clean-docs:
	rm -rf documentation/io