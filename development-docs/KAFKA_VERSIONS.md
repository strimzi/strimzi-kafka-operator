# Adding / removing Kafka versions

## Adding new Kafka versions

There is no _definitive_ guide as every version might bring surprises.
But these are some of the tasks you usually have to do.

* If you add a new minor version, add new 3rd party libs directory to `docker-images/artifacts/kafka-thirdparty-libs`
  * This directory is typically shared by all patch releases which are part of the same minor release. So it is named `<MAJOR>.<MINOR>.x`- e.g. `1.1.x`
  * Add a `pom.xml` file with dependencies and make sure they are aligned with the dependencies inside Kafka (e.g. use similar version of Jackson libraries)
  * Typically, you would do this by copying the directory for the previous minor version
* Add the new version to `kafka-versions.yaml` file
  * If the version you are adding is the latest, mark it as `default: true` and mark the previous one as `default: false`
* If the version you are adding is the latest
  * Update the Kafka versions and protocol versions in the documentation `attributes.adoc`
  * If needed, update the examples (only in the `packaging/examples` directory):
    * The `version` and the `inter.broker.protocol.version` fields
    * The Docker image tag in the Kafka connect Build example
  * Update the main `pom.xml` to use the latest Kafka version in the operator
  * Update `systemtest/src/test/resources/upgrade/BundleUpgrade.yaml` with the new version
  * Update protocol versions in `cluster-operator/src/test/java/io/strimzi/operator/cluster/model/KafkaConfigurationTests.java` if needed
* Make sure the `cluster-operator/src/test/java/io/strimzi/operator/cluster/KafkaVersionTestUtils.java` is up-to-date
* If you are adding a release candidate which is not yet published to Maven central, make sure to update the `ST_FILE_PLUGIN_URL_DEFAULT` field in `systemtest/src/main/java/io/strimzi/systemtest/Environment.java` to get the system tests to pass.
* Run `make all` to update all the installation files, generated docs files, Helm chart etc.
  You will need to commit also all the files it updates before opening a PR.
* Add a `CHANGELOG.md` record
* Run unit tests
* Run system tests (typically as part of the PR on Azure Pipelines)

## Removing Kafka version

* Edit the `kafka-versions.yaml` file and mark the version as unsupported (do not remove it, just set the `supported` flag to `false`)
* Run `make all` to update all the installation files, generated docs files, Helm chart etc.
  You will need to commit also all the files it updates before opening a PR.
* Make sure the version is not referenced anywhere anymore (apart from the obvious places such as CHANGELOG.md)
* Add a `CHANGELOG.md` record
* Run unit tests
* Run system tests