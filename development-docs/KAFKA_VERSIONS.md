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
    * The `.spec.kafka.version` and the `spec.kafka.metadataVersion` fields within the `Kafka` custom resource
    * The `.spec.version` field within the `KafkaConnect` and `KafkaMirrorMaker2` custom resources
    * The Docker image tag in the Kafka connect Build example
  * Update the main `pom.xml` to use the latest Kafka version in the operator
  * Update `systemtest/src/test/resources/upgrade/BundleUpgrade.yaml` and `systemtest/src/test/resources/upgrade/BundleDowngrade.yaml` with the new version
  * Update protocol versions in `cluster-operator/src/test/java/io/strimzi/operator/cluster/model/KafkaConfigurationTests.java` if needed
* Make sure the `cluster-operator/src/test/java/io/strimzi/operator/cluster/KafkaVersionTestUtils.java` is up-to-date
* If you are adding a release candidate which is not yet published to Maven central, make sure to update the `ST_FILE_PLUGIN_URL_DEFAULT` field in `systemtest/src/main/java/io/strimzi/systemtest/Environment.java` to get the system tests to pass.
* Run `make all` to update all the installation files, generated docs files, Helm chart etc.
  You will need to commit also all the files it updates before opening a PR.
* Add a `CHANGELOG.md` record
* Run unit tests
* Run system tests (typically as part of the PR on GitHub Actions)

You can also run unit and systems tests in an early stage on an Apache Kafka RC to validate it and report issues to the community before the GA.
In order to so so, please follow the above steps but also add the Apache staging repository to the `pom.xml` file as follow:

```xml
<repository>
    <!--
    We use the Kafka staging repository while we use the RC releases of Kafka 4.1.0.
    This should be removed once we move to the final release of Kafka 4.1.0.
    -->
    <id>apache-staging</id>
    <url>https://repository.apache.org/content/groups/staging/</url>
</repository>
```

Remember to remove the staging repository when the Apache Kafka release is GA.

## Removing Kafka version

* Edit the `kafka-versions.yaml` file and mark the version as unsupported (do not remove it, just set the `supported` flag to `false`)
* Delete the corresponding thirdparty libs folder under `docker-images/artifacts/kafka-thirdparty-libs` (for example, `3.9.x`)
* Run `make all` to update all the installation files, generated docs files, Helm chart etc.
  You will need to commit also all the files it updates before opening a PR.
* Make sure the version is not referenced anywhere anymore (apart from the obvious places such as CHANGELOG.md)
* Add a `CHANGELOG.md` record
* Run unit tests
* Run system tests

## Using custom Kafka versions

Since Strimzi 1.1.0, users can add custom Apache Kafka versions by adding them to the `kafka-versions.yaml` file and building the Strimzi project.
The custom version configuration should look similar to this:

```yaml
- version: "4.3.0-my-patch-1"
  maven-version: 4.3.0-my-build-00001
  metadata: 4.3-IV0
  url: https://pub-d8e7da268f054c92b640faf3f30587ce.r2.dev/kafka_2.13-4.3.0-jakub-00001.tgz
  checksum: 98DDDC6248E0CAED4A6426D57DE0351A33CABB8431DE1463859847D2FC0C66B91FB4687D939F62060A2CFC620F45AB868A6E7C5C3E41DD39F290B6EBA8B43E95
  third-party-libs: 4.3.x
  supported: true
  default: true
```

* The `version` field is the version which will be used in the `Kafka`, `KafkaConnect` and `KafkaMirrorMaker2` custom resources.
  The `version` should always start with the Kafka version it is based on and can use an additional suffix to distinguish it from the original version (e.g. `4.3.0-my-patch-1`).
* The `maven-version` field can be used to specify the version of the Apache Kafka Maven artifacts to use in the build.
  When not set, it is assumed that the Maven artifacts use the same version as the `version` field.
  If you want to use the Maven artifacts from the official Apache Kafka release, you can just set it to its version (e.g. `4.3.0`).
  If you use your own version that is not available in the Maven Central repository, you should also add your repository to the main `pom.xml` file:
  ```xml
  <repository>
      <id>my-repo</id>
      <url>https://my.repo.url/</url>
  </repository>
  ```
* The `metadata` field is the version which will be used as the default Kafka protocol metadata version.
* The `url` and `checksum` fields should point to the URL of the Kafka binary distribution and its SHA512 checksum.
* The `third-party-libs` field should point to the directory with the 3rd party libraries which should be used for this version.
  You can either use one of the directories from an existing version, or create your own by copying an existing one and updating the dependencies in its `pom.xml` file.
* The `supported` field should be set to `true` if you want to use this version.
* The `default` field should be set to `true` if you want this version to be the default one (i.e. the one used when the user does not specify any version in the custom resource).
  Only one version can be marked as default.

In order to allow smooth upgrades to future Strimzi versions, you should keep this version present in your `kafka-versions.yaml` file and just mark it as unsupported when you want to stop using it.
