# System tests documentation

This module is for storing the documentation of tests located inside the `systemtest` module.

## Annotating the systemtest

To generate documentation of the test, you can use the following annotations:

* `@TestDoc` - is the main annotation, which consists all other annotation and should be used right above the method. 
            It contains three fields - `description`, `steps`, and `usecases`, that are set using other annotations.
* `@Desc` - overall description of the test, it can contain anything
* `@Step` - particular step done in a test, contains two fields - `value` that contains the step information, `expected`
            is for the expected result of the step.
* `@Usecase` - one of the use-cases that the test is testing.

Example of how the systemtest can be annotated:
```java
    @TestDoc(
        description = @Desc("Test checking that we are able to do message exchange without auth"),
        steps = {
            @Step(value = "Deploy Kafka with 3 Kafka and 3 ZK pods and wait for readiness", expected = "Kafka cluster is deployed"),
            @Step(value = "Deploy KafkaTopic inside the created Kafka cluster", expected = "KafkaTopic is deployed"),
            @Step(value = "Using test-clients, do the message exchange", expected = "Messages are successfully sent and received"),
            @Step(value = "Check that Kafka bootstrap service contains expected values", expected = "Kafka bootstrap service contains expected values")
        },
        usecases = {
            @Usecase(id = "message-transmission")
        }
    )
    void testSendMessagesPlainAnonymous(ExtensionContext extensionContext) {
```

## Generating the documentation

The `DocGenerator` needs to have all dependencies built before it is executed.
That can be done using:
```bash
mvn clean install -DskipTests
mvn clean install -DskipTests -pl systemtest -Pcopy-deps
```
from the root directory of the project.

After that, the generator can be executed using the following command:
```bash
java -classpath ../systemtest/target/lib/\*:../systemtest/target/test-classes:../systemtest/target/systemtest-<RELEASE-VERSION>.jar \
io.strimzi.systemtestdoc.DocGenerator --filePath ../systemtest/src/test/java/io/strimzi/systemtest --generatePath ./documentation/
```

The generator accepts two arguments:

* `--filePath` - path to the Strimzi systemtest classes, from where all the names of the STs are taken
* `--generatePath` - path to the place where the documentation should be generated

Or you can use the `make` command from the repository root: 
```bash
make generate-docs -C systemtest-doc
```
To remove the generated documentation, you can use:
```bash
make clean-docs -C systemtest-doc
```