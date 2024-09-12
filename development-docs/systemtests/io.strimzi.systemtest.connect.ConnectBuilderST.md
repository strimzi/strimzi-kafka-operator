# ConnectBuilderST

**Description:** Testing Kafka Connect build and plugin management.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize TestStorage and perform the setup | TestStorage is initialized and setup is completed with Kafka cluster resources in place |

**Labels:**

* [connect](labels/connect.md)

<hr style="border:1px solid">

## testBuildFailsWithWrongChecksumOfArtifact

**Description:** Test that ensures Kafka Connect build fails with wrong artifact checksum and recovers with correct checksum.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize TestStorage and get test image name | TestStorage instance is created and the image name for the test case is retrieved |
| 2. | Create a Plugin with wrong checksum and build Kafka Connect resource with it | Kafka Connect resource is created but the build fails due to wrong checksum |
| 3. | Wait for Kafka Connect status to indicate build failure | Kafka Connect status contains message about build failure |
| 4. | Deploy network policies for Kafka Connect | Network policies are successfully deployed for Kafka Connect |
| 5. | Replace the plugin checksum with the correct one and update Kafka Connect resource | Kafka Connect resource is updated with the correct checksum |
| 6. | Wait for Kafka Connect to be ready | Kafka Connect becomes ready |
| 7. | Verify that EchoSink KafkaConnector is available in Kafka Connect API | EchoSink KafkaConnector is returned by Kafka Connect API |
| 8. | Verify that EchoSink KafkaConnector is listed in Kafka Connect resource status | EchoSink KafkaConnector is listed in the status of Kafka Connect resource |

**Labels:**

* [connect](labels/connect.md)


## testBuildOtherPluginTypeWithAndWithoutFileName

**Description:** Test verifying Kafka Connect plugin behavior with and without file names for different plugin types.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage and create namespace and topic | Namespace and topic are created successfully |
| 2. | Create and set up Kafka Connect with specified plugin and build configurations | Kafka Connect is deployed and configured correctly |
| 3. | Take a snapshot of current Kafka Connect pods and verify plugin file name | Plugin file name matches the expected file name |
| 4. | Modify Kafka Connect to use a plugin without a file name and trigger a rolling update | Kafka Connect plugin is updated without the file name successfully |
| 5. | Verify plugin file name after update using the plugin's hash | Plugin file name is different from the previous name and matches the hash |

**Labels:**

* [connect](labels/connect.md)


## testBuildPluginUsingMavenCoordinatesArtifacts

**Description:** Test building a plugin using Maven coordinates artifacts.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a test storage object | Test storage object is created |
| 2. | Generate image name for the test case | Image name is generated successfully |
| 3. | Create Kafka topic and Kafka Connect resources | Resources are created and available |
| 4. | Configure Kafka Connector and deploy it | Connector is deployed with correct configuration |
| 5. | Create Kafka consumer and start consuming messages | Consumer starts consuming messages successfully |
| 6. | Verify that consumer receives messages | Consumer receives the expected messages |

**Labels:**

* [connect](labels/connect.md)


## testBuildWithJarTgzAndZip

**Description:** Test for building Kafka Connect image with combined jar, tar.gz, and zip plugins, and validating message send-receive functionality.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create TestStorage object | TestStorage instance is created with context |
| 2. | Get image name for test case | Image name is successfully retrieved |
| 3. | Create Kafka Topic resources | Kafka Topic resources are created with wait |
| 4. | Create Kafka Connect resources | Kafka Connect resources are created with wait |
| 5. | Configure Kafka Connector | Kafka Connector is configured and created with wait |
| 6. | Verify Kafka Connector class name | Connector class name matches expected ECHO_SINK_CLASS_NAME |
| 7. | Create Kafka Clients and send messages | Kafka Clients created and messages sent and verified |
| 8. | Check logs for received message | Logs contain the expected received message |

**Labels:**

* [connect](labels/connect.md)


## testPushIntoImageStream

**Description:** Test verifying the successful push of a KafkaConnect build into an OpenShift ImageStream.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage | Test storage is initialized with the test context |
| 2. | Create ImageStream | ImageStream is created in the specified namespace |
| 3. | Deploy KafkaConnect with the image stream output | KafkaConnect is deployed with the expected build configuration |
| 4. | Verify KafkaConnect build artifacts and status | KafkaConnect has two plugins, uses the image stream output and is in the 'Ready' state |

**Labels:**

* [connect](labels/connect.md)


## testUpdateConnectWithAnotherPlugin

**Description:** Test updating and validating Kafka Connect with another plugin.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create TestStorage instance | Instance of TestStorage is created |
| 2. | Generate random topic name and create Kafka topic | Kafka topic is successfully created |
| 3. | Deploy Kafka Connect and Scraper pod with specific configurations | Kafka Connect and Scraper pod are successfully deployed |
| 4. | Deploy network policies for KafkaConnect | Network policies are successfully deployed |
| 5. | Create and validate EchoSink KafkaConnector | EchoSink KafkaConnector is successfully created and validated |
| 6. | Add a second plugin to Kafka Connect and perform rolling update | Second plugin is added and rolling update is performed |
| 7. | Create and validate Camel-HTTP-Sink KafkaConnector | Camel-HTTP-Sink KafkaConnector is successfully created and validated |
| 8. | Verify that both connectors and plugins are present in Kafka Connect | Both connectors and plugins are verified successfully |

**Labels:**

* [connect](labels/connect.md)

