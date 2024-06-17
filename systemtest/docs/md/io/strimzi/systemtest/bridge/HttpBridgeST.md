# HttpBridgeST

**Description:** Test suite for various Kafka Bridge operations.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize Test Storage and deploy Kafka and Kafka Bridge | Kafka and Kafka Bridge are deployed with necessary configuration |

**Use-cases:**

* `send-simple-message`
* `label-verification`
* `simple-message-receive`
* `kafka-bridge-consumer`
* `update-configuration`
* `service_discovery_verification`
* `annotation-validation`
* `automated_test`
* `bridge-scaling`
* `bridge-stability`
* `scaling`
* `verify-custom-labels`
* `verify-custom-annotations`

**Tags:**

* `regression`
* `bridge`
* `internalclients`

<hr style="border:1px solid">

## testConfigureDeploymentStrategy

**Description:** Test verifies KafkaBridge deployment strategy configuration and label updates with RECREATE and ROLLING_UPDATE strategies.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaBridge resource with deployment strategy RECREATE | KafkaBridge resource is created in RECREATE strategy |
| 2. | Add a label to KafkaBridge resource | KafkaBridge resource is recreated with new label |
| 3. | Check that observed generation is 1 and the label is present | Observed generation is 1 and label 'some=label' is present |
| 4. | Change deployment strategy to ROLLING_UPDATE | Deployment strategy is changed to ROLLING_UPDATE |
| 5. | Add another label to KafkaBridge resource | Pods are rolled with new label |
| 6. | Check that observed generation is 2 and the new label is present | Observed generation is 2 and label 'another=label' is present |

**Use-cases:**

* `configure-deployment-strategy`

**Tags:**

* `regression`
* `bridge`
* `internalclients`


## testCustomAndUpdatedValues

**Description:** Test that validates the creation, update, and verification of a Kafka Bridge with specific initial and updated configurations.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a Kafka Bridge resource with initial configuration | Kafka Bridge is created and deployed with the specified initial configuration |
| 2. | Remove an environment variable that is in use | Environment variable TEST_ENV_1 is removed from the initial configuration |
| 3. | Verify initial probe values and environment variables | The probe values and environment variables match the initial configuration |
| 4. | Update Kafka Bridge resource with new configuration | Kafka Bridge is updated and redeployed with the new configuration |
| 5. | Verify updated probe values and environment variables | The probe values and environment variables match the updated configuration |
| 6. | Verify Kafka Bridge configurations for producer and consumer | Producer and consumer configurations match the updated settings |

**Use-cases:**

* `update_configuration`

**Tags:**

* `regression`
* `bridge`
* `internalclients`


## testCustomBridgeLabelsAreProperlySet

**Description:** Test verifying if custom labels and annotations for Kafka Bridge services are properly set and validated.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize TestStorage | TestStorage instance is created with the current test context |
| 2. | Create Kafka Bridge resource with custom labels and annotations | Kafka Bridge resource is successfully created and available |
| 3. | Retrieve Kafka Bridge service with custom labels | Kafka Bridge service is retrieved with specified custom labels |
| 4. | Filter and validate custom labels and annotations | Custom labels and annotations match the expected values |

**Use-cases:**

* `verify-custom-labels`
* `verify-custom-annotations`

**Tags:**

* `regression`
* `bridge`
* `internalclients`


## testDiscoveryAnnotation

**Description:** Test verifying the presence and correctness of the discovery annotation in the Kafka Bridge service.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Retrieve the Kafka Bridge service using kubeClient | Kafka Bridge service instance is obtained |
| 2. | Extract the discovery annotation from the service metadata | The discovery annotation is retrieved as a string |
| 3. | Convert the discovery annotation to a JsonArray | JsonArray representation of the discovery annotation is created |
| 4. | Validate the content of the JsonArray against expected values | The JsonArray matches the expected service discovery information |

**Use-cases:**

* `service_discovery_verification`
* `annotation_validation`

**Tags:**

* `regression`
* `bridge`
* `internalclients`


## testReceiveSimpleMessage

**Description:** Test verifying that a simple message can be received using Kafka Bridge.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize the test storage | TestStorage instance is initialized |
| 2. | Create Kafka topic resource | Kafka topic resource is created with specified configurations |
| 3. | Setup and deploy Kafka Bridge consumer client | Kafka Bridge consumer client is set up and started receiving messages |
| 4. | Send messages using Kafka producer | Messages are sent to Kafka successfully |
| 5. | Verify message reception | All messages are received by Kafka Bridge consumer client |

**Use-cases:**

* `simple-message-receive`
* `kafka-bridge-consumer`

**Tags:**

* `regression`
* `bridge`
* `internalclients`


## testScaleBridgeSubresource

**Description:** Test checks the scaling of a KafkaBridge subresource and verifies the scaling operation.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize the KafkaBridge resource. | KafkaBridge resource is created in the specified namespace. |
| 2. | Scale the KafkaBridge resource to the desired replicas. | KafkaBridge resource is scaled to the expected number of replicas. |
| 3. | Verify the number of replicas. | The number of replicas is as expected and the observed generation is correct. |
| 4. | Check pod naming conventions. | Pod names should match the naming convention and be consistent. |

**Use-cases:**

* `scaling`

**Tags:**

* `regression`
* `bridge`
* `internalclients`


## testScaleBridgeToZero

**Description:** Test that scales a KafkaBridge instance to zero replicas and verifies that it is properly handled.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a KafkaBridge resource and wait for it to be ready | KafkaBridge resource is created and ready with 1 replica |
| 2. | Fetch the current number of KafkaBridge pods | There should be exactly 1 KafkaBridge pod initially |
| 3. | Scale KafkaBridge to zero replicas | Scaling action is acknowledged |
| 4. | Wait for KafkaBridge to scale down to zero replicas | KafkaBridge scales down to zero replicas correctly |
| 5. | Check the number of KafkaBridge pods after scaling | No KafkaBridge pods should be running |
| 6. | Verify the status of KafkaBridge | KafkaBridge status should indicate it is ready with zero replicas |

**Use-cases:**

* `bridge-scaling`
* `bridge-stability`

**Tags:**

* `regression`
* `bridge`
* `internalclients`


## testSendSimpleMessage

**Description:** Test validating that sending a simple message through Kafka Bridge works correctly and checks labels.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage | Test storage is initialized with necessary context |
| 2. | Create a Kafka Bridge client job | Kafka Bridge client job is configured and instantiated |
| 3. | Create Kafka topic | Kafka topic is successfully created |
| 4. | Start Kafka Bridge producer | Kafka Bridge producer successfully begins sending messages |
| 5. | Wait for producer success | All messages are sent successfully |
| 6. | Start Kafka consumer | Kafka consumer is instantiated and starts consuming messages |
| 7. | Wait for consumer success | All messages are consumed successfully |
| 8. | Verify Kafka Bridge pod labels | Labels for Kafka Bridge pods are correctly set and verified |
| 9. | Verify Kafka Bridge service labels | Labels for Kafka Bridge service are correctly set and verified |

**Use-cases:**

* `send-simple-message`
* `label-verification`

**Tags:**

* `regression`
* `bridge`
* `internalclients`

