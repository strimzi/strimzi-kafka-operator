# HttpBridgeST

**Description:** Test suite for various HTTP Bridge operations.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize Test Storage and deploy Kafka and HTTP Bridge. | Kafka and HTTP Bridge are deployed with necessary configuration. |

**Labels:**

* [bridge](labels/bridge.md)

<hr style="border:1px solid">

## testConfigureDeploymentStrategy

**Description:** Test verifies HTTP Bridge deployment strategy configuration and label updates with RECREATE and ROLLING_UPDATE strategies.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create HTTP Bridge resource with deployment strategy RECREATE | KafkaBridge resource is created in RECREATE strategy |
| 2. | Add a label to HTTP Bridge resource | HTTP Bridge resource is recreated with new label |
| 3. | Check that observed generation is 1 and the label is present | Observed generation is 1 and label 'some=label' is present |
| 4. | Change deployment strategy to ROLLING_UPDATE | Deployment strategy is changed to ROLLING_UPDATE |
| 5. | Add another label to HTTP Bridge resource | Pods are rolled with new label |
| 6. | Check that observed generation is 2 and the new label is present | Observed generation is 2 and label 'another=label' is present |

**Labels:**

* [bridge](labels/bridge.md)


## testCustomAndUpdatedValues

**Description:** Test that validates the creation, update, and verification of a HTTP Bridge with specific initial and updated configurations.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a KafkaBridge resource with initial configuration. | HTTP Bridge is created and deployed with the specified initial configuration. |
| 2. | Remove an environment variable that is in use. | Environment variable TEST_ENV_1 is removed from the initial configuration. |
| 3. | Verify initial probe values and environment variables. | The probe values and environment variables match the initial configuration. |
| 4. | Update KafkaBridge resource with new configuration. | HTTP Bridge is updated and redeployed with the new configuration. |
| 5. | Verify updated probe values and environment variables. | The probe values and environment variables match the updated configuration. |
| 6. | Verify HTTP Bridge configurations for producer and consumer. | Producer and consumer configurations match the updated settings. |

**Labels:**

* [bridge](labels/bridge.md)


## testCustomBridgeLabelsAreProperlySet

**Description:** Test verifying if custom labels and annotations for HTTP Bridge services are properly set and validated.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize TestStorage | TestStorage instance is created with the current test context |
| 2. | Create HTTP Bridge resource with custom labels and annotations | HTTP Bridge resource is successfully created and available |
| 3. | Retrieve HTTP Bridge service with custom labels | HTTP Bridge service is retrieved with specified custom labels |
| 4. | Filter and validate custom labels and annotations | Custom labels and annotations match the expected values |

**Labels:**

* `label` (description file doesn't exist)
* `annotation` (description file doesn't exist)


## testDiscoveryAnnotation

**Description:** Test verifying the presence and correctness of the discovery annotation in the HTTP Bridge service.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Retrieve the HTTP Bridge service using kubeClient. | HTTP Bridge service instance is obtained. |
| 2. | Extract the discovery annotation from the service metadata. | The discovery annotation is retrieved as a string. |
| 3. | Convert the discovery annotation to a JsonArray. | JsonArray representation of the discovery annotation is created. |
| 4. | Validate the content of the JsonArray against expected values. | The JsonArray matches the expected service discovery information. |

**Labels:**

* `service_discovery_verification` (description file doesn't exist)
* `annotation_validation` (description file doesn't exist)


## testReceiveSimpleMessage

**Description:** Test verifying that a simple message can be received using HTTP Bridge.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize the test storage. | TestStorage instance is initialized. |
| 2. | Create Kafka topic resource. | Kafka topic resource is created with specified configurations. |
| 3. | Setup and deploy HTTP Bridge consumer client. | HTTP Bridge consumer client is set up and started receiving messages. |
| 4. | Send messages using Kafka producer. | Messages are sent to Kafka successfully. |
| 5. | Verify message reception. | All messages are received by HTTP Bridge consumer client. |

**Labels:**

* [bridge](labels/bridge.md)


## testScaleBridgeSubresource

**Description:** Test checks the scaling of a HTTP Bridge subresource and verifies the scaling operation.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize the HTTP Bridge resource. | KafkaBridge resource is created in the specified namespace. |
| 2. | Scale the HTTP Bridge resource to the desired replicas. | HTTP Bridge resource is scaled to the expected number of replicas. |
| 3. | Verify the number of replicas. | The number of replicas is as expected and the observed generation is correct. |
| 4. | Check pod naming conventions. | Pod names should match the naming convention and be consistent. |

**Labels:**

* [bridge](labels/bridge.md)


## testScaleBridgeToZero

**Description:** Test that scales a HTTP Bridge instance to zero replicas and verifies that it is properly handled.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a KafkaBridge resource and wait for it to be ready. | KafkaBridge resource is created and ready with 1 replica. |
| 2. | Fetch the current number of HTTP Bridge pods. | There should be exactly 1 HTTP Bridge pod initially. |
| 3. | Scale HTTP Bridge to zero replicas. | Scaling action is acknowledged. |
| 4. | Wait for HTTP Bridge to scale down to zero replicas. | HTTP Bridge scales down to zero replicas correctly. |
| 5. | Check the number of HTTP Bridge pods after scaling | No HTTP Bridge pods should be running |
| 6. | Verify the status of HTTP Bridge | HTTP Bridge status should indicate it is ready with zero replicas |

**Labels:**

* [bridge](labels/bridge.md)


## testSendSimpleMessage

**Description:** Test validating that sending a simple message through HTTP Bridge works correctly and checks labels.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage. | Test storage is initialized with necessary context. |
| 2. | Create a HTTP Bridge client job. | HTTP Bridge client job is configured and instantiated. |
| 3. | Create Kafka topic. | Kafka topic is successfully created. |
| 4. | Start HTTP Bridge producer. | HTTP Bridge producer successfully begins sending messages. |
| 5. | Wait for producer success. | All messages are sent successfully. |
| 6. | Start Kafka consumer. | Kafka consumer is instantiated and starts consuming messages. |
| 7. | Wait for consumer success. | All messages are consumed successfully. |
| 8. | Verify HTTP Bridge pod labels. | Labels for HTTP Bridge pods are correctly set and verified. |
| 9. | Verify HTTP Bridge service labels. | Labels for HTTP Bridge service are correctly set and verified. |

**Labels:**

* [bridge](labels/bridge.md)

