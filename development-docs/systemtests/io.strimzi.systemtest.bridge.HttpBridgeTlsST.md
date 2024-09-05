# HttpBridgeTlsST

**Description:** Test suite for verifying TLS functionalities in the HTTP Bridge.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage and context | Test storage and context are initialized successfully |
| 2. | Deploy Kafka and KafkaBridge | Kafka and KafkaBridge are deployed and running |
| 3. | Create Kafka user with TLS configuration | Kafka user with TLS configuration is created |
| 4. | Deploy HTTP bridge with TLS configuration | HTTP bridge is deployed with TLS configuration |

**Labels:**

* [bridge](labels/bridge.md)

<hr style="border:1px solid">

## testReceiveSimpleMessageTls

**Description:** Test to verify that a simple message can be received using TLS in a parallel environment.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize the test storage instance | TestStorage object is instantiated with the test context. |
| 2. | Configure Kafka Bridge client for consumption | Kafka Bridge client is configured with topic and consumer names. |
| 3. | Create Kafka topic with provided configurations | Kafka topic resource is created and available. |
| 4. | Deploy the Kafka Bridge consumer | Kafka Bridge consumer starts successfully and is ready to consume messages. |
| 5. | Initialize TLS Kafka client for message production | TLS Kafka client is configured and initialized. |
| 6. | Deploy the Kafka producer TLS client | TLS Kafka producer client starts successfully and begins sending messages. |
| 7. | Verify message consumption | Messages are successfully consumed by the Kafka Bridge consumer. |

**Labels:**

* [bridge](labels/bridge.md)


## testSendSimpleMessageTls

**Description:** Test to verify that sending a simple message using TLS works correctly.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize TestStorage and BridgeClients with TLS configuration | TestStorage and BridgeClients are initialized with TLS configuration |
| 2. | Create Kafka topic using resource manager | Kafka topic is successfully created |
| 3. | Create Kafka Bridge Client job for producing messages | Kafka Bridge Client job is created and produces messages successfully |
| 4. | Verify that the producer successfully sends messages | Producer successfully sends the expected number of messages |
| 5. | Create Kafka client consumer with TLS configuration | Kafka client consumer is created with TLS configuration |
| 6. | Verify that the consumer successfully receives messages | Consumer successfully receives the expected number of messages |

**Labels:**

* [bridge](labels/bridge.md)

