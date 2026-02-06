# HttpBridgeServerTlsST

**Description:** Test suite for verifying TLS support for HTTP Bridge server.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize test storage and context. | Test storage and context are initialized successfully. |
| 2. | Create Kafka user to generate HTTP Bridge server certificate and key | Kafka user for generating HTTP Bridge server certificate and key is created. |
| 3. | Deploy Kafka and KafkaBridge configured HTTP Bridge server certificate and key. | Kafka and KafkaBridge configured with HTTP Bridge server certificate and key are deployed and running. |
| 4. | Create BridgeClients instance. | BridgeClients instance is created. |

**Labels:**

* [bridge](labels/bridge.md)

<hr style="border:1px solid">

## testReceiveSimpleMessageTls

**Description:** Test to verify that a simple message can be received using TLS in a parallel environment.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize TestStorage and BridgeClients. | TestStorage and BridgeClients are initialized. |
| 2. | Create Kafka topic with provided configurations. | Kafka topic resource is created and available. |
| 3. | Create Kafka Bridge client job with TLS configuration for consuming messages. | Kafka Bridge client with TLS configuration is created and started consuming messages. |
| 4. | Create Kafka client for message production. | Kafka client is configured and initialized. |
| 5. | Verify that producer successfully sent messages. | Kafka producer client starts successfully and begins sending messages. |
| 6. | Verify message consumption. | Messages are successfully consumed by the Kafka Bridge consumer. |

**Labels:**

* [bridge](labels/bridge.md)


## testSendSimpleMessageTls

**Description:** Test to verify that sending a simple message using TLS works correctly.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initialize TestStorage and BridgeClients. | TestStorage and BridgeClients are initialized. |
| 2. | Create Kafka topic using resource manager. | Kafka topic is successfully created. |
| 3. | Create Kafka Bridge Client job with TLS configuration for producing messages. | Kafka Bridge Client job with TLS configuration is created and produces messages successfully. |
| 4. | Verify that the producer successfully sends messages. | Producer successfully sends the expected number of messages. |
| 5. | Create Kafka client for message consumption | Kafka client consumer is created. |
| 6. | Verify that the consumer successfully receives messages. | Consumer successfully receives the expected number of messages. |

**Labels:**

* [bridge](labels/bridge.md)

