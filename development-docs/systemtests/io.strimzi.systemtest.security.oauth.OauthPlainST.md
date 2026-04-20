# OauthPlainST

**Description:** Test suite for verifying OAuth 2.0 authentication using OAUTHBEARER and PLAIN SASL mechanisms over a plain (i.e., non-TLS) listener, including producer, consumer, KafkaConnect, KafkaMirrorMaker2, and KafkaBridge components with OAuth metrics validation.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator, Keycloak, and necessary OAuth secrets. | Cluster Operator and Keycloak are deployed and ready. |
| 2. | Deploy Kafka cluster with custom OAuth authentication listener supporting OAUTHBEARER and PLAIN mechanisms, and OAuth metrics enabled. | Kafka cluster is deployed and ready with OAuth listener configured. |
| 3. | Verify OAuth listener configuration is propagated to Kafka broker logs. | Kafka broker logs contain expected OAuth configuration. |

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testProducerConsumerBridgeWithOauthMetrics

**Description:** Test verifying that KafkaBridge with OAuth authentication can produce messages to a Kafka topic via the bridge HTTP endpoint, and that OAuth metrics are exposed by the KafkaBridge component.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy OAuth producer and consumer and verify message exchange. | Messages are produced and consumed successfully. |
| 2. | Deploy KafkaBridge with custom OAuth OAUTHBEARER authentication over plain listener. | KafkaBridge is deployed and authenticates successfully. |
| 3. | Verify OAuth configuration in KafkaBridge logs. | OAuth configuration is present in the logs. |
| 4. | Produce messages via bridge HTTP endpoint and verify delivery. | Messages are produced via the bridge and delivered successfully. |
| 5. | Collect and verify OAuth metrics from KafkaBridge Pods. | OAuth metrics are present in the collected metrics data. |

**Labels:**

* [security](labels/security.md)


## testProducerConsumerConnectWithOauthMetrics

**Description:** Test verifying that KafkaConnect with OAuth authentication can sink messages from a Kafka topic, and that OAuth metrics are exposed by the KafkaConnect component.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy OAuth producer and consumer and verify message exchange. | Messages are produced and consumed successfully. |
| 2. | Deploy KafkaConnect with custom OAuth OAUTHBEARER authentication over plain listener. | KafkaConnect is deployed and authenticates successfully. |
| 3. | Create a FileSink connector and verify messages are sinked. | Messages appear in the KafkaConnect file sink. |
| 4. | Verify OAuth configuration in KafkaConnect logs. | OAuth configuration is present in the logs. |
| 5. | Collect and verify OAuth metrics from KafkaConnect Pods. | OAuth metrics are present in the collected metrics data. |

**Labels:**

* [security](labels/security.md)


## testProducerConsumerMirrorMaker2WithOauthMetrics

**Description:** Test verifying that KafkaMirrorMaker2 with OAuth authentication can mirror messages between two Kafka clusters, and that OAuth metrics are exposed by the KafkaMirrorMaker2 component.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy OAuth producer and consumer and verify message exchange on source cluster. | Messages are produced and consumed successfully on source cluster. |
| 2. | Deploy target Kafka cluster with custom OAuth authentication. | Target Kafka cluster is deployed and ready. |
| 3. | Deploy KafkaMirrorMaker2 with custom OAuth OAUTHBEARER authentication for both source and target clusters. | KafkaMirrorMaker2 is deployed and authenticates successfully. |
| 4. | Verify OAuth configuration in KafkaMirrorMaker2 logs. | OAuth configuration is present in the logs. |
| 5. | Wait for messages to be mirrored to the target cluster and verify. | Messages are mirrored successfully to the target cluster. |
| 6. | Collect and verify OAuth metrics from KafkaMirrorMaker2 Pods. | OAuth metrics are present in the collected metrics data. |

**Labels:**

* [security](labels/security.md)


## testProducerConsumerWithOauthMetrics

**Description:** Test verifying that an OAuth producer can produce and an OAuth consumer can consume messages from Kafka using OAUTHBEARER mechanism, and that OAuth metrics are exposed by the Kafka brokers.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a topic for the test. | Topic is created. |
| 2. | Deploy OAuth producer and consumer using OAUTHBEARER mechanism over plain listener. | Producer and consumer successfully authenticate and exchange messages. |
| 3. | Collect and verify OAuth metrics from Kafka broker Pods. | OAuth metrics are present in the collected metrics data. |

**Labels:**

* [security](labels/security.md)


## testSaslPlainProducerConsumer

**Description:** Test verifying that an OAuth producer and consumer can authenticate using SASL PLAIN mechanism backed by OAuth token endpoint.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy OAuth producer using SASL PLAIN mechanism with audience credentials. | Producer successfully authenticates and sends messages. |
| 2. | Deploy OAuth consumer using SASL PLAIN mechanism with audience credentials. | Consumer successfully authenticates and receives messages. |

**Labels:**

* [security](labels/security.md)

