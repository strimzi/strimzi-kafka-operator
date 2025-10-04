# CustomResourceStatusST

**Description:** Test suite containing Custom Resource status verification scenarios, ensuring proper status reporting for Kafka, KafkaConnect, KafkaBridge, KafkaUser, and KafkaMirrorMaker2 resources.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with custom configuration. | Cluster Operator is deployed with operation timeout settings. |
| 2. | Deploy shared Kafka cluster with multiple listeners. | Kafka cluster is deployed and ready with plain, TLS, and external listeners. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testKafkaBridgeStatus

**Description:** This test verifies that KafkaBridge status is correctly reported, including observed generation updates and URL information during ready and not ready states.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaBridge and verify initial status. | KafkaBridge is ready with correct status information including URL. |
| 2. | Modify KafkaBridge resource requests/limits to cause NotReady state. | KafkaBridge becomes NotReady due to insufficient CPU resources. |
| 3. | Restore KafkaBridge resources to recover. | KafkaBridge returns to Ready state with updated observed generation. |

**Labels:**

* [kafka](labels/kafka.md)
* [bridge](labels/bridge.md)


## testKafkaConnectAndConnectorStatus

**Description:** This test verifies that KafkaConnect and KafkaConnector status is correctly reported during various state transitions including Ready, NotReady, and configuration changes.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaConnect with file plugin and KafkaConnector. | KafkaConnect and KafkaConnector are ready with correct status information. |
| 2. | Modify KafkaConnect bootstrap servers to invalid value. | KafkaConnect becomes NotReady due to invalid bootstrap configuration. |
| 3. | Restore valid KafkaConnect bootstrap servers. | KafkaConnect returns to Ready state. |
| 4. | Modify KafkaConnector cluster label to invalid value. | KafkaConnector becomes NotReady and connector status is not reported (i.e., null). |
| 5. | Restore valid KafkaConnector cluster label. | KafkaConnector returns to Ready state. |
| 6. | Modify KafkaConnector class name to invalid value. | KafkaConnector becomes NotReady due to invalid connector class. |
| 7. | Restore valid KafkaConnector configuration. | KafkaConnector returns to Ready state. |

**Labels:**

* [kafka](labels/kafka.md)
* [connect](labels/connect.md)


## testKafkaConnectorWithoutClusterConfig

**Description:** This test verifies that KafkaConnector without proper cluster configuration fails gracefully without causing NullPointerException (NPE) in Cluster Operator logs.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaConnector without cluster configuration in labels. | KafkaConnector is created but becomes NotReady. |
| 2. | Verify connector remains in NotReady state. | KafkaConnector status shows NotReady without causing NPE in Cluster Operator logs. |
| 3. | Delete the invalid KafkaConnector. | KafkaConnector is successfully deleted. |

**Labels:**

* [kafka](labels/kafka.md)
* [connect](labels/connect.md)


## testKafkaMirrorMaker2Status

**Description:** This test verifies that KafkaMirrorMaker2 status is correctly reported, including observed generation updates, URL information, and connector status during Ready and NotReady states.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy source Kafka cluster and KafkaMirrorMaker2. | KafkaMirrorMaker2 is ready with correct status information including URL and connector states. |
| 2. | Modify KafkaMirrorMaker2 resources to cause NotReady state. | KafkaMirrorMaker2 becomes NotReady due to insufficient CPU resources. |
| 3. | Restore KafkaMirrorMaker2 resources to recover. | KafkaMirrorMaker2 returns to Ready state with updated observed generation. |
| 4. | Verify pod stability after recovery. | KafkaMirrorMaker2 pods remain stable, with no unexpected rolling updates. |

**Labels:**

* [kafka](labels/kafka.md)
* [mirror-maker-2](labels/mirror-maker-2.md)


## testKafkaMirrorMaker2WrongBootstrap

**Description:** This test verifies that KafkaMirrorMaker2 with invalid bootstrap server configuration fails gracefully and can be deleted.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaMirrorMaker2 with non-existing bootstrap servers. | KafkaMirrorMaker2 is created but becomes NotReady. |
| 2. | Verify KafkaMirrorMaker2 remains in NotReady state. | KafkaMirrorMaker2 status shows NotReady due to invalid bootstrap configuration. |
| 3. | Delete the KafkaMirrorMaker2 with invalid configuration. | KafkaMirrorMaker2 and its deployment are successfully deleted. |

**Labels:**

* [kafka](labels/kafka.md)
* [mirror-maker-2](labels/mirror-maker-2.md)


## testKafkaStatus

**Description:** This test verifies that Kafka cluster status is correctly reported, including observed generation updates and listener status information. The test also verifies recovery from NotReady state.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Verify Kafka cluster is ready and functioning. | Kafka cluster is ready and can produce/consume messages. |
| 2. | Check initial Kafka status information. | Kafka status shows correct observed generation and listener details. |
| 3. | Modify Kafka resources to cause NotReady state. | Kafka cluster becomes NotReady due to insufficient CPU resources. |
| 4. | Restore Kafka resources to recover. | Kafka cluster returns to Ready state with updated observed generation. |

**Labels:**

* [kafka](labels/kafka.md)


## testKafkaStatusCertificate

**Description:** This test verifies that certificates reported in Kafka status match the certificates stored in the cluster CA secret.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Retrieve certificates from Kafka status. | Certificates are successfully retrieved from Kafka status. |
| 2. | Retrieve certificates from cluster CA secret. | Certificates are successfully retrieved from the cluster CA secret. |
| 3. | Compare status and secret certificates. | Certificates from status and secret are identical. |

**Labels:**

* [kafka](labels/kafka.md)


## testKafkaUserStatus

**Description:** This test verifies that KafkaUser status is correctly reported during creation, authorization changes, and ready state transitions.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaUser with ACL authorization rules. | KafkaUser is created but not ready due to missing topic. |
| 2. | Remove authorization from KafkaUser specification. | KafkaUser becomes ready after authorization removal. |
| 3. | Verify KafkaUser status condition. | KafkaUser status shows Ready condition type. |

**Labels:**

* [kafka](labels/kafka.md)

