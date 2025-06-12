# OlmAllNamespaceST

**Description:** Tests Strimzi deployments managed by OLM when configured to watch all namespaces.

**Labels:**

* [olm](labels/olm.md)

<hr style="border:1px solid">

## testDeployExampleKafka

**Description:** Verifies the deployment of a Kafka cluster using the OLM example when the operator watches all namespaces.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a Kafka CR using the OLM example. | Kafka CR is created in Kubernetes. |
| 2. | Wait for readiness of the Kafka cluster, meaning that the operator (watching all namespaces) manages the Kafka cluster in the designated test namespace. | Kafka cluster is operational and managed by the operator. |

**Labels:**

* [olm](labels/olm.md)


## testDeployExampleKafkaBridge

**Description:** Verifies the deployment of a KafkaBridge using the OLM example when the operator watches all namespaces.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaBridge CR using the OLM example. | KafkaBridge CR is created in Kubernetes. |
| 2. | Wait for readiness of the KafkaBridge, meaning that the operator (watching all namespaces) manages the KafkaBridge in the designated test namespace. | KafkaBridge is operational and managed by the operator. |

**Labels:**

* [olm](labels/olm.md)


## testDeployExampleKafkaConnect

**Description:** Verifies the deployment of a KafkaConnect cluster using the OLM example when the operator watches all namespaces.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a KafkaConnect CR using the OLM example. | KafkaConnect CR is created in Kubernetes. |
| 2. | Wait for readiness of the KafkaConnect, meaning that the operator (watching all namespaces) manages the KafkaConnect cluster in the designated test namespace. | KafkaConnect cluster is operational and managed by the operator. |

**Labels:**

* [olm](labels/olm.md)


## testDeployExampleKafkaMirrorMaker2

**Description:** Verifies the deployment of a KafkaMirrorMaker2 cluster using the OLM example when the operator watches all namespaces.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaMirrorMaker2 CR using the OLM example. | KafkaMirrorMaker2 CR is created in Kubernetes. |
| 2. | Wait for readiness of the KafkaMirrorMaker2, meaning that the operator (watching all namespaces) manages the KafkaMirrorMaker2 cluster in the designated test namespace. | KafkaMirrorMaker2 cluster is operational and managed by the operator. |

**Labels:**

* [olm](labels/olm.md)


## testDeployExampleKafkaRebalance

**Description:** Verifies the deployment of a KafkaRebalance resource using the OLM example when the operator watches all namespaces.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaRebalance CR using the OLM example. | KafkaRebalance CR created in Kubernetes and reaches PendingProposal state. |
| 2. | Wait for readiness of the KafkaRebalance, meaning that the operator (watching all namespaces) manages the KafkaRebalance resource in the designated test namespace. | KafkaRebalance resource is operational and managed by the operator. |

**Labels:**

* [olm](labels/olm.md)


## testDeployExampleKafkaTopic

**Description:** Verifies the deployment of a KafkaTopic using the OLM example when the operator watches all namespaces.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaTopic using the OLM example. | KafkaTopic is deployed and becomes ready. |
| 2. | Wait for readiness of the KafkaTopic, meaning that the operator (watching all namespaces) manages the KafkaTopic in the designated test namespace. | KafkaTopic is operational and managed by the operator. |

**Labels:**

* [olm](labels/olm.md)


## testDeployExampleKafkaUser

**Description:** Verifies the deployment of a KafkaUser using the OLM example when the operator watches all namespaces.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with simple authorization. | Kafka cluster with simple authz is deployed and ready. |
| 2. | Deploy KafkaUser using the OLM example. | KafkaUser is deployed and becomes ready. |
| 3. | Wait for readiness of the KafkaUser, meaning that the operator (watching all namespaces) manages the KafkaUser in the designated test namespace. | KafkaUser is operational and managed by the operator. |

**Labels:**

* [olm](labels/olm.md)

