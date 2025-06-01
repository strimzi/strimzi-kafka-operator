# OlmSingleNamespaceST

**Description:** Tests Strimzi deployments managed by OLM when configured to watch a single, specific namespace.

**Labels:**

* [olm](labels/olm.md)

<hr style="border:1px solid">

## testDeployExampleKafka

**Description:** Verifies the deployment of a Kafka cluster using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster using the OLM example in the designated single namespace. | Kafka cluster is deployed and becomes ready within the watched namespace. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)
* [kafka](labels/kafka.md)


## testDeployExampleKafkaBridge

**Description:** Verifies the deployment of a KafkaBridge using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaBridge using the OLM example in the designated single namespace. | KafkaBridge is deployed and becomes ready within the watched namespace. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)
* [bridge](labels/bridge.md)


## testDeployExampleKafkaConnect

**Description:** Verifies the deployment of a KafkaConnect cluster using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaConnect cluster using the OLM example in the designated single namespace. | KafkaConnect cluster is deployed and becomes ready within the watched namespace. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)
* [connect](labels/connect.md)


## testDeployExampleKafkaMirrorMaker2

**Description:** Verifies the deployment of a KafkaMirrorMaker2 cluster using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaMirrorMaker2 cluster using the OLM example in the designated single namespace. | KafkaMirrorMaker2 cluster is deployed and becomes ready within the watched namespace. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)
* `mirror-maker-2` (description file doesn't exist)


## testDeployExampleKafkaRebalance

**Description:** Verifies the deployment of a KafkaRebalance resource using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaRebalance resource using the OLM example in the designated single namespace. | KafkaRebalance resource is deployed and reaches PendingProposal state within the watched namespace. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)
* [cruise-control](labels/cruise-control.md)


## testDeployExampleKafkaTopic

**Description:** Verifies the deployment of a KafkaTopic using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaTopic using the OLM example in the designated single namespace. | KafkaTopic is deployed and becomes ready within the watched namespace. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)
* [kafka](labels/kafka.md)


## testDeployExampleKafkaUser

**Description:** Verifies the deployment of a KafkaUser using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaUser using the OLM example in the designated single namespace. | KafkaUser is deployed and becomes ready within the watched namespace. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)

