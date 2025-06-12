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
| 1. | Deploy Kafka CR using the OLM example in the designated single namespace. | Kafka CR is created in Kubernetes within the watched namespace.. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)


## testDeployExampleKafkaBridge

**Description:** Verifies the deployment of a KafkaBridge using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaBridge CR using the OLM example in the designated single namespace. | KafkaBridge CR is created in Kubernetes within the watched namespace.. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)


## testDeployExampleKafkaConnect

**Description:** Verifies the deployment of a KafkaConnect cluster using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaConnect CR using the OLM example in the designated single namespace. | KafkaConnect CR is created in Kubernetes within the watched namespace.. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)


## testDeployExampleKafkaMirrorMaker2

**Description:** Verifies the deployment of a KafkaMirrorMaker2 cluster using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaMirrorMaker2 CR using the OLM example in the designated single namespace. | KafkaMirrorMaker2 CR is created in Kubernetes within the watched namespace. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)


## testDeployExampleKafkaRebalance

**Description:** Verifies the deployment of a KafkaRebalance resource using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaRebalance CR using the OLM example in the designated single namespace. | KafkaRebalance CR is created in Kubernetes and reaches PendingProposal state within the watched namespace. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)


## testDeployExampleKafkaTopic

**Description:** Verifies the deployment of a KafkaTopic using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy KafkaTopic CR using the OLM example in the designated single namespace. | KafkaTopic CR is created in Kubernetes within the watched namespace. |
| 2. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)


## testDeployExampleKafkaUser

**Description:** Verifies the deployment of a KafkaUser using the OLM example in a single-namespace watch configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka CR with simple authorization. | Kafka CR is created with simple authz. |
| 2. | Deploy KafkaUser using the OLM example in the designated single namespace. | KafkaUser is deployed and becomes ready within the watched namespace. |
| 3. | Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace. | The resource is operational and managed by the operator within its watched namespace. |

**Labels:**

* [olm](labels/olm.md)

