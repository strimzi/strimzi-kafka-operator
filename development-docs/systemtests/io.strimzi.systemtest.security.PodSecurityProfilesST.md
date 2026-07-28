# PodSecurityProfilesST

**Description:** Test suite for verifying Pod Security profiles with Strimzi operands, ensuring containers have properly set security contexts when using the restricted Pod Security provider class.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Cluster Operator with STRIMZI_POD_SECURITY_PROVIDER_CLASS set to restricted. | Cluster Operator is deployed with the restricted Pod Security provider. |

**Labels:**

* [security](labels/security.md)

<hr style="border:1px solid">

## testOperandsWithRestrictedSecurityProfile

**Description:** Test verifying that all Strimzi operands function correctly under the Kubernetes restricted Pod Security profile, including proper security context generation for all containers and successful message exchange across Kafka, KafkaConnect, KafkaBridge, and KafkaMirrorMaker2 components.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Label the test namespace with pod-security.kubernetes.io/enforce: restricted. | Namespace is labeled with restricted Pod Security profile. |
| 2. | Deploy two Kafka clusters: one source and one target for MirrorMaker2. | Kafka clusters are deployed. |
| 3. | Deploy KafkaConnect with file plugin, KafkaBridge, KafkaMirrorMaker2, and a FileSink KafkaConnector. | All additional operands are deployed. |
| 4. | Produce and consume messages in the source Kafka cluster using clients with restricted security profile applied. | Messages are produced and consumed successfully. |
| 5. | Verify that all Pod containers for Kafka, KafkaConnect, KafkaBridge, and KafkaMirrorMaker2 have proper security context settings. | All containers have expected security context properties. |
| 6. | Verify KafkaConnect FileSink connector received messages. | Messages are present in the sink file. |
| 7. | Verify KafkaMirrorMaker2 mirrored messages to the target cluster. | Messages are present in the target cluster. |
| 8. | Deploy a producer without restricted security profile and verify it fails. | Producer fails due to Pod Security profile violation. |

**Labels:**

* [security](labels/security.md)

