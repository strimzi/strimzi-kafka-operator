# RecoveryST

**Description:** Test suite for verifying Cluster Operator's ability to recover Kafka cluster components from various deletion and failure scenarios.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy shared Kafka cluster and KafkaBridge. | Kafka cluster with 3 replicas and KafkaBridge is deployed. |

<hr style="border:1px solid">

## testRecoveryFromImpossibleMemoryRequest

**Description:** Test verifies that Kafka cluster can recover from impossible resource requests by correcting the resource configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Update broker node pool with impossibly large memory request (465458732Gi). | Node pool is updated. |
| 2. | Wait for Kafka pods to enter Pending state. | Kafka pods are in Pending state due to unsatisfiable resource requests. |
| 3. | Verify pods remain stable in Pending state. | Pods stay in Pending state for stability period. |
| 4. | Update broker node pool with reasonable memory request (512Mi). | Node pool is updated with schedulable resources. |
| 5. | Wait for pods to become ready. | All Kafka pods are scheduled and running. |
| 6. | Wait for Kafka cluster to be ready. | Kafka cluster reaches Ready state. |

**Labels:**

* [kafka](labels/kafka.md)


## testRecoveryFromKafkaHeadlessServiceDeletion

**Description:** Test verifies that Cluster Operator can recover and recreate a deleted Kafka brokers headless Service.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Capture Kafka brokers headless Service UID. | Service UID is recorded. |
| 2. | Delete Kafka brokers headless Service. | Service is deleted. |
| 3. | Wait for Service recovery. | New Service is created with different UID. |
| 4. | Verify cluster stability by producing and consuming messages. | Messages are successfully produced and consumed. |

**Labels:**

* [kafka](labels/kafka.md)


## testRecoveryFromKafkaPodDeletion

**Description:** Test verifies that Kafka cluster can recover from multiple pod deletions by recreating the deleted pods.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Delete majority of Kafka broker pods (all but one). | Most broker pods are deleted. |
| 2. | Wait for all StrimziPodSets and pods to be ready. | Deleted pods are recreated and all pods are running. |
| 3. | Wait for Kafka cluster to be ready. | Kafka cluster reaches Ready state. |

**Labels:**

* [kafka](labels/kafka.md)


## testRecoveryFromKafkaServiceDeletion

**Description:** Test verifies that Cluster Operator can recover and recreate a deleted Kafka bootstrap Service.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Capture Kafka bootstrap Service UID. | Service UID is recorded. |
| 2. | Delete Kafka bootstrap Service. | Service is deleted. |
| 3. | Wait for Service recovery. | New Service is created with different UID. |
| 4. | Verify cluster stability by producing and consuming messages. | Messages are successfully produced and consumed. |

**Labels:**

* [kafka](labels/kafka.md)


## testRecoveryFromKafkaStrimziPodSetDeletion

**Description:** Test verifies that Cluster Operator can recover and recreate a deleted StrimziPodSet resource for Kafka brokers.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Capture StrimziPodSet UID for Kafka brokers. | StrimziPodSet UID is recorded. |
| 2. | Scale down Cluster Operator to 0 replicas. | Cluster Operator is stopped. |
| 3. | Delete Kafka broker StrimziPodSet. | StrimziPodSet and associated pods are deleted. |
| 4. | Scale up Cluster Operator to 1 replica. | Cluster Operator is running. |
| 5. | Wait for StrimziPodSet recovery. | New StrimziPodSet is created with different UID and all pods are ready. |

**Labels:**

* [kafka](labels/kafka.md)

