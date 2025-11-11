# RecoveryST

**Description:** Test suite verifying the Cluster Operator's ability to recover Kafka cluster components from various deletion and failure scenarios.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy a Kafka cluster with broker and controller node pools configured for 3 replicas each, and HTTP Bridge. | Kafka cluster with 3 broker and 3 controller replicas, and HTTP Bridge deployed. |

<hr style="border:1px solid">

## testRecoveryFromImpossibleMemoryRequest

**Description:** This test verifies that the Kafka cluster can recover from invalid resource requests by correcting the resource configuration.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Update the broker node pool configuration with a memory request to large to process (465458732Gi). | Node pool is updated. |
| 2. | Wait for Kafka pods to enter `Pending` state. | Kafka pods are in `Pending` state due to unfulfillable resource requests. |
| 3. | Verify pods remain stable in `Pending` state. | Pods stay in `Pending` state for stability period. |
| 4. | Update the broker node pool configuration with a fulfillable memory request (512Mi). | Node pool is updated. |
| 5. | Wait for pods to reach a `Ready` state. | All Kafka pods are scheduled and running. |
| 6. | Wait for the Kafka cluster to reach a `Ready` state. | Kafka cluster is running. |

**Labels:**

* [kafka](labels/kafka.md)


## testRecoveryFromKafkaHeadlessServiceDeletion

**Description:** Test verifies that Cluster Operator can recover and recreate a deleted Kafka brokers headless Service.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Capture the headless `Service` UID. | Service UID is recorded. |
| 2. | Delete the headless `Service`. | `Service` is deleted. |
| 3. | Wait for `Service` recovery. | A new `Service` is created with a different UID. |
| 4. | Verify cluster stability by producing and consuming messages. | Messages are successfully produced and consumed. |

**Labels:**

* [kafka](labels/kafka.md)


## testRecoveryFromKafkaPodDeletion

**Description:** This test verifies that the Kafka cluster can recover from multiple pod deletions by recreating the deleted pods.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Delete all but one Kafka broker pods in the cluster. | One broker pod remains. |
| 2. | Wait for all `StrimziPodSet` resources and pods to reach a `Ready` state. | Deleted pods are recreated and all pods are running. |
| 3. | Wait for the Kafka cluster to reach a `Ready` state. | Kafka cluster is running. |

**Labels:**

* [kafka](labels/kafka.md)


## testRecoveryFromKafkaServiceDeletion

**Description:** This test verifies that the Cluster Operator can recover and recreate a deleted Kafka bootstrap `Service`.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Capture Kafka bootstrap `Service` UID. | `Service` UID is recorded. |
| 2. | Delete Kafka bootstrap `Service`. | `Service` is deleted. |
| 3. | Wait for `Service` recovery. | A new `Service` is created with a different UID. |
| 4. | Verify cluster stability by producing and consuming messages. | Messages are successfully produced and consumed. |

**Labels:**

* [kafka](labels/kafka.md)


## testRecoveryFromKafkaStrimziPodSetDeletion

**Description:** This test verifies that the Cluster Operator can recover and recreate a deleted `StrimziPodSet` resource for Kafka brokers.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Capture `StrimziPodSet` UID. | `StrimziPodSet` UID is recorded. |
| 2. | Scale down Cluster Operator to 0 replicas. | Cluster Operator is stopped. |
| 3. | Delete `StrimziPodSet` for the Kafka broker. | `StrimziPodSet` and associated pods are deleted. |
| 4. | Scale up Cluster Operator to 1 replica. | Cluster Operator is running. |
| 5. | Wait for `StrimziPodSet` recovery. | A new `StrimziPodSet` with a different UID is created, and all pods are in in a `Ready` state. |

**Labels:**

* [kafka](labels/kafka.md)

