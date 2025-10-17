# NamespaceDeletionRecoveryST

**Description:** Test suite for verifying Kafka cluster recovery after namespace deletion. Tests cover scenarios with and without KafkaTopic resources available, using persistent volumes with Retain policy. Note: Suite requires StorageClass with local provisioner on Minikube.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create StorageClass with Retain reclaim policy. | StorageClass is created with WaitForFirstConsumer volume binding mode. |

**After test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Clean up orphaned PersistentVolumes. | All Kafka-related PersistentVolumes are deleted. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testTopicAvailable

**Description:** This test verifies Kafka cluster recovery when all KafkaTopic resources are available after namespace deletion, including internal topics. The test recreates KafkaTopic resources first, then deploys the Kafka cluster, and validates that existing data is preserved.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Prepare test environment with Kafka cluster and KafkaTopic. | Kafka cluster is deployed with persistent storage and topic contains test data. |
| 2. | Store list of all KafkaTopic resources and PersistentVolumeClaims. | All KafkaTopic and PVC resources are captured for recovery. |
| 3. | Delete and recreate the namespace. | Namespace is deleted and recreated successfully. |
| 4. | Recreate PersistentVolumeClaims and update PersistentVolumes. | PVCs are recreated and bound to existing PVs. |
| 5. | Recreate Cluster Operator in the namespace. | Cluster Operator is deployed and ready. |
| 6. | Recreate all KafkaTopic resources. | All KafkaTopic resources are recreated successfully. |
| 7. | Deploy Kafka cluster with persistent storage. | Kafka cluster is deployed and becomes ready. |
| 8. | Verify data recovery by producing and consuming messages. | Messages can be consumed, confirming data was preserved through recovery. |

**Labels:**

* [kafka](labels/kafka.md)


## testTopicNotAvailable

**Description:** This test verifies Kafka cluster recovery when KafkaTopic resources are not available after namespace deletion. The test deploys Kafka without Topic Operator first to preserve existing topics, then enables Topic Operator after cluster is stable.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Prepare test environment with Kafka cluster and test data. | Kafka cluster is deployed with persistent storage and topic contains test data. |
| 2. | Store cluster ID and list of PersistentVolumeClaims. | Cluster ID and PVC list are captured for recovery. |
| 3. | List current topics in Kafka cluster. | Topic list is logged for verification. |
| 4. | Delete and recreate the namespace. | Namespace is deleted and recreated successfully. |
| 5. | Recreate PersistentVolumeClaims and update PersistentVolumes. | PVCs are recreated and bound to existing PVs. |
| 6. | Recreate Cluster Operator in the namespace. | Cluster Operator is deployed and ready. |
| 7. | Deploy Kafka without Topic Operator using pause annotation. | Kafka cluster is created without Topic Operator to prevent topic deletion. |
| 8. | Patch Kafka status with original cluster ID. | Cluster ID is restored to match the original cluster. |
| 9. | Unpause Kafka reconciliation. | Kafka cluster becomes ready and operational. |
| 10. | Enable Topic Operator by updating Kafka spec. | Topic Operator is deployed and starts managing topics. |
| 11. | Verify data recovery by producing and consuming messages. | Messages can be consumed, confirming topics and data were preserved. |

**Labels:**

* [kafka](labels/kafka.md)

