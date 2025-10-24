# NamespaceDeletionRecoveryST

**Description:** Test suite for verifying Kafka cluster recovery after namespace deletion. Tests cover scenarios with and without `KafkaTopic` resources available, using persistent volumes with the `Retain` reclaim policy. Note: This suite requires `StorageClass` with a local provisioner on Minikube.

**Before test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a `StorageClass` with the `Retain` reclaim policy. | `StorageClass` is created with `WaitForFirstConsumer` volume binding mode. |

**After test execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Clean up orphaned persistent volumes. | All Kafka-related persistent volumes are deleted. |

**Labels:**

* [kafka](labels/kafka.md)

<hr style="border:1px solid">

## testTopicAvailable

**Description:** This test verifies Kafka cluster recovery when all `KafkaTopic` resources are available after namespace deletion, including internal topics. The test recreates `KafkaTopic` resources first, then deploys the Kafka cluster and verifies that existing data is preserved.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Prepare the test environment with a Kafka cluster and `KafkaTopic`. | The Kafka cluster is deployed with persistent storage, and the topic contains test data. |
| 2. | Store the list of all `KafkaTopic` and `PersistentVolumeClaim` resources. | All `KafkaTopic` and `PersistentVolumeClaim`  resources are captured for recovery. |
| 3. | Delete and recreate the namespace. | Namespace is deleted and recreated successfully. |
| 4. | Recreate `PersistentVolumeClaims` and rebind `PersistentVolumes` resources. | The `PersistentVolumeClaim` resources are recreated and bound to the existing `PersistentVolumeClaim` resources. |
| 5. | Recreate the Cluster Operator in the namespace. | The Cluster Operator is deployed and ready. |
| 6. | Recreate all `KafkaTopic` resources. | All `KafkaTopic` resources are recreated successfully. |
| 7. | Deploy the Kafka cluster with persistent storage. | Kafka cluster is deployed and becomes ready. |
| 8. | Verify data recovery by producing and consuming messages. | Messages can be consumed, confirming data persisted through the recovery process. |

**Labels:**

* [kafka](labels/kafka.md)


## testTopicNotAvailable

**Description:** This test verifies Kafka cluster recovery when `KafkaTopic` resources are not available after namespace deletion. The test deploys Kafka without the Topic Operator first to preserve existing topics, then enables Topic Operator after cluster becomes stable.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Prepare test environment with a Kafka cluster and test data. | The Kafka cluster is deployed with persistent storage, and the topic contains test data. |
| 2. | Store the cluster ID and list of `PersistentVolumeClaim` resources. | The Cluster ID and `PersistentVolumeClaim` list are captured for recovery. |
| 3. | List the current topics in Kafka cluster. | The topic list is logged for verification. |
| 4. | Delete and recreate the namespace. | The namespace is deleted and recreated successfully. |
| 5. | Recreate `PersistentVolumeClaims` and rebind `PersistentVolumes` resources. | The `PersistentVolumeClaim` resources are recreated and bound to the existing `PersistentVolume` resources. |
| 6. | Recreate the Cluster Operator in the namespace. | The Cluster Operator is deployed and ready. |
| 7. | Deploy Kafka without the Topic Operator using the pause annotation. | The Kafka cluster is created without Topic Operator to prevent topic deletion. |
| 8. | Patch the Kafka status with original cluster ID. | The Cluster ID is restored to match the original cluster. |
| 9. | Unpause Kafka reconciliation. | The Kafka cluster becomes ready and operational. |
| 10. | Enable the Topic Operator by updating the `Kafka` resource. | The Topic Operator is deployed and starts managing topics. |
| 11. | Verify data recovery by producing and consuming messages. | Messages can be consumed, confirming that topics and data were preserved. |

**Labels:**

* [kafka](labels/kafka.md)

