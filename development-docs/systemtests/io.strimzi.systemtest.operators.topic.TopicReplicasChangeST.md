# TopicReplicasChangeST

**Description:** Validates KafkaTopic replication factor change logic using the Topic Operator and Cruise Control.

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with Cruise Control and Topic Operator configured for rapid reconciliation. | Kafka cluster with Cruise Control is deployed and ready. |
| 2. | Deploy scraper pod for topic status validation. | Scraper pod is running and accessible. |
| 3. | Create initial KafkaTopic resources with valid and invalid replication factors. | KafkaTopics are created and visible to Topic Operator. |

**Labels:**

* [topic-operator](labels/topic-operator.md)

<hr style="border:1px solid">

## testKafkaTopicReplicaChangeNegativeRoundTrip

**Description:** Attempts to increase KafkaTopic replication factor beyond broker count, then corrects it.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaTopic with valid replication factor. | KafkaTopic is created and Ready. |
| 2. | Attempt to increase replication factor beyond available brokers. | Replica change fails with error due to insufficient brokers. |
| 3. | Restore to valid replication factor. | KafkaTopic becomes Ready and replica change status clears. |

**Labels:**

* [topic-operator](labels/topic-operator.md)


## testKafkaTopicReplicaChangePositiveRoundTrip

**Description:** Tests increasing and then decreasing a KafkaTopic’s replication factor, verifying correctness and topic readiness.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaTopic with initial replication factor. | KafkaTopic is created and ready. |
| 2. | Increase the replication factor. | Replica change is applied and KafkaTopic remains Ready. |
| 3. | Decrease replication factor back to original value. | KafkaTopic is updated and Ready with correct replica count. |

**Labels:**

* [topic-operator](labels/topic-operator.md)


## testMoreReplicasThanAvailableBrokersWithFreshKafkaTopic

**Description:** Verifies behavior when creating a KafkaTopic with a replication factor exceeding the number of brokers.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create a KafkaTopic with more replicas than available brokers. | Topic exists in Kubernetes, but not in Kafka. |
| 2. | Check KafkaTopic status for replication factor error. | Error message and status reason indicate KafkaError. |
| 3. | Adjust replication factor to valid value and wait for reconciliation. | KafkaTopic becomes Ready and replication status clears. |

**Labels:**

* [topic-operator](labels/topic-operator.md)


## testRecoveryOfReplicationChangeDuringCcCrash

**Description:** Verifies replication factor change recovery after Cruise Control crash.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initiate replication factor change. | Replica change is ongoing. |
| 2. | Crash the Cruise Control pod. | Cruise Control is terminated and recovers. |
| 3. | Wait for change to complete. | Replica change status clears and KafkaTopic is Ready. |

**Labels:**

* [topic-operator](labels/topic-operator.md)


## testRecoveryOfReplicationChangeDuringEoCrash

**Description:** Verifies KafkaTopic replication change proceeds after Entity Operator crash.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Initiate replication factor change. | Replica change is ongoing. |
| 2. | Crash the Entity Operator pod. | Entity Operator is terminated and recovers. |
| 3. | Wait for change to complete. | KafkaTopic is updated and Ready with correct replication factor. |

**Labels:**

* [topic-operator](labels/topic-operator.md)

