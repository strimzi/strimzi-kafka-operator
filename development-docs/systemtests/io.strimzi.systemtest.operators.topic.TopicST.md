# TopicST

**Description:** Covers Topic Operator general functionality and edge-case scenarios.

**Labels:**

* [topic-operator](labels/topic-operator.md)

<hr style="border:1px solid">

## testCreateDeleteCreate

**Description:** Repeatedly create, delete, and recreate a KafkaTopic. Checks proper creation/deletion in both CRD and Kafka.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaTopic. | Topic appears in adminClient and CRD. |
| 2. | Loop ten times: Delete topic, verify absence, recreate, verify presence. | Topics are correctly deleted and recreated in both places. |


## testCreateTopicAfterUnsupportedOperation

**Description:** Verifies topic creation after an unsupported operation, like decreasing partitions or replicas.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaTopic with partitions and replicas. | Topic created and Ready. |
| 2. | Attempt unsupported decrease of partitions/replicas. | KafkaTopic NotReady, error message in status. |
| 3. | Create new valid topic after failed operation. | New topic created and Ready. |
| 4. | Delete both topics. | Topics deleted from CRD and Kafka. |


## testDeleteTopicEnableFalse

**Description:** Verifies Topic deletion behavior when 'delete.topic.enable' is false, then enabled.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka with 'delete.topic.enable=false'. | Cluster is up, topic deletion disabled. |
| 2. | Create KafkaTopic and ensure it exists. | Topic present in CRD and Kafka. |
| 3. | Produce and consume messages. | Messages sent and received successfully. |
| 4. | Attempt to delete KafkaTopic. | Deletion blocked, error surfaced in status. |
| 5. | Enable topic deletion and delete topic. | Topic deleted from both CRD and Kafka. |


## testKafkaTopicChangingMinInSyncReplicas

**Description:** Verifies status handling for invalid min.insync.replicas configuration in KafkaTopic.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaTopic and ensure Ready. | Topic Ready. |
| 2. | Set min.insync.replicas to invalid value. | KafkaTopic NotReady, error message in status. |
| 3. | Wait for reconciliation and check error persists. | Status and error remain NotReady. |


## testKafkaTopicDifferentStatesInUTOMode

**Description:** Checks Topic Operator metrics and KafkaTopic status transitions when modifying topic name, replicas, and partitions in UTO mode.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaTopic. | KafkaTopic is ready. |
| 2. | Create metrics collector and collect Topic Operator metrics. | Metrics collected. |
| 3. | Check that Topic Operator metrics contain reconciliation data. | Metrics contain proper data. |
| 4. | Check that metrics include KafkaTopic in Ready state. | Metrics contain proper data. |
| 5. | Change spec.topicName and wait for NotReady. | KafkaTopic is in NotReady state. |
| 6. | Check that metrics show renaming is not allowed and status reflects this. | Metrics and status have proper values. |
| 7. | Revert topic name and change replica count. | Replica count is changed. |
| 8. | Check that metrics show replica change is not allowed and status reflects this. | Metrics and status have proper values. |
| 9. | Decrease partition count. | Partition count is changed. |
| 10. | Check that metrics and status reflect partition count change is not allowed. | Metrics and status have proper values. |
| 11. | Set KafkaTopic configuration to defaults. | KafkaTopic is in Ready state. |
| 12. | Check that metrics include KafkaTopic in Ready state. | Metrics contain proper data. |


## testMoreReplicasThanAvailableBrokers

**Description:** Verifies Topic CRs with more replicas than brokers are not created in Kafka, error surfaced in status.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create KafkaTopic with replicationFactor > brokers. | KafkaTopic appears in Kubernetes but not in Kafka. |
| 2. | Wait for NotReady status and error message. | KafkaTopic status NotReady, error message present. |
| 3. | Delete topic and verify cleanup. | Topic removed from Kubernetes and Kafka. |
| 4. | Create topic with correct replication factor. | Topic is created in Kafka and Kubernetes. |


## testSendingMessagesToNonExistingTopic

**Description:** Verifies that sending messages to a non-existing topic triggers auto-creation when 'auto.topic.creation' is enabled.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Ensure topic does not exist in Kafka. | Topic is absent. |
| 2. | Send messages to non-existent topic. | Kafka auto-creates the topic. |
| 3. | Check topic exists in Kafka. | Topic appears in adminClient. |


## testTopicWithoutLabels

**Description:** Verifies that KafkaTopic CRs without labels are ignored by the Topic Operator.

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy Kafka cluster with short reconciliation interval. | Kafka cluster and Topic Operator are ready. |
| 2. | Create KafkaTopic without labels. | KafkaTopic is created but not handled. |
| 3. | Verify KafkaTopic is not created and check logs. | KafkaTopic absent in Kafka, log shows ignored topic. |
| 4. | Delete KafkaTopic. | KafkaTopic is deleted, topic absent in Kafka. |

