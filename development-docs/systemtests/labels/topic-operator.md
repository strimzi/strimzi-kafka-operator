# **Topic Operator**

## Description

These tests cover management of KafkaTopic resources by the Topic Operator. 
They verify topic creation, updates, deletion behavior, quota enforcement, and metrics to ensure reliable topic operations within a Kafka cluster.

<!-- generated part -->
**Tests:**
- [testCreateDeleteCreate](../io.strimzi.systemtest.operators.topic.TopicST.md)
- [testCreateTopicAfterUnsupportedOperation](../io.strimzi.systemtest.operators.topic.TopicST.md)
- [testDeleteTopicEnableFalse](../io.strimzi.systemtest.operators.topic.TopicST.md)
- [testKafkaTopicChangingMinInSyncReplicas](../io.strimzi.systemtest.operators.topic.TopicST.md)
- [testKafkaTopicDifferentStatesInUTOMode](../io.strimzi.systemtest.operators.topic.TopicST.md)
- [testKafkaTopicReplicaChangeNegativeRoundTrip](../io.strimzi.systemtest.operators.topic.TopicReplicasChangeST.md)
- [testKafkaTopicReplicaChangePositiveRoundTrip](../io.strimzi.systemtest.operators.topic.TopicReplicasChangeST.md)
- [testMoreReplicasThanAvailableBrokers](../io.strimzi.systemtest.operators.topic.TopicST.md)
- [testMoreReplicasThanAvailableBrokersWithFreshKafkaTopic](../io.strimzi.systemtest.operators.topic.TopicReplicasChangeST.md)
- [testRecoveryOfReplicationChangeDuringCcCrash](../io.strimzi.systemtest.operators.topic.TopicReplicasChangeST.md)
- [testRecoveryOfReplicationChangeDuringEoCrash](../io.strimzi.systemtest.operators.topic.TopicReplicasChangeST.md)
- [testSendingMessagesToNonExistingTopic](../io.strimzi.systemtest.operators.topic.TopicST.md)
- [testThrottlingQuotasDuringAllTopicOperations](../io.strimzi.systemtest.operators.topic.ThrottlingQuotaST.md)
- [testTopicWithoutLabels](../io.strimzi.systemtest.operators.topic.TopicST.md)
