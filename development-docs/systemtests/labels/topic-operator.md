# **Topic Operator**

## Description

These tests cover management of KafkaTopic resources by the Topic Operator. 
They verify topic creation, updates, deletion behavior, quota enforcement, and metrics to ensure reliable topic operations within a Kafka cluster.

<!-- generated part -->
**Tests:**
- [testKafkaTopicReplicaChangeNegativeRoundTrip](../io.strimzi.systemtest.operators.topic.TopicReplicasChangeST.md)
- [testKafkaTopicReplicaChangePositiveRoundTrip](../io.strimzi.systemtest.operators.topic.TopicReplicasChangeST.md)
- [testMoreReplicasThanAvailableBrokersWithFreshKafkaTopic](../io.strimzi.systemtest.operators.topic.TopicReplicasChangeST.md)
- [testRecoveryOfReplicationChangeDuringCcCrash](../io.strimzi.systemtest.operators.topic.TopicReplicasChangeST.md)
- [testRecoveryOfReplicationChangeDuringEoCrash](../io.strimzi.systemtest.operators.topic.TopicReplicasChangeST.md)
- [testThrottlingQuotasDuringAllTopicOperations](../io.strimzi.systemtest.operators.topic.ThrottlingQuotaST.md)
