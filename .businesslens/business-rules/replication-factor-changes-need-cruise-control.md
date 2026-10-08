---
appliesTo:
  - type: capability-scenario
    id: edit-topic-replication-factor
  - type: capability-scenario
    id: edit-topic-replication-factor-without-cruise-control
references:
  - kind: code
    role: implementation
    target: topic-operator/src/main/java/io/strimzi/operator/topic/BatchingTopicController.java#checkReplicasChanges
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/model/EntityTopicOperator.java#EntityTopicOperator
---

# A topic's replication factor changes only through Cruise Control

The Topic Operator carries out a replication factor change only when Cruise Control is deployed for the Kafka cluster; otherwise it refuses the change and reports the partitions that would need it.
