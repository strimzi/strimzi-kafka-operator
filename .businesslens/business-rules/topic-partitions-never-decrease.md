---
appliesTo:
  - type: entity
    id: kafka-topic
    effect: changes
    facts:
      - Partitions
references:
  - kind: code
    role: implementation
    target: topic-operator/src/main/java/io/strimzi/operator/topic/BatchingTopicController.java#partitionChanges
---

# A topic's partitions are never decreased

Kafka cannot remove partitions, so the Topic Operator refuses a KafkaTopic that asks for fewer partitions than the topic has and reports it Not ready with a NotSupported reason.
