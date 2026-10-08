---
appliesTo:
  - type: capability
    id: create-kafka-topic
  - type: entity
    id: kafka-topic
references:
  - kind: code
    role: implementation
    target: topic-operator/src/main/java/io/strimzi/operator/topic/BatchingTopicController.java#validateSingleManagingResource
---

# Only the oldest KafkaTopic resource for a topic manages it

When several KafkaTopic resources name the same topic, the one created first manages it and the others are reported Not ready with a ResourceConflict reason naming it.
