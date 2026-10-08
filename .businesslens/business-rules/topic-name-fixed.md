---
appliesTo:
  - type: entity
    id: kafka-topic
    effect: changes
    facts:
      - Topic name
references:
  - kind: code
    role: implementation
    target: topic-operator/src/main/java/io/strimzi/operator/topic/BatchingTopicController.java#validateUnchangedTopicName
---

# A topic's name never changes once the topic exists

A KafkaTopic whose topic name differs from the one it was created with is refused and reported Not ready; the topic keeps its name in Kafka.
