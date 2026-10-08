---
availability:
  - place: custom-resources
domain: topics
references:
  - kind: code
    role: implementation
    target: topic-operator/src/main/java/io/strimzi/operator/topic/BatchingTopicController.java#BatchingTopicController
  - kind: doc
    role: context
    target: documentation/modules/operators/proc-configuring-kafka-topic.adoc
---

# Create a topic

Declare a topic with a `KafkaTopic` resource labelled with its Kafka cluster. The Topic Operator creates it in Kafka, or adopts a topic of that name that already exists there.
