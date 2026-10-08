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
    target: documentation/modules/operators/con-disabling-topic-deletion.adoc
---

# Delete a topic

Delete a topic by deleting its `KafkaTopic` resource. A managed topic is deleted from Kafka; an unmanaged one stays there.
