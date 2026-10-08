---
availability:
  - place: custom-resources
domain: topics
references:
  - kind: code
    role: implementation
    target: topic-operator/src/main/java/io/strimzi/operator/topic/TopicOperatorUtil.java#isManaged
  - kind: doc
    role: context
    target: documentation/modules/operators/proc-converting-managed-topics.adoc
---

# Stop managing a topic

Mark a `KafkaTopic` resource as unmanaged so the Topic Operator leaves the topic in Kafka alone, for example to delete or recreate the resource without affecting the topic.
