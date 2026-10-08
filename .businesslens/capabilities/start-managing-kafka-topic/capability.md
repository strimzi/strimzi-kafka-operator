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
    target: documentation/modules/operators/proc-converting-non-managed-topics.adoc
---

# Start managing a topic again

Mark an unmanaged `KafkaTopic` resource as managed again so the Topic Operator applies it to the topic.
