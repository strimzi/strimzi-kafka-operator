---
domain: topics
references:
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/topic/KafkaTopicSpec.java#KafkaTopicSpec
  - kind: code
    role: implementation
    target: api/src/main/java/io/strimzi/api/kafka/model/topic/KafkaTopicStatus.java#KafkaTopicStatus
  - kind: code
    role: implementation
    target: topic-operator/src/main/java/io/strimzi/operator/topic/BatchingTopicController.java#BatchingTopicController
  - kind: doc
    role: context
    target: documentation/modules/operators/proc-configuring-kafka-topic.adoc
---

# Kafka topic

A topic in a Kafka cluster that a `KafkaTopic` resource declares and the Topic Operator keeps in line with it.

## Information kept

- **Resource name** — the name of the KafkaTopic resource
- **Topic name** — the name of the topic in Kafka; the resource name when omitted, and the name first used, reported in status
- **Partitions** — the number of partitions
- **Replicas** — the replication factor
- **Configuration** — topic-level Kafka configuration
- **Topic ID** — the topic's ID in Kafka, reported in status
- **Replicas change** — the progress of a replication factor change carried out by Cruise Control, reported in status
- **Status message** — the reason and message of the latest NotReady or Warning condition

## States

### Not ready

The topic in Kafka does not match the resource yet, or the change was refused.

### Ready

The topic in Kafka matches the resource.

### Unmanaged

The Topic Operator leaves the topic in Kafka alone.

### Reconciliation paused

The Topic Operator ignores the resource until the pause is removed.
