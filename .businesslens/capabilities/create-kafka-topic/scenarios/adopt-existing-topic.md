---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaTopic resource for a topic that already exists in Kafka
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: creates
        to: Not ready
        facts:
          - Resource name
          - Topic name
          - Partitions
          - Replicas
          - Configuration
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product applies the declared configuration and any added partitions to the existing topic and reports it ready
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        from: Not ready
        to: Ready
        facts:
          - Topic name
          - Topic ID
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
---

# Manage a topic that already exists in Kafka

## Trigger

A topic was created in Kafka outside Strimzi and should be managed from now on.

## Outcome

The existing topic is managed by its KafkaTopic resource.
