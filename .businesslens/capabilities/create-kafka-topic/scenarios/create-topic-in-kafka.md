---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaTopic resource labelled with its cluster, with partitions, replicas and configuration
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
  - text: The Product creates the topic in Kafka and reports it ready with its topic name and ID
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

# Create a topic

## Trigger

An application needs a topic.

## Outcome

The topic exists in Kafka as declared.

## Edge cases

- A KafkaTopic resource without the label of a cluster the Topic Operator serves is ignored.
- A KafkaTopic resource created with reconciliation paused is not applied until the pause is removed.
