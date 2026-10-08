---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the topic name in the KafkaTopic resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts:
          - Topic name
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product refuses the change and reports the topic not ready
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        from: Ready
        to: Not ready
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
---

# Change a topic's name

## Trigger

The Strimzi administrator tries to rename a topic.

## Outcome

The topic keeps its name in Kafka and the resource is Not ready with a NotSupported reason.
