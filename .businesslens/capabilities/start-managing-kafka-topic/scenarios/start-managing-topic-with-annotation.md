---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator marks the KafkaTopic resource as managed
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product applies the resource to the topic in Kafka and reports it ready
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        from: Unmanaged
        to: Ready
        facts:
          - Topic name
          - Topic ID
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
---

# Start managing a topic again

## Trigger

The Strimzi administrator wants the Topic Operator to manage an unmanaged topic again.

## Outcome

The topic in Kafka matches the resource.
