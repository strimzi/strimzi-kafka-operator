---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator marks the KafkaTopic resource as not managed
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product stops changing the topic in Kafka and reports it unmanaged
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        from: Ready
        to: Unmanaged
        facts:
          - Topic name
          - Topic ID
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
---

# Stop managing a topic

## Trigger

The Strimzi administrator wants to change or remove a KafkaTopic resource without affecting the topic.

## Outcome

The topic stays in Kafka as it is and changes to the resource are not applied.
