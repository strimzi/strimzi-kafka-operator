---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator deletes the KafkaTopic resource of an unmanaged topic
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product lets the resource go and leaves the topic in Kafka
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: removes
        from: Unmanaged
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
---

# Delete the resource of an unmanaged topic

## Trigger

The Strimzi administrator wants to stop tracking a topic without deleting its data.

## Outcome

The topic stays in Kafka without a KafkaTopic resource.
