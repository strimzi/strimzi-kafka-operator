---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator deletes the KafkaTopic resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product deletes the topic from Kafka and lets the resource go
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: removes
        from: Ready
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
---

# Delete a managed topic

## Trigger

An application no longer needs a topic.

## Outcome

The topic no longer exists in Kafka.

## Edge cases

- When the Kafka cluster forbids topic deletion, the resource stays Not ready with a KafkaError reason until its finalizer is removed by hand, and the topic stays in Kafka.
- A KafkaTopic resource whose reconciliation is paused still has its topic deleted from Kafka.
