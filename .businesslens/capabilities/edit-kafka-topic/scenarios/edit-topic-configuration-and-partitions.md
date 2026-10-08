---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the topic configuration or raises the partitions in the KafkaTopic resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts:
          - Configuration
          - Partitions
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product alters the topic configuration and adds the partitions in Kafka
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
---

# Change a topic's configuration or add partitions

## Trigger

An application needs the topic configured differently or with more partitions.

## Outcome

The topic in Kafka matches the resource and stays Ready.

## Edge cases

- Configuration the Topic Operator is set not to alter is left unchanged, and the resource carries an InvalidConfig warning while staying Ready.
