---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator sets an older Kafka version in the Kafka resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts:
          - Kafka version
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The current metadata version is supported by the older Kafka version
    kind: condition
    entities:
      - entity: kafka-cluster
        effect: reads
        facts:
          - Current metadata version
          - Kafka version
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product rolls the Kafka nodes one at a time onto the older Kafka version
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product reports the older current Kafka version
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts:
          - Current Kafka version
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Downgrade Kafka

## Trigger

The Strimzi administrator needs to return to an earlier Kafka version.

## Outcome

The cluster runs the older Kafka version.
