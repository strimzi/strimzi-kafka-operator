---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator sets a metadata version newer than the Kafka version
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts:
          - Metadata version
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The requested metadata version is newer than the Kafka version the cluster runs
    kind: condition
    entities:
      - entity: kafka-cluster
        effect: reads
        facts:
          - Metadata version
          - Current Kafka version
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product keeps the current metadata version
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Request a metadata version newer than Kafka

## Trigger

The Strimzi administrator asks for a metadata version the cluster's Kafka version does not support.

## Outcome

The Kafka resource keeps reporting the current metadata version.
