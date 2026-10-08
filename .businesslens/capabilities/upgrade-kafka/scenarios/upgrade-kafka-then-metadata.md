---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator sets a newer Kafka version in the Kafka resource
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
  - text: The Product rolls the Kafka nodes one at a time onto the new Kafka version, keeping the current metadata version
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product reports the new current Kafka version
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
  - text: The Strimzi administrator sets the newer metadata version
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
  - text: The Product updates the cluster's metadata version and reports it
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts:
          - Current metadata version
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Upgrade Kafka and its metadata version

## Trigger

A newer Kafka version is supported by the installed Strimzi version.

## Outcome

The cluster runs the newer Kafka version with the newer metadata version.

## Edge cases

- When the Kafka resource names no metadata version, the Product moves to the new Kafka version's default metadata version after the roll.
- An unsupported Kafka version leaves the Kafka resource Not ready and the Kafka nodes unchanged.
