---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaRebalance resource labelled with its cluster
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: creates
        to: New
        facts:
          - Name
          - Mode
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: Cruise Control is not deployed for the Kafka cluster
    kind: condition
    entities:
      - entity: kafka-cluster
        effect: reads
        facts:
          - Cruise Control
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: The Product reports the rebalance not ready with the reason
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: New
        to: Not ready
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
---

# Request a rebalance without Cruise Control

## Trigger

The Kafka cluster has no Cruise Control.

## Outcome

No proposal is requested and the rebalance is Not ready.

## Edge cases

- Once Cruise Control is deployed, the rebalance asks for a proposal on its own.
- A rebalance for a Kafka cluster that is missing or not Ready is Not ready too.
