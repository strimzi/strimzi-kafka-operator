---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator deletes the KafkaConnector resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: removes
        from: Ready
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product deletes the connector from Kafka Connect
    kind: product
    actor: strimzi-administrator
    entities: []
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Delete a connector

## Trigger

The connector is no longer needed.

## Outcome

The connector no longer runs.

## Edge cases

- A connector whose reconciliation is paused is still deleted from Kafka Connect.
- When the Kafka Connect cluster has no workers at the time, the connector is deleted once workers run again.
