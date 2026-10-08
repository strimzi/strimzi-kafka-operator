---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaConnector resource to restart the connector, optionally including all or only failed tasks
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Restart request
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product restarts the connector in Kafka Connect and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Connector status
          - Restart request
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Restart a connector

## Trigger

A connector misbehaves.

## Outcome

The connector has restarted.

## Edge cases

- An annotation value Strimzi does not recognize stays on the resource with a warning, and nothing restarts.
- A restart Kafka Connect refuses leaves the request in place with a warning, to be retried at the next reconciliation.
