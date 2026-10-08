---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaConnector resource to alter its offsets
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Offsets request
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The connector is not stopped
    kind: condition
    entities:
      - entity: connector
        effect: reads
        facts:
          - Connector status
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product leaves the offsets unchanged and reports a warning, retrying on the next reconciliation
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Alter the offsets of a connector that is not stopped

## Trigger

The Strimzi administrator asks to alter offsets while the connector runs.

## Outcome

The offsets are unchanged and the resource carries a warning.
