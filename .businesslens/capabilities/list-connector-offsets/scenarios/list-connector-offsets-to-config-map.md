---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaConnector resource to list its offsets
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
  - text: The Product writes the connector's offsets to the ConfigMap and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Offsets
          - Offsets request
    contexts:
      api:
        place: custom-resources::connector-offsets-config-map
---

# List connector offsets

## Trigger

The Strimzi administrator needs to see how far a connector has got.

## Outcome

The ConfigMap holds the connector's current offsets.

## Edge cases

- Without a listOffsets ConfigMap configured, the resource carries a warning, nothing is written and the request stays to be retried.
