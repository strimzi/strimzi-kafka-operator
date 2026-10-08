---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator writes the offsets to apply into the alterOffsets ConfigMap
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Offsets
    contexts:
      api:
        place: custom-resources::connector-offsets-config-map
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
  - text: The connector is stopped
    kind: condition
    entities:
      - entity: connector
        effect: reads
        facts:
          - Connector status
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product applies the offsets from the ConfigMap and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Offsets request
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Alter a stopped connector's offsets

## Trigger

A connector must reprocess or skip data.

## Outcome

The connector resumes from the new offsets when it runs again.

## Edge cases

- A missing ConfigMap, missing data or invalid offsets leave a warning on the resource and the request in place.
