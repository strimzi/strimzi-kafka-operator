---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaConnector resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: creates
        to: Not ready
        facts:
          - Name
          - Connector class
          - Tasks max
          - Configuration
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Kafka Connect cluster does not have connector resources enabled
    kind: condition
    entities:
      - entity: connect-cluster
        effect: reads
        facts:
          - Connector resources enabled
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product does not create the connector and reports why
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

# Create a connector for a cluster without connector resources

## Trigger

The Kafka Connect cluster is not set up to be managed through KafkaConnector resources.

## Outcome

No connector runs and the resource stays Not ready with the reason.
