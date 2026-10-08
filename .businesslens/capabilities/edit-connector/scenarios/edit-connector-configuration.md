---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the configuration, plugin version or tasks in the KafkaConnector resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Configuration
          - Plugin version
          - Tasks max
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product updates the connector in Kafka Connect and reports its status
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        facts:
          - Connector status
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Change a connector's configuration

## Trigger

The connector needs a different configuration.

## Outcome

The connector runs with the new configuration.
