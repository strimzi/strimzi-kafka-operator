---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaConnect resource to use connector resources
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        facts:
          - Connector resources enabled
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
  - text: The Product deletes connectors that no KafkaConnector resource declares and reports the connector plugins
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        facts:
          - Connector plugins
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
---

# Manage a Kafka Connect cluster's connectors with KafkaConnector resources

## Trigger

The Strimzi administrator switches from the Kafka Connect REST API to KafkaConnector resources.

## Outcome

Only connectors declared by KafkaConnector resources run in the Kafka Connect cluster.
