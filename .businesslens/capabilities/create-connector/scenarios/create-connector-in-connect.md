---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaConnector resource with the connector class, tasks and configuration
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
          - Requested state
          - Auto-restart
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Kafka Connect cluster has connector resources enabled and at least one worker
    kind: condition
    entities:
      - entity: connect-cluster
        effect: reads
        facts:
          - Connector resources enabled
          - Replicas
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
  - text: The Product creates the connector in Kafka Connect and reports it ready with its status and topics
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        from: Not ready
        to: Ready
        facts:
          - Connector status
          - Topics
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Create a connector

## Trigger

Data needs to flow between Kafka and another system.

## Outcome

The connector runs with the declared configuration.

## Edge cases

- A KafkaConnector resource without the cluster label, or naming a Kafka Connect cluster that does not exist or has no workers, stays Not ready with the reason.
- A connector or task that fails makes the resource Not ready, naming the failed tasks.
