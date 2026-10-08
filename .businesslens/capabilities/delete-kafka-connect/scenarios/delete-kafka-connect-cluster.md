---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator deletes the KafkaConnect resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: removes
        from: Ready
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
  - text: The Product reports on each KafkaConnector resource that named it that its cluster no longer exists
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connector
        effect: changes
        from: Ready
        to: Not ready
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-connector-resource
---

# Delete a Kafka Connect cluster

## Trigger

The Kafka Connect cluster is no longer needed.

## Outcome

The workers no longer run; KafkaConnector resources that named the cluster remain, Not ready.
