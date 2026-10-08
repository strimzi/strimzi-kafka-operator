---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the replicas or configuration in the KafkaConnect resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        facts:
          - Replicas
          - Connect configuration
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
  - text: The Product scales the workers and rolls them where the configuration changed
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
---

# Change a Kafka Connect cluster

## Trigger

The Kafka Connect cluster needs a different size or configuration.

## Outcome

The workers run as declared and the Kafka Connect cluster stays Ready.

## Edge cases

- With zero replicas, no REST API address or plugins are reported and every KafkaConnector resource of the cluster reports that it has no workers.
