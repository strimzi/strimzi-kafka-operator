---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator adds or removes the Topic Operator, User Operator, Cruise Control or Kafka Exporter in the Kafka resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts:
          - Topic Operator
          - User Operator
          - Cruise Control
          - Kafka Exporter
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product deploys the added components, removes the others, and adds or removes Cruise Control's metrics reporter in the brokers' configuration
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Add or remove components deployed beside a Kafka cluster

## Trigger

The cluster needs topic or user management, rebalancing or lag metrics, or no longer does.

## Outcome

Exactly the declared components run beside the cluster.
