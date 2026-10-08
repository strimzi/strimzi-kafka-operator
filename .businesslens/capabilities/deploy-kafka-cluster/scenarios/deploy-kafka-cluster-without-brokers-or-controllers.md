---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates the Kafka resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: creates
        to: Not ready
        facts:
          - Name
          - Kafka version
          - Broker configuration
      - entity: listener
        effect: creates
        facts:
          - Name
          - Port
          - Type
          - TLS encryption
          - Client authentication
          - Listener configuration
          - Network policy peers
        with: kafka-cluster
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: No node pool labelled with the cluster name has the broker role, or none has the controller role, with at least one replica
    kind: condition
    entities:
      - entity: node-pool
        effect: reads
        facts:
          - Roles
          - Replicas
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product starts nothing and reports what is missing
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Deploy a Kafka cluster without brokers or controllers

## Trigger

The node pools of a new Kafka cluster are missing, empty or lack a role.

## Outcome

No Kafka node runs and the Kafka resource stays Not ready with the reason.
