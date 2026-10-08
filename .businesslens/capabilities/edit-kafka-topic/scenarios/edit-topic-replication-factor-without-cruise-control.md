---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the replicas in the KafkaTopic resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts:
          - Replicas
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: Cruise Control is not deployed for the Kafka cluster
    kind: condition
    entities:
      - entity: kafka-cluster
        effect: reads
        facts:
          - Cruise Control
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product refuses the change and reports the partitions that would need it
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        from: Ready
        to: Not ready
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
---

# Change the replication factor without Cruise Control

## Trigger

A topic needs a different replication factor in a cluster without Cruise Control.

## Outcome

The replication factor is unchanged and the resource is Not ready with a NotSupported reason.
