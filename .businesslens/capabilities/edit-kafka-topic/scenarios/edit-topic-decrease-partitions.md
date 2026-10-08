---
kind: validation
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator lowers the partitions in the KafkaTopic resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts:
          - Partitions
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product refuses the change and reports the topic not ready
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

# Lower a topic's partitions

## Trigger

The Strimzi administrator tries to remove partitions.

## Outcome

The topic keeps its partitions and the resource is Not ready with a NotSupported reason.
