---
kind: edge
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaTopic resource for a topic another KafkaTopic resource already manages
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: creates
        to: Not ready
        facts:
          - Resource name
          - Topic name
          - Partitions
          - Replicas
          - Configuration
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
  - text: The Product leaves the topic as the older resource declares it and reports a resource conflict naming it on the newer one
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-topic
        effect: changes
        facts:
          - Status message
    contexts:
      api:
        place: custom-resources::kafka-topic-resource
---

# Create a second resource for a managed topic

## Trigger

Two KafkaTopic resources name the same topic.

## Outcome

Only the older resource manages the topic; the newer one stays Not ready with a ResourceConflict reason.

## Edge cases

- When both resources were created at the same moment, the one already Ready keeps managing the topic.
