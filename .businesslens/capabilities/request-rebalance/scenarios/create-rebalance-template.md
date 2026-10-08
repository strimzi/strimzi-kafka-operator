---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaRebalance resource marked as a template, with its goals and movement limits
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: creates
        to: New
        facts:
          - Name
          - Goals
          - Movement limits
          - Template
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: The Product never runs the template and keeps it for automatic rebalancing to copy
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: reads
        facts:
          - Template
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
---

# Create a rebalance template

## Trigger

Automatic rebalancing on scaling should use particular goals.

## Outcome

The template is available to the Kafka cluster's auto-rebalance on scaling; no proposal is requested for it.
