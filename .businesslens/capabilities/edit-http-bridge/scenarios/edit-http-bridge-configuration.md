---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the replicas or configuration in the KafkaBridge resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: http-bridge
        effect: changes
        facts:
          - Replicas
          - HTTP configuration
          - Client configuration
    contexts:
      api:
        place: custom-resources::kafka-bridge-resource
  - text: The Product updates the HTTP Bridge deployment
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: http-bridge
        effect: changes
        facts: []
    contexts:
      api:
        place: custom-resources::kafka-bridge-resource
---

# Change an HTTP Bridge

## Trigger

The HTTP Bridge needs a different size or configuration.

## Outcome

The HTTP Bridge runs as declared.

## Edge cases

- With zero replicas, the HTTP Bridge reports no address.
