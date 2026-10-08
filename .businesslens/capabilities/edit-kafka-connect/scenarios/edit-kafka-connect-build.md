---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator changes the plugin artifacts or output image in the build
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        facts:
          - Build
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
  - text: The Product builds a new image and rolls the workers onto it
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

# Change the plugins built into the Kafka Connect image

## Trigger

The workers need different connector plugins.

## Outcome

The workers run with the new plugins available.
