---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator deletes the KafkaBridge resource
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: http-bridge
        effect: removes
        from: Ready
    contexts:
      api:
        place: custom-resources::kafka-bridge-resource
  - text: The Product's bridge pods and services are removed, together with the cluster-wide permission they held for rack awareness
    kind: product
    actor: strimzi-administrator
    entities: []
    contexts:
      api:
        place: custom-resources::kafka-bridge-resource
---

# Delete an HTTP Bridge

## Trigger

HTTP access is no longer needed.

## Outcome

The HTTP Bridge address no longer answers.
