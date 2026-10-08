---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the Kafka Connect StrimziPodSet to force a rebuild
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        facts:
          - Image rebuild requested
    contexts:
      api:
        place: custom-resources::strimzi-pod-set
  - text: The Product rebuilds the image, rolls the workers onto it and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        facts:
          - Image rebuild requested
    contexts:
      api:
        place: custom-resources::strimzi-pod-set
---

# Rebuild the Kafka Connect image

## Trigger

The plugins or base image changed upstream.

## Outcome

The workers run a freshly built image.
