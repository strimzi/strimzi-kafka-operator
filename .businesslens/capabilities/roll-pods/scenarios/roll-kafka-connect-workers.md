---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the Kafka Connect cluster's StrimziPodSet for a manual rolling update
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        facts:
          - Manual rolling update
    contexts:
      api:
        place: custom-resources::strimzi-pod-set
  - text: The Product restarts the workers one at a time and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        facts:
          - Manual rolling update
    contexts:
      api:
        place: custom-resources::strimzi-pod-set
---

# Roll the workers of a Kafka Connect cluster

## Trigger

The Strimzi administrator wants the Kafka Connect workers restarted.

## Outcome

Every worker has restarted.

## Edge cases

- MirrorMaker 2 workers roll the same way, from their StrimziPodSet or from single pods.
