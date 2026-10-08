---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the Kafka node's pod to delete the pod and its volume claims
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts:
          - Pod and volume deletion
    contexts:
      api:
        place: custom-resources::pod
  - text: The Product deletes the pod and its volume claims, then starts the Kafka node again with new volumes
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts:
          - Volumes
          - Pod and volume deletion
    contexts:
      api:
        place: custom-resources::pod
---

# Delete a node's pod and volume

## Trigger

A Kafka node's volume needs to be replaced.

## Outcome

The Kafka node runs again with the same node ID on new volumes and catches up from the other replicas.

## Edge cases

- Only one annotated pod is handled per reconciliation; others wait for the next one.
