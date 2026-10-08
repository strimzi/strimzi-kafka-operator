---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the node pool's StrimziPodSet for a manual rolling update
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: node-pool
        effect: changes
        facts:
          - Manual rolling update
    contexts:
      api:
        place: custom-resources::strimzi-pod-set
  - text: The Product restarts the pool's Kafka nodes one at a time and clears the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts: []
      - entity: node-pool
        effect: changes
        facts:
          - Manual rolling update
    contexts:
      api:
        place: custom-resources::strimzi-pod-set
---

# Roll every Kafka node of a node pool

## Trigger

The Strimzi administrator wants all Kafka nodes of a pool restarted.

## Outcome

Every Kafka node of the pool has restarted and rejoined the cluster.
