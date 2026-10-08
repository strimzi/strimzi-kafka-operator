---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Drain Cleaner annotates the pod of a Kafka node being evicted for a manual rolling update
    kind: actor
    actor: drain-cleaner
    entities:
      - entity: kafka-node
        effect: changes
        facts:
          - Manual rolling update
    contexts:
      api:
        place: custom-resources::pod
  - text: The Product restarts the Kafka node, which moves it to another Kubernetes node
    kind: product
    actor: drain-cleaner
    entities:
      - entity: kafka-node
        effect: changes
        facts:
          - Manual rolling update
    contexts:
      api:
        place: custom-resources::pod
---

# Roll a Kafka node evicted by a Kubernetes node drain

## Trigger

Kubernetes drains the node a Kafka pod runs on and the Drain Cleaner intercepts the eviction.

## Outcome

The Kafka node runs on another Kubernetes node without making partitions unavailable.
