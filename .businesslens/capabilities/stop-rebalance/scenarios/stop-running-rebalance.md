---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the KafkaRebalance resource to stop it
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        facts:
          - Rebalance request
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
  - text: The Product stops the Cruise Control task and reports the rebalance stopped
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: Rebalancing
        to: Stopped
        facts:
          - Rebalance request
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
---

# Stop a running rebalance

## Trigger

A rebalance puts too much load on the cluster.

## Outcome

No more replicas move; replicas already moved stay where they are.
