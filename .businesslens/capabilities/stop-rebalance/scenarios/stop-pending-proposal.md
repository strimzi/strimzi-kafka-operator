---
kind: alternative
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates a KafkaRebalance resource whose proposal is pending to stop it
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
  - text: The Product stops waiting for the proposal and reports the rebalance stopped
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: rebalance
        effect: changes
        from: Pending proposal
        to: Stopped
        facts:
          - Rebalance request
    contexts:
      api:
        place: custom-resources::kafka-rebalance-resource
---

# Stop waiting for a proposal

## Trigger

The proposal is no longer wanted.

## Outcome

The rebalance is stopped and no proposal will be carried out.
