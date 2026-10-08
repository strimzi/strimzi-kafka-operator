---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator annotates the Kafka node's pod for a manual rolling update
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts:
          - Manual rolling update
    contexts:
      api:
        place: custom-resources::pod
  - text: The Product restarts the Kafka node when doing so keeps partitions available, and the restarted pod no longer carries the request
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: changes
        facts:
          - Manual rolling update
    contexts:
      api:
        place: custom-resources::pod
---

# Roll one Kafka node

## Trigger

The Strimzi administrator wants one Kafka node restarted.

## Outcome

The Kafka node has restarted and rejoined the cluster.

## Edge cases

- When the manual rolling update fails, the rest of the reconciliation still runs and the request stays in place.
