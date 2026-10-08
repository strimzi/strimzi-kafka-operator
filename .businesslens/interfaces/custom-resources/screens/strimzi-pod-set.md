---
entities:
  - entity: node-pool
    collects:
      - Manual rolling update
  - entity: connect-cluster
    collects:
      - Manual rolling update
---

# StrimziPodSet

The `StrimziPodSet` Strimzi keeps for a node pool or a Kafka Connect cluster, annotated to roll all of its pods.
