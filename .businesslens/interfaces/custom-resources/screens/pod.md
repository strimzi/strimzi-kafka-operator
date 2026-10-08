---
entities:
  - entity: kafka-node
    shows:
      - Node ID
      - Roles
    collects:
      - Manual rolling update
      - Pod and volume deletion
---

# Pod

A pod Strimzi runs for one Kafka node, annotated to ask for a restart or for the pod and its volumes to be deleted.
