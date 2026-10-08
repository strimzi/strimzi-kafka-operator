---
entities:
  - entity: node-pool
    shows:
      - Name
      - Roles
      - Replicas
      - Node IDs
      - Status message
    collects:
      - Name
      - Roles
      - Replicas
      - Storage
      - Resources
      - Template
      - Next node IDs
      - Node IDs to remove
---

# KafkaNodePool resource

The `KafkaNodePool` resource of one node pool, where its roles, size and storage are declared and the node IDs it uses are reported.
