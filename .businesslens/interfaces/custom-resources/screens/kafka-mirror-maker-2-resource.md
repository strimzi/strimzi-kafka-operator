---
entities:
  - entity: mirror-maker-2
    shows:
      - Name
      - Replicas
      - Connector states
      - Connector status
      - Auto-restart status
      - Status message
    collects:
      - Name
      - Replicas
      - Kafka version
      - Target cluster
      - Mirrors
      - Connector states
      - Metrics and logging
      - Connector restart request
      - Offsets request
---

# KafkaMirrorMaker2 resource

The `KafkaMirrorMaker2` resource of one MirrorMaker 2 deployment, where its target and source clusters and connectors are declared, connector restarts requested and connector status reported.
