---
entities:
  - entity: http-bridge
    shows:
      - Name
      - Replicas
      - URL
      - Status message
    collects:
      - Name
      - Replicas
      - Bootstrap servers
      - TLS and authentication
      - HTTP configuration
      - Client configuration
      - Metrics and logging
---

# KafkaBridge resource

The `KafkaBridge` resource of one HTTP Bridge, where its connection to Kafka and HTTP settings are declared and its address reported.
