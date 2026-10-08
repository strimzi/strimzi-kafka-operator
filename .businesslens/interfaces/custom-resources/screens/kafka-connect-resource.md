---
entities:
  - entity: connect-cluster
    shows:
      - Name
      - Replicas
      - REST API URL
      - Connector plugins
      - Status message
    collects:
      - Name
      - Replicas
      - Kafka Connect version
      - Bootstrap servers
      - TLS and authentication
      - Connect configuration
      - Build
      - Mounted plugins
      - Connector resources enabled
      - Rack awareness
      - Metrics and logging
      - Image rebuild requested
---

# KafkaConnect resource

The `KafkaConnect` resource of one Kafka Connect cluster, where its workers, connection to Kafka and plugins are declared and its REST API address and plugins reported.
