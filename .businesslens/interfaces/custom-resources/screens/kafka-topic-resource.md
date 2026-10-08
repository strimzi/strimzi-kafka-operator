---
entities:
  - entity: kafka-topic
    shows:
      - Resource name
      - Topic name
      - Partitions
      - Replicas
      - Configuration
      - Topic ID
      - Replicas change
      - Status message
    collects:
      - Resource name
      - Topic name
      - Partitions
      - Replicas
      - Configuration
---

# KafkaTopic resource

The `KafkaTopic` resource of one topic, where its partitions, replicas and configuration are declared and its state in Kafka reported.
