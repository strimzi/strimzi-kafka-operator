---
entities:
  - entity: connector
    shows:
      - Name
      - Requested state
      - Connector status
      - Topics
      - Auto-restart status
      - Status message
    collects:
      - Name
      - Connector class
      - Plugin version
      - Tasks max
      - Configuration
      - Requested state
      - Auto-restart
      - Restart request
      - Offsets request
---

# KafkaConnector resource

The `KafkaConnector` resource of one connector, where its class, configuration and requested state are declared, restarts and offset actions are requested, and its status reported.
