---
entities:
  - entity: kafka-cluster
    shows:
      - Name
      - Kafka version
      - Metadata version
      - Cruise Control
      - Cluster ID
      - Current Kafka version
      - Current metadata version
      - Node pools in use
      - Auto-rebalance status
      - Status message
    collects:
      - Name
      - Kafka version
      - Metadata version
      - Authorization
      - Broker configuration
      - Rack awareness
      - Tiered storage
      - Quotas plugin
      - Metrics and logging
      - Template
      - Maintenance time windows
      - Topic Operator
      - User Operator
      - Cruise Control
      - Auto-rebalance on scaling
      - Kafka Exporter
      - Skip broker scale-down check
  - entity: listener
    shows:
      - Name
      - Bootstrap address
      - Broker addresses
      - Certificates
    collects:
      - Name
      - Port
      - Type
      - TLS encryption
      - Client authentication
      - Listener configuration
      - Network policy peers
  - entity: certificate-authority
    collects:
      - Issuer
      - Validity days
      - Renewal days
      - Expiration policy
      - Secret owner reference
---

# Kafka resource

The `Kafka` resource of one Kafka cluster, where its configuration and listeners are declared and its status reported. Its `clusterCa` and `clientsCa` sections configure the cluster's certificate authorities.
