---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates KafkaNodePool resources for controllers and brokers, each labelled with the cluster name
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: node-pool
        effect: creates
        facts:
          - Name
          - Roles
          - Replicas
          - Storage
          - Resources
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
  - text: The Strimzi administrator creates the Kafka resource with its listeners and configuration
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: creates
        to: Not ready
        facts:
          - Name
          - Kafka version
          - Authorization
          - Broker configuration
          - Topic Operator
          - User Operator
          - Cruise Control
      - entity: listener
        effect: creates
        facts:
          - Name
          - Port
          - Type
          - TLS encryption
          - Client authentication
          - Listener configuration
          - Network policy peers
        with: kafka-cluster
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product generates the cluster CA and the clients CA as the Kafka resource configures them
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: certificate-authority
        as: cluster-ca
        effect: creates
        facts:
          - Purpose
          - Issuer
          - Validity days
          - Renewal days
          - Expiration policy
          - Secret owner reference
          - CA certificate
          - CA private key
          - Certificate generation
          - Key generation
      - entity: certificate-authority
        as: clients-ca
        effect: creates
        facts:
          - Purpose
          - Issuer
          - Validity days
          - Renewal days
          - Expiration policy
          - Secret owner reference
          - CA certificate
          - CA private key
          - Certificate generation
          - Key generation
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product starts one pod per Kafka node with the next free node IDs, the roles of its node pool and volumes for its storage
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-node
        effect: creates
        facts:
          - Node ID
          - Roles
          - Volumes
      - entity: node-pool
        effect: changes
        facts:
          - Node IDs
    contexts:
      api:
        place: custom-resources::kafka-node-pool-resource
  - text: The Product exposes each listener and deploys the operators and Cruise Control the resource asks for
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: listener
        effect: changes
        facts:
          - Bootstrap address
          - Broker addresses
          - Certificates
      - entity: kafka-cluster
        effect: reads
        facts:
          - Topic Operator
          - User Operator
          - Cruise Control
    contexts:
      api:
        place: custom-resources::kafka-resource
  - text: The Product reports the Kafka cluster ready with its cluster ID, versions and the pools in use
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: kafka-cluster
        effect: changes
        from: Not ready
        to: Ready
        facts:
          - Cluster ID
          - Current Kafka version
          - Current metadata version
          - Node pools in use
    contexts:
      api:
        place: custom-resources::kafka-resource
---

# Deploy a Kafka cluster with its node pools

## Trigger

The Strimzi administrator wants a new Kafka cluster in a namespace the Cluster Operator watches.

## Outcome

A running Kafka cluster whose Kafka resource is Ready and reports the bootstrap addresses clients use.

## Edge cases

- An unsupported Kafka version leaves the Kafka resource Not ready with the reason.
- An invalid listener configuration leaves the Kafka resource Not ready, listing every problem found.
- A Kafka resource created with reconciliation paused is ignored until the pause is removed.
