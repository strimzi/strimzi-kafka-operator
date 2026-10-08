---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaConnect resource with its bootstrap servers, authentication and configuration
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: creates
        to: Not ready
        facts:
          - Name
          - Replicas
          - Kafka Connect version
          - Bootstrap servers
          - TLS and authentication
          - Connect configuration
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
  - text: The Product starts the workers and reports the Kafka Connect cluster ready with its REST API address
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: connect-cluster
        effect: changes
        from: Not ready
        to: Ready
        facts:
          - REST API URL
    contexts:
      api:
        place: custom-resources::kafka-connect-resource
---

# Deploy a Kafka Connect cluster

## Trigger

The Strimzi administrator needs to stream data between Kafka and other systems.

## Outcome

A Kafka Connect cluster runs and reports where its REST API is.

## Edge cases

- With connector resources enabled, the status also lists the connector plugins the workers offer.
