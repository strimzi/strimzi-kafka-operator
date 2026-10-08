---
kind: primary
routes:
  api: Kubernetes API
steps:
  - text: The Strimzi administrator creates a KafkaBridge resource with the bootstrap servers and HTTP configuration
    kind: actor
    actor: strimzi-administrator
    entities:
      - entity: http-bridge
        effect: creates
        to: Not ready
        facts:
          - Name
          - Replicas
          - Bootstrap servers
          - TLS and authentication
          - HTTP configuration
          - Client configuration
    contexts:
      api:
        place: custom-resources::kafka-bridge-resource
  - text: The Product starts the HTTP Bridge and reports it ready with its address
    kind: product
    actor: strimzi-administrator
    entities:
      - entity: http-bridge
        effect: changes
        from: Not ready
        to: Ready
        facts:
          - URL
    contexts:
      api:
        place: custom-resources::kafka-bridge-resource
---

# Deploy an HTTP Bridge

## Trigger

Applications need to reach Kafka over HTTP.

## Outcome

HTTP clients can produce and consume through the reported address.
